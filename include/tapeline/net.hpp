#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <string>
#include <utility>

// Thin wrappers over POSIX sockets. Nothing here is clever; the point is to
// keep errno handling and the multicast/loopback details out of the main
// programs so they read as protocol logic.
namespace tapeline::net {

struct Error {
  int errnum{};
  std::string what;
};

inline Error last_error(std::string what) { return Error{errno, std::move(what) + ": " + std::strerror(errno)}; }

inline void ignore_sigpipe() { std::signal(SIGPIPE, SIG_IGN); }

class Fd {
 public:
  Fd() = default;
  explicit Fd(int fd) noexcept : fd_(fd) {}
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;
  Fd(Fd&& o) noexcept : fd_(std::exchange(o.fd_, -1)) {}
  Fd& operator=(Fd&& o) noexcept {
    if (this != &o) {
      reset();
      fd_ = std::exchange(o.fd_, -1);
    }
    return *this;
  }
  ~Fd() { reset(); }

  void reset() noexcept {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
  }
  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] int release() noexcept { return std::exchange(fd_, -1); }
  explicit operator bool() const noexcept { return fd_ >= 0; }

 private:
  int fd_ = -1;
};

inline std::expected<void, Error> set_nonblocking(int fd, bool on) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return std::unexpected(last_error("fcntl(F_GETFL)"));
  flags = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
  if (::fcntl(fd, F_SETFL, flags) < 0) return std::unexpected(last_error("fcntl(F_SETFL)"));
  return {};
}

inline std::expected<void, Error> set_nodelay(int fd) {
  int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
    return std::unexpected(last_error("setsockopt(TCP_NODELAY)"));
  }
  return {};
}

inline sockaddr_in make_addr(const std::string& ip, std::uint16_t port) {
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  if (ip.empty() || ip == "0.0.0.0") {
    a.sin_addr.s_addr = htonl(INADDR_ANY);
  } else {
    ::inet_pton(AF_INET, ip.c_str(), &a.sin_addr);
  }
  return a;
}

inline bool is_multicast(const std::string& ip) {
  in_addr a{};
  if (::inet_pton(AF_INET, ip.c_str(), &a) != 1) return false;
  return IN_MULTICAST(ntohl(a.s_addr));
}

inline std::uint16_t local_port(int fd) {
  sockaddr_in a{};
  socklen_t len = sizeof(a);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len) < 0) return 0;
  return ntohs(a.sin_port);
}

class TcpListener {
 public:
  std::expected<void, Error> listen(const std::string& ip, std::uint16_t port, int backlog = 64) {
    Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
    if (!fd) return std::unexpected(last_error("socket"));
    int one = 1;
    ::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in a = make_addr(ip, port);
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) {
      return std::unexpected(last_error("bind"));
    }
    if (::listen(fd.get(), backlog) < 0) return std::unexpected(last_error("listen"));
    if (auto r = set_nonblocking(fd.get(), true); !r) return r;
    fd_ = std::move(fd);
    return {};
  }

  // Returns an invalid Fd (not an error) when no connection is pending.
  std::expected<Fd, Error> accept() {
    sockaddr_in a{};
    socklen_t len = sizeof(a);
    int c = ::accept(fd_.get(), reinterpret_cast<sockaddr*>(&a), &len);
    if (c < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return Fd{};
      return std::unexpected(last_error("accept"));
    }
    Fd fd(c);
    (void)set_nodelay(c);
    if (auto r = set_nonblocking(c, true); !r) return std::unexpected(r.error());
    return fd;
  }

  [[nodiscard]] int fd() const noexcept { return fd_.get(); }
  [[nodiscard]] std::uint16_t port() const { return local_port(fd_.get()); }

 private:
  Fd fd_;
};

inline std::expected<Fd, Error> tcp_connect(const std::string& ip, std::uint16_t port) {
  Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
  if (!fd) return std::unexpected(last_error("socket"));
  sockaddr_in a = make_addr(ip, port);
  if (::connect(fd.get(), reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) {
    return std::unexpected(last_error("connect"));
  }
  (void)set_nodelay(fd.get());
  return fd;
}

// > 0: bytes moved, 0: peer closed (recv) or nothing sent, -1: would block.
inline std::expected<int, Error> recv_some(int fd, std::span<std::byte> buf) {
  const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return -1;
    return std::unexpected(last_error("recv"));
  }
  return static_cast<int>(n);
}

inline std::expected<int, Error> send_some(int fd, std::span<const std::byte> buf) {
  const ssize_t n = ::send(fd, buf.data(), buf.size(), 0);
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return -1;
    return std::unexpected(last_error("send"));
  }
  return static_cast<int>(n);
}

// Blocking loop that keeps sending until everything is out. Works on both
// blocking and non-blocking sockets (spins politely on the latter).
inline std::expected<void, Error> send_all(int fd, std::span<const std::byte> buf) {
  while (!buf.empty()) {
    auto n = send_some(fd, buf);
    if (!n) return std::unexpected(n.error());
    if (*n < 0) {
      pollfd p{fd, POLLOUT, 0};
      ::poll(&p, 1, 10);
      continue;
    }
    buf = buf.subspan(static_cast<std::size_t>(*n));
  }
  return {};
}

// Blocking read of exactly n bytes. Returns false on EOF.
inline std::expected<bool, Error> recv_exact(int fd, std::span<std::byte> buf) {
  while (!buf.empty()) {
    auto n = recv_some(fd, buf);
    if (!n) return std::unexpected(n.error());
    if (*n == 0) return false;
    if (*n < 0) {
      pollfd p{fd, POLLIN, 0};
      ::poll(&p, 1, 10);
      continue;
    }
    buf = buf.subspan(static_cast<std::size_t>(*n));
  }
  return true;
}

class UdpSocket {
 public:
  std::expected<void, Error> open() {
    Fd fd(::socket(AF_INET, SOCK_DGRAM, 0));
    if (!fd) return std::unexpected(last_error("socket(udp)"));
    fd_ = std::move(fd);
    return {};
  }

  // Bind for receiving. A multicast group address joins the group on the
  // loopback-friendly default interface; anything else binds directly.
  std::expected<void, Error> bind(const std::string& ip, std::uint16_t port, int rcvbuf = 4 << 20) {
    if (!fd_) {
      if (auto r = open(); !r) return r;
    }
    int one = 1;
    ::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    ::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
    ::setsockopt(fd_.get(), SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    const bool mcast = is_multicast(ip);
    sockaddr_in a = make_addr(mcast ? "0.0.0.0" : ip, port);
    if (::bind(fd_.get(), reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) {
      return std::unexpected(last_error("bind(udp)"));
    }
    if (mcast) {
      ip_mreq req{};
      ::inet_pton(AF_INET, ip.c_str(), &req.imr_multiaddr);
      req.imr_interface.s_addr = htonl(INADDR_ANY);
      if (::setsockopt(fd_.get(), IPPROTO_IP, IP_ADD_MEMBERSHIP, &req, sizeof(req)) < 0) {
        return std::unexpected(last_error("IP_ADD_MEMBERSHIP"));
      }
    }
    return set_nonblocking(fd_.get(), true);
  }

  std::expected<void, Error> set_destination(const std::string& ip, std::uint16_t port) {
    if (!fd_) {
      if (auto r = open(); !r) return r;
    }
    dest_ = make_addr(ip, port);
    if (is_multicast(ip)) {
      unsigned char ttl = 1;
      ::setsockopt(fd_.get(), IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
      unsigned char loop = 1;
      ::setsockopt(fd_.get(), IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
    }
    return {};
  }

  std::expected<void, Error> send(std::span<const std::byte> bytes) {
    const ssize_t n = ::sendto(fd_.get(), bytes.data(), bytes.size(), 0,
                               reinterpret_cast<const sockaddr*>(&dest_), sizeof(dest_));
    if (n < 0) return std::unexpected(last_error("sendto"));
    return {};
  }

  // > 0 bytes received, -1 nothing pending.
  std::expected<int, Error> recv(std::span<std::byte> buf) {
    const ssize_t n = ::recvfrom(fd_.get(), buf.data(), buf.size(), 0, nullptr, nullptr);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return -1;
      return std::unexpected(last_error("recvfrom"));
    }
    return static_cast<int>(n);
  }

  [[nodiscard]] int fd() const noexcept { return fd_.get(); }

 private:
  Fd fd_;
  sockaddr_in dest_{};
};

} // namespace tapeline::net
