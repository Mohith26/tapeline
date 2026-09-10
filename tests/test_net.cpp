#include "tapeline/net.hpp"
#include "test.hpp"

#include <array>
#include <cstring>
#include <poll.h>
#include <string>

using namespace tapeline;

TEST(net_tcp_loopback_roundtrip) {
  net::ignore_sigpipe();
  net::TcpListener listener;
  auto ok = listener.listen("127.0.0.1", 0);
  CHECK_MSG(ok.has_value(), ok ? "" : ok.error().what);
  if (!ok) return;
  const auto port = listener.port();
  CHECK(port != 0);
  auto client = net::tcp_connect("127.0.0.1", port);
  CHECK(client.has_value());
  if (!client) return;
  net::Fd server;
  for (int i = 0; i < 100 && !server; ++i) {
    pollfd p{listener.fd(), POLLIN, 0};
    ::poll(&p, 1, 50);
    auto a = listener.accept();
    CHECK(a.has_value());
    if (a && *a) server = std::move(*a);
  }
  CHECK(static_cast<bool>(server));
  if (!server) return;
  const std::string msg = "hello over loopback";
  CHECK(net::send_all(client->get(), std::as_bytes(std::span(msg))).has_value());
  std::array<std::byte, 64> buf{};
  auto got = net::recv_exact(server.get(), std::span(buf).first(msg.size()));
  CHECK(got && *got);
  CHECK(std::memcmp(buf.data(), msg.data(), msg.size()) == 0);
  client->reset();
  auto eof = net::recv_exact(server.get(), std::span(buf).first(1));
  CHECK(eof && !*eof);
}

TEST(net_udp_loopback_roundtrip) {
  net::UdpSocket rx;
  auto ok = rx.bind("127.0.0.1", 0);
  CHECK_MSG(ok.has_value(), ok ? "" : ok.error().what);
  if (!ok) return;
  const auto port = net::local_port(rx.fd());
  net::UdpSocket tx;
  CHECK(tx.set_destination("127.0.0.1", port).has_value());
  const std::string msg = "datagram";
  CHECK(tx.send(std::as_bytes(std::span(msg))).has_value());
  std::array<std::byte, 64> buf{};
  int n = -1;
  for (int i = 0; i < 100 && n < 0; ++i) {
    pollfd p{rx.fd(), POLLIN, 0};
    ::poll(&p, 1, 50);
    auto r = rx.recv(buf);
    CHECK(r.has_value());
    if (r) n = *r;
  }
  CHECK_EQ(n, static_cast<int>(msg.size()));
  CHECK(std::memcmp(buf.data(), msg.data(), msg.size()) == 0);
}

TEST(net_multicast_detection) {
  CHECK(net::is_multicast("239.1.2.3"));
  CHECK(net::is_multicast("224.0.0.1"));
  CHECK(!net::is_multicast("127.0.0.1"));
  CHECK(!net::is_multicast("10.0.0.1"));
  CHECK(!net::is_multicast("not an address"));
}
