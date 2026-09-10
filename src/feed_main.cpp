// Market-data feed handler. Listens for the UDP feed, rebuilds every book from
// it, and fills any gaps through the TCP recovery channel. Optionally writes
// every packet it receives to a capture file that tools/feedtap.py can read.

#include "args.hpp"
#include "tapeline/clock.hpp"
#include "tapeline/feed.hpp"
#include "tapeline/net.hpp"
#include "tapeline/wire.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <format>
#include <poll.h>
#include <print>
#include <string>
#include <vector>

using namespace tapeline;

namespace {

std::string json_books(const FeedHandler& h) {
  std::string out = "[";
  for (std::uint32_t s = 0; s < h.symbols(); ++s) {
    const Book& b = h.book(s);
    out += std::format(
        "{}{{\"symbol\": {}, \"live_orders\": {}, \"bid_levels\": {}, \"ask_levels\": {}, \"best_bid\": {}, "
        "\"best_ask\": {}, \"hash\": \"{:016x}\"}}",
        s ? ", " : "", s, b.live_orders(), b.levels(Side::Buy), b.levels(Side::Sell),
        b.best_bid() ? std::to_string(*b.best_bid()) : "null", b.best_ask() ? std::to_string(*b.best_ask()) : "null",
        b.hash());
  }
  return out + "]";
}

} // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (args.has("--help")) {
    std::println("feed [--addr 127.0.0.1] [--port 9002] [--retrans-host 127.0.0.1] [--retrans-port 9003]");
    std::println("     [--symbols 8] [--timeout-ms 60000] [--capture path] [--out path] [--quiet]");
    return 0;
  }
  const std::string addr = args.get("--addr", "127.0.0.1");
  const auto port = static_cast<std::uint16_t>(args.get_int("--port", 9002));
  const std::string rhost = args.get("--retrans-host", "127.0.0.1");
  const auto rport = static_cast<std::uint16_t>(args.get_int("--retrans-port", 9003));
  const auto n_symbols = static_cast<std::uint32_t>(args.get_int("--symbols", 8));
  const auto timeout_ms = static_cast<std::uint64_t>(args.get_int("--timeout-ms", 60000));
  const std::string capture_path = args.get("--capture", "");
  const std::string out = args.get("--out", "");
  const bool quiet = args.has("--quiet");

  net::ignore_sigpipe();
  net::UdpSocket udp;
  if (auto r = udp.bind(addr, port); !r) {
    std::println(stderr, "udp bind: {}", r.error().what);
    return 1;
  }
  auto tcp = net::tcp_connect(rhost, rport);
  if (!tcp) {
    std::println(stderr, "retrans connect: {}", tcp.error().what);
    return 1;
  }
  (void)net::set_nonblocking(tcp->get(), true);

  std::FILE* capture = nullptr;
  if (!capture_path.empty()) {
    capture = std::fopen(capture_path.c_str(), "wb");
    if (!capture) {
      std::println(stderr, "cannot open capture {}", capture_path);
      return 1;
    }
  }
  auto record = [&](std::span<const std::byte> p) {
    if (!capture) return;
    const auto len = static_cast<std::uint32_t>(p.size());
    std::fwrite(&len, sizeof(len), 1, capture);
    std::fwrite(p.data(), 1, p.size(), capture);
  };

  FeedHandler handler(n_symbols);
  std::uint64_t requests = 0;
  handler.set_retrans_requester([&](SeqNo start, std::uint16_t count) {
    ++requests;
    wire::retrans::Request q;
    q.type = wire::retrans::ReqType::Retransmit;
    q.session = handler.session();
    q.start = start;
    q.count = count;
    std::array<std::byte, wire::retrans::kFrameSize> buf{};
    wire::Writer w(buf);
    wire::retrans::encode(q, w);
    (void)net::send_all(tcp->get(), w.written());
  });

  std::array<std::byte, 65536> buf{};
  std::vector<std::byte> tcp_in;
  Histogram handle_ns(1 << 20);
  std::uint64_t udp_packets = 0, tcp_packets = 0, udp_bytes = 0;
  const Timestamp t_start = now_ns();
  Timestamp last_activity = t_start;
  bool tcp_open = true;

  while (true) {
    pollfd p[2] = {{udp.fd(), POLLIN, 0}, {tcp->get(), static_cast<short>(tcp_open ? POLLIN : 0), 0}};
    ::poll(p, 2, 10);
    const Timestamp now = now_ns();
    if (p[0].revents & POLLIN) {
      while (true) {
        auto n = udp.recv(buf);
        if (!n || *n < 0) break;
        ++udp_packets;
        udp_bytes += static_cast<std::uint64_t>(*n);
        last_activity = now;
        const auto packet = std::span<const std::byte>(buf.data(), static_cast<std::size_t>(*n));
        record(packet);
        const Timestamp t0 = now_ns();
        handler.on_packet(packet, t0);
        handle_ns.add(now_ns() - t0);
      }
    }
    if (tcp_open && (p[1].revents & (POLLIN | POLLHUP | POLLERR))) {
      auto n = net::recv_some(tcp->get(), buf);
      if (!n || *n == 0) {
        tcp_open = false;
      } else if (*n > 0) {
        tcp_in.insert(tcp_in.end(), buf.begin(), buf.begin() + *n);
        std::size_t pos = 0;
        while (tcp_in.size() - pos >= wire::feed::kHeaderSize) {
          wire::Reader r(std::span<const std::byte>(tcp_in).subspan(pos));
          auto h = wire::feed::read_header(r);
          if (!h) {
            tcp_in.clear();
            pos = 0;
            break;
          }
          if (tcp_in.size() - pos < h->length) break;
          const auto packet = std::span<const std::byte>(tcp_in).subspan(pos, h->length);
          ++tcp_packets;
          last_activity = now;
          record(packet);
          const Timestamp t0 = now_ns();
          handler.on_packet(packet, t0);
          handle_ns.add(now_ns() - t0);
          pos += h->length;
        }
        tcp_in.erase(tcp_in.begin(), tcp_in.begin() + static_cast<std::ptrdiff_t>(pos));
      }
    }
    handler.on_timer(now_ns());
    if (handler.end_of_session() && !handler.gap_outstanding()) break;
    if ((now - last_activity) / 1'000'000ull > timeout_ms) {
      if (!quiet) std::println(stderr, "feed: timed out waiting for data");
      break;
    }
  }
  if (capture) std::fclose(capture);

  const double elapsed_s = static_cast<double>(now_ns() - t_start) / 1e9;
  const auto& st = handler.stats();
  const std::string json = std::format(
      "{{\n  \"elapsed_s\": {:.3f},\n  \"session\": {},\n  \"udp_packets\": {},\n  \"udp_bytes\": {},\n"
      "  \"tcp_packets\": {},\n  \"messages\": {},\n  \"applied\": {},\n  \"gaps\": {},\n"
      "  \"retrans_requests\": {},\n  \"recovered_packets\": {},\n  \"duplicates\": {},\n  \"buffered\": {},\n"
      "  \"snapshots\": {},\n  \"decode_errors\": {},\n  \"unknown_orders\": {},\n  \"heartbeats\": {},\n"
      "  \"next_seq\": {},\n  \"end_of_session\": {},\n  \"gap_outstanding\": {},\n  \"packet_handling_ns\": {},\n"
      "  \"books_hash\": \"{:016x}\",\n  \"books\": {}\n}}\n",
      elapsed_s, handler.session(), udp_packets, udp_bytes, tcp_packets, st.messages, st.applied, st.gaps,
      st.retrans_requests, st.recovered, st.duplicates, st.buffered, st.snapshots, st.decode_errors,
      st.unknown_orders, st.heartbeats, handler.next_seq(), handler.end_of_session(), handler.gap_outstanding(),
      handle_ns.json(), handler.books_hash(), json_books(handler));
  if (!out.empty()) write_text(out, json);
  if (!quiet) std::print("{}", json);
  (void)requests;
  return handler.end_of_session() && !handler.gap_outstanding() ? 0 : 2;
}
