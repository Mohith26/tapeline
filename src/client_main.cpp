// Order-entry load generator. Logs in, keeps a window of orders in flight,
// measures the round trip from send to the exchange's first response, and can
// drop its connection partway through to exercise session resume.

#include "args.hpp"
#include "tapeline/clock.hpp"
#include "tapeline/net.hpp"
#include "tapeline/wire.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <format>
#include <poll.h>
#include <print>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

using namespace tapeline;

namespace {

struct Client {
  net::Fd fd;
  std::vector<std::byte> inbuf;
  SessionId session = 0;
  SeqNo next_expected = 1;
  std::uint64_t seq_gaps = 0;
  std::uint64_t frames = 0;
  std::uint64_t accepted = 0, executed = 0, canceled = 0, replaced = 0, rejected = 0, heartbeats = 0;
  std::uint64_t replayed_on_resume = 0;
  bool end_of_session = false;
  std::unordered_map<ClientOrderId, Timestamp> inflight;
  std::unordered_map<ClientOrderId, Qty> live;
  Histogram rtt{1 << 20};
  std::array<std::byte, 65536> rbuf{};

  bool connect(const std::string& host, std::uint16_t port) {
    auto f = net::tcp_connect(host, port);
    if (!f) {
      std::println(stderr, "connect: {}", f.error().what);
      return false;
    }
    fd = std::move(*f);
    (void)net::set_nonblocking(fd.get(), true);
    return true;
  }

  void send_frame(const wire::gw::InMsg& m) {
    std::array<std::byte, wire::gw::kMaxFrame> buf{};
    wire::Writer w(buf);
    wire::gw::encode(m, w);
    (void)net::send_all(fd.get(), w.written());
  }

  // Read whatever is available and dispatch complete frames.
  bool pump(int timeout_ms) {
    pollfd p{fd.get(), POLLIN, 0};
    ::poll(&p, 1, timeout_ms);
    auto n = net::recv_some(fd.get(), rbuf);
    if (!n) return false;
    if (*n == 0) return false;
    if (*n < 0) return true;
    inbuf.insert(inbuf.end(), rbuf.begin(), rbuf.begin() + *n);
    wire::Reader r(inbuf);
    while (true) {
      auto m = wire::gw::decode_out(r);
      if (!m) break;
      ++frames;
      on_frame(*m);
    }
    inbuf.erase(inbuf.begin(), inbuf.begin() + static_cast<std::ptrdiff_t>(r.pos()));
    return true;
  }

  void on_frame(const wire::gw::OutMsg& m) {
    using wire::gw::OutType;
    if (m.type != OutType::LoginAccepted && m.type != OutType::LoginRejected && m.type != OutType::Heartbeat) {
      if (m.seq != next_expected) ++seq_gaps;
      next_expected = m.seq + 1;
    }
    auto settle = [&](ClientOrderId cl) {
      auto it = inflight.find(cl);
      if (it != inflight.end()) {
        rtt.add(now_ns() - it->second);
        inflight.erase(it);
      }
    };
    switch (m.type) {
      case OutType::LoginAccepted:
        session = m.session;
        break;
      case OutType::LoginRejected:
        std::println(stderr, "login rejected: {}", m.reason);
        break;
      case OutType::Accepted:
        ++accepted;
        settle(m.cl_id);
        live[m.cl_id] = m.qty;
        break;
      case OutType::Executed:
        ++executed;
        if (m.leaves == 0) live.erase(m.cl_id); else live[m.cl_id] = m.leaves;
        break;
      case OutType::Canceled:
        ++canceled;
        settle(m.cl_id);
        if (m.leaves == 0) live.erase(m.cl_id); else live[m.cl_id] = m.leaves;
        break;
      case OutType::Replaced:
        ++replaced;
        settle(m.cl_id);
        live.erase(m.cl_id);
        live[m.cl_id2] = m.qty;
        break;
      case OutType::Rejected:
        ++rejected;
        settle(m.cl_id);
        break;
      case OutType::Heartbeat:
        ++heartbeats;
        break;
      case OutType::EndOfSession:
        end_of_session = true;
        break;
    }
  }
};

} // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (args.has("--help")) {
    std::println("client [--host 127.0.0.1] [--port 9001] [--orders 100000] [--window 16] [--symbols 8]");
    std::println("       [--seed 1] [--cancel-ratio 0.3] [--ioc-ratio 0.1] [--resume-at N] [--user name] [--out path]");
    return 0;
  }
  const std::string host = args.get("--host", "127.0.0.1");
  const auto port = static_cast<std::uint16_t>(args.get_int("--port", 9001));
  const auto n_orders = static_cast<std::uint64_t>(args.get_int("--orders", 100000));
  const auto window = static_cast<std::size_t>(args.get_int("--window", 16));
  const auto n_symbols = static_cast<std::uint32_t>(args.get_int("--symbols", 8));
  const auto seed = static_cast<unsigned>(args.get_int("--seed", 1));
  const double cancel_ratio = args.get_double("--cancel-ratio", 0.3);
  const double ioc_ratio = args.get_double("--ioc-ratio", 0.1);
  const auto resume_at = static_cast<std::uint64_t>(args.get_int("--resume-at", 0));
  const std::string user = args.get("--user", "loadgen");
  const std::string out = args.get("--out", "");

  net::ignore_sigpipe();
  Client c;
  if (!c.connect(host, port)) return 1;

  auto login = [&](SessionId session, SeqNo next_expected) {
    wire::gw::InMsg m;
    m.type = wire::gw::InType::Login;
    std::memset(m.login.user.data(), ' ', m.login.user.size());
    std::memcpy(m.login.user.data(), user.data(), std::min(user.size(), m.login.user.size()));
    m.login.session = session;
    m.login.next_expected = next_expected;
    c.send_frame(m);
    for (int i = 0; i < 200 && c.session == 0; ++i) {
      if (!c.pump(10)) break;
    }
    return c.session != 0;
  };
  if (!login(0, 0)) {
    std::println(stderr, "login failed");
    return 1;
  }

  std::mt19937 rng(seed);
  std::uniform_real_distribution<double> coin(0.0, 1.0);
  std::uniform_int_distribution<Price> px(9900, 10100);
  std::uniform_int_distribution<Qty> qty(1, 100);
  std::uniform_int_distribution<SymbolId> sym(0, n_symbols - 1);

  ClientOrderId next_cl = 1;
  std::uint64_t sent_orders = 0, sent_cancels = 0, sent_replaces = 0;
  bool resumed = false;
  std::uint64_t frames_before_resume = 0;
  const Timestamp t_start = now_ns();

  while (sent_orders + sent_cancels + sent_replaces < n_orders || !c.inflight.empty()) {
    while (c.inflight.size() < window && sent_orders + sent_cancels + sent_replaces < n_orders) {
      wire::gw::InMsg m;
      const double r = coin(rng);
      std::vector<ClientOrderId> pick;
      if (r < cancel_ratio && !c.live.empty()) {
        // Cancel or replace something that is resting.
        auto it = c.live.begin();
        std::advance(it, static_cast<std::ptrdiff_t>(rng() % c.live.size()));
        if (coin(rng) < 0.25) {
          m.type = wire::gw::InType::Replace;
          m.replace = ReplaceOrder{it->first, next_cl, px(rng), qty(rng)};
          c.inflight[it->first] = now_ns();
          c.live.erase(it);
          ++next_cl;
          ++sent_replaces;
        } else {
          m.type = wire::gw::InType::Cancel;
          m.cancel = CancelOrder{it->first, 0};
          c.inflight[it->first] = now_ns();
          c.live.erase(it);
          ++sent_cancels;
        }
      } else {
        m.type = wire::gw::InType::EnterOrder;
        const bool ioc = coin(rng) < ioc_ratio;
        m.order = NewOrder{next_cl, sym(rng), coin(rng) < 0.5 ? Side::Buy : Side::Sell, OrderType::Limit,
                           ioc ? TimeInForce::IOC : TimeInForce::Day, px(rng), qty(rng)};
        c.inflight[next_cl] = now_ns();
        ++next_cl;
        ++sent_orders;
      }
      c.send_frame(m);
    }
    if (!c.pump(1)) {
      std::println(stderr, "connection lost");
      break;
    }
    if (resume_at > 0 && !resumed && sent_orders >= resume_at && c.inflight.empty()) {
      // Simulate a dropped connection: close without logging out, then log
      // back in with the last sequence we saw and let the gateway replay.
      resumed = true;
      frames_before_resume = c.frames;
      c.fd.reset();
      c.inbuf.clear();
      const SessionId session = c.session;
      const SeqNo resume_from = c.next_expected;
      c.session = 0;
      if (!c.connect(host, port) || !login(session, resume_from)) {
        std::println(stderr, "resume failed");
        return 1;
      }
      // The cancel-on-disconnect frames arrive as part of the replay.
      for (int i = 0; i < 50; ++i) c.pump(2);
      c.replayed_on_resume = c.frames - frames_before_resume;
      c.live.clear();
    }
  }
  const double elapsed_s = static_cast<double>(now_ns() - t_start) / 1e9;

  wire::gw::InMsg bye;
  bye.type = wire::gw::InType::Logout;
  c.send_frame(bye);
  for (int i = 0; i < 20; ++i) {
    if (!c.pump(5)) break;
  }

  const std::uint64_t total_sent = sent_orders + sent_cancels + sent_replaces;
  const std::string json = std::format(
      "{{\n  \"session\": {},\n  \"elapsed_s\": {:.3f},\n  \"sent_orders\": {},\n  \"sent_cancels\": {},\n"
      "  \"sent_replaces\": {},\n  \"sent_total\": {},\n  \"window\": {},\n  \"msgs_per_s\": {:.0f},\n"
      "  \"frames_received\": {},\n  \"accepted\": {},\n  \"executed\": {},\n  \"canceled\": {},\n"
      "  \"replaced\": {},\n  \"rejected\": {},\n  \"heartbeats\": {},\n  \"seq_gaps\": {},\n"
      "  \"resumed\": {},\n  \"replayed_on_resume\": {},\n  \"end_of_session_seen\": {},\n  \"rtt_ns\": {}\n}}\n",
      c.session, elapsed_s, sent_orders, sent_cancels, sent_replaces, total_sent, window,
      static_cast<double>(total_sent) / elapsed_s, c.frames, c.accepted, c.executed, c.canceled, c.replaced,
      c.rejected, c.heartbeats, c.seq_gaps, resumed, c.replayed_on_resume, c.end_of_session, c.rtt.json());
  if (!out.empty()) write_text(out, json);
  std::print("{}", json);
  return c.seq_gaps == 0 ? 0 : 2;
}
