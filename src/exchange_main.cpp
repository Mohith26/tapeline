// The exchange process: one thread that accepts order-entry sessions over TCP,
// sequences everything it reads, runs the matching engine, publishes market
// data over UDP, and serves retransmissions and snapshots over a second TCP
// port. Single-threaded on purpose: the sequencer is the serialisation point,
// so there is nothing to lock.

#include "args.hpp"
#include "tapeline/clock.hpp"
#include "tapeline/engine.hpp"
#include "tapeline/feed.hpp"
#include "tapeline/gateway.hpp"
#include "tapeline/net.hpp"
#include "tapeline/sequencer.hpp"
#include "tapeline/wire.hpp"

#include <array>
#include <csignal>
#include <cstdint>
#include <format>
#include <poll.h>
#include <print>
#include <string>
#include <vector>

using namespace tapeline;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

struct Fanout : EventSink {
  FeedPublisher* feed;
  Gateway* gw;
  Fanout(FeedPublisher* f, Gateway* g) : feed(f), gw(g) {}
  void on_event(const Event& ev) override {
    feed->on_event(ev);
    gw->on_event(ev);
  }
};

struct Conn {
  net::Fd fd;
  SessionId session = 0;
  std::vector<std::byte> inbuf;
  std::vector<std::byte> pending_out; // used only for LoginRejected before close
  bool closing = false;
};

struct RetransConn {
  net::Fd fd;
  std::vector<std::byte> inbuf;
};

std::string json_books(const Engine& engine) {
  std::string out = "[";
  for (std::uint32_t s = 0; s < engine.symbols(); ++s) {
    const Book& b = engine.book(s);
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
    std::println("exchange [--gw-port 9001] [--feed-addr 127.0.0.1] [--feed-port 9002] [--retrans-port 9003]");
    std::println("         [--symbols 8] [--drop-every N] [--log path] [--state-out path] [--idle-exit-ms 2000]");
    std::println("         [--end-after-records N] [--quiet]");
    return 0;
  }
  const auto gw_port = static_cast<std::uint16_t>(args.get_int("--gw-port", 9001));
  const std::string feed_addr = args.get("--feed-addr", "127.0.0.1");
  const auto feed_port = static_cast<std::uint16_t>(args.get_int("--feed-port", 9002));
  const auto retrans_port = static_cast<std::uint16_t>(args.get_int("--retrans-port", 9003));
  const auto n_symbols = static_cast<std::uint32_t>(args.get_int("--symbols", 8));
  const auto drop_every = args.get_int("--drop-every", 0);
  const std::string log_path = args.get("--log", "");
  const std::string state_out = args.get("--state-out", "");
  const auto idle_exit_ms = args.get_int("--idle-exit-ms", 2000);
  const auto end_after = static_cast<std::uint64_t>(args.get_int("--end-after-records", 0));
  const bool quiet = args.has("--quiet");

  net::ignore_sigpipe();
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  Sequencer sequencer;
  if (!log_path.empty() && !sequencer.open_file(log_path)) {
    std::println(stderr, "cannot open log {}", log_path);
    return 1;
  }
  FeedPublisher publisher(static_cast<std::uint32_t>(wall_ns() / 1'000'000'000ull));
  Gateway gateway;
  Fanout fanout(&publisher, &gateway);
  Engine engine(n_symbols, &fanout);

  net::UdpSocket feed_sock;
  if (auto r = feed_sock.set_destination(feed_addr, feed_port); !r) {
    std::println(stderr, "feed socket: {}", r.error().what);
    return 1;
  }
  std::uint64_t feed_packets_sent = 0;
  std::uint64_t feed_packets_dropped = 0;
  publisher.set_sink([&](std::span<const std::byte> p) {
    ++feed_packets_sent;
    if (drop_every > 0 && feed_packets_sent % static_cast<std::uint64_t>(drop_every) == 0) {
      ++feed_packets_dropped; // simulated loss: the ring still has it
      return;
    }
    (void)feed_sock.send(p);
  });

  net::TcpListener gw_listener;
  if (auto r = gw_listener.listen("0.0.0.0", gw_port); !r) {
    std::println(stderr, "gateway listen: {}", r.error().what);
    return 1;
  }
  net::TcpListener retrans_listener;
  if (auto r = retrans_listener.listen("0.0.0.0", retrans_port); !r) {
    std::println(stderr, "retrans listen: {}", r.error().what);
    return 1;
  }

  std::vector<Conn> conns;
  std::vector<RetransConn> rconns;
  std::array<std::byte, 65536> rbuf{};
  Histogram inbound_ns(1 << 20);
  std::uint64_t frames_in = 0;
  std::uint64_t sessions_ever = 0;
  std::uint64_t retrans_served = 0;
  std::uint64_t snapshots_served = 0;
  const Timestamp t_start = now_ns();
  Timestamp last_heartbeat = t_start;
  Timestamp idle_since = 0;

  publisher.system_event(now_ns(), wire::feed::SystemCode::StartOfSession);
  publisher.flush();
  if (!quiet) {
    std::println("exchange up: gateway tcp/{} feed udp {}:{} retrans tcp/{} symbols={} drop_every={}",
                 gw_port, feed_addr, feed_port, retrans_port, n_symbols, drop_every);
  }

  auto process = [&](Conn& c, Inbound rec) {
    const Timestamp t0 = now_ns();
    rec.session = c.session;
    engine.apply(sequencer.append(rec, t0));
    inbound_ns.add(now_ns() - t0);
  };

  auto handle_frames = [&](Conn& c) {
    wire::Reader r(c.inbuf);
    while (true) {
      auto m = wire::gw::decode(r);
      if (!m) {
        if (m.error() != wire::Error::Truncated) {
          if (!quiet) std::println("session {}: bad frame ({}), closing", c.session, wire::to_string(m.error()));
          c.closing = true;
        }
        break;
      }
      ++frames_in;
      switch (m->type) {
        case wire::gw::InType::Login: {
          auto res = gateway.login(m->login);
          if (!res) {
            c.pending_out = Gateway::login_rejected_frame(res.error());
            c.closing = true;
            break;
          }
          c.session = *res;
          ++sessions_ever;
          Inbound rec;
          rec.kind = InboundKind::SessionOpen;
          process(c, rec);
          break;
        }
        case wire::gw::InType::EnterOrder: {
          if (c.session == 0) break;
          Inbound rec;
          rec.kind = InboundKind::NewOrder;
          rec.new_order = m->order;
          process(c, rec);
          break;
        }
        case wire::gw::InType::Cancel: {
          if (c.session == 0) break;
          Inbound rec;
          rec.kind = InboundKind::Cancel;
          rec.cancel = m->cancel;
          process(c, rec);
          break;
        }
        case wire::gw::InType::Replace: {
          if (c.session == 0) break;
          Inbound rec;
          rec.kind = InboundKind::Replace;
          rec.replace = m->replace;
          process(c, rec);
          break;
        }
        case wire::gw::InType::Heartbeat:
          break;
        case wire::gw::InType::Logout:
          c.closing = true;
          break;
      }
    }
    c.inbuf.erase(c.inbuf.begin(), c.inbuf.begin() + static_cast<std::ptrdiff_t>(r.pos()));
  };

  auto close_conn = [&](Conn& c) {
    if (c.session != 0) {
      Inbound rec;
      rec.kind = InboundKind::SessionClose;
      process(c, rec); // cancel on disconnect
      gateway.disconnect(c.session);
    }
    if (!c.pending_out.empty()) (void)net::send_all(c.fd.get(), c.pending_out);
    c.fd.reset();
  };

  auto serve_retrans = [&](RetransConn& rc) {
    wire::Reader r(rc.inbuf);
    while (true) {
      auto q = wire::retrans::decode(r);
      if (!q) break;
      auto out = [&](std::span<const std::byte> p) { (void)net::send_all(rc.fd.get(), p); };
      bool ok = false;
      if (q->type == wire::retrans::ReqType::Retransmit) {
        ok = publisher.retransmit(q->start, q->count, out);
        if (ok) ++retrans_served;
      }
      if (!ok) {
        publisher.snapshot(engine, now_ns(), out);
        ++snapshots_served;
      }
    }
    rc.inbuf.erase(rc.inbuf.begin(), rc.inbuf.begin() + static_cast<std::ptrdiff_t>(r.pos()));
  };

  std::vector<pollfd> pfds;
  while (!g_stop) {
    pfds.clear();
    pfds.push_back({gw_listener.fd(), POLLIN, 0});
    pfds.push_back({retrans_listener.fd(), POLLIN, 0});
    for (const Conn& c : conns) {
      short ev = POLLIN;
      if (c.session != 0 && !gateway.session(c.session).outbuf.empty()) ev |= POLLOUT;
      pfds.push_back({c.fd.get(), ev, 0});
    }
    for (const RetransConn& rc : rconns) pfds.push_back({rc.fd.get(), POLLIN, 0});
    ::poll(pfds.data(), pfds.size(), 10);

    if (pfds[0].revents & POLLIN) {
      while (true) {
        auto a = gw_listener.accept();
        if (!a || !*a) break;
        Conn c;
        c.fd = std::move(*a);
        conns.push_back(std::move(c));
      }
    }
    if (pfds[1].revents & POLLIN) {
      while (true) {
        auto a = retrans_listener.accept();
        if (!a || !*a) break;
        RetransConn rc;
        rc.fd = std::move(*a);
        rconns.push_back(std::move(rc));
      }
    }

    std::size_t pi = 2;
    for (std::size_t i = 0; i < conns.size(); ++i, ++pi) {
      Conn& c = conns[i];
      if (pfds[pi].revents & (POLLIN | POLLHUP | POLLERR)) {
        auto n = net::recv_some(c.fd.get(), rbuf);
        if (!n || *n == 0) {
          c.closing = true;
        } else if (*n > 0) {
          c.inbuf.insert(c.inbuf.end(), rbuf.begin(), rbuf.begin() + *n);
          handle_frames(c);
        }
      }
    }
    for (std::size_t i = 0; i < rconns.size(); ++i, ++pi) {
      RetransConn& rc = rconns[i];
      if (pfds[pi].revents & (POLLIN | POLLHUP | POLLERR)) {
        auto n = net::recv_some(rc.fd.get(), rbuf);
        if (!n || *n == 0) {
          rc.fd.reset();
        } else if (*n > 0) {
          rc.inbuf.insert(rc.inbuf.end(), rbuf.begin(), rbuf.begin() + *n);
          serve_retrans(rc);
        }
      }
    }

    // Everything sequenced in this pass goes out together: one feed packet
    // (or more if it overflowed the MTU) and one write per session.
    publisher.flush();
    for (Conn& c : conns) {
      if (c.session == 0) continue;
      auto& ob = gateway.session(c.session).outbuf;
      if (ob.empty()) continue;
      auto n = net::send_some(c.fd.get(), ob);
      if (!n) {
        c.closing = true;
      } else if (*n > 0) {
        ob.erase(ob.begin(), ob.begin() + *n);
      }
    }
    for (Conn& c : conns) {
      if (c.closing) close_conn(c);
    }
    std::erase_if(conns, [](const Conn& c) { return !c.fd; });
    std::erase_if(rconns, [](const RetransConn& rc) { return !rc.fd; });

    const Timestamp now = now_ns();
    if (now - last_heartbeat >= 1'000'000'000ull) {
      publisher.heartbeat(now);
      publisher.flush();
      gateway.heartbeat_all();
      last_heartbeat = now;
    }
    if (end_after > 0 && sequencer.size() >= end_after) break; // scheduled close
    if (idle_exit_ms > 0 && sessions_ever > 0 && conns.empty()) {
      if (idle_since == 0) idle_since = now;
      if ((now - idle_since) / 1'000'000ull >= static_cast<std::uint64_t>(idle_exit_ms)) break;
    } else {
      idle_since = 0;
    }
  }

  // Orderly close: tell everyone the session is over, then persist state.
  Inbound end;
  end.kind = InboundKind::EndOfSession;
  engine.apply(sequencer.append(end, now_ns()));
  publisher.system_event(now_ns(), wire::feed::SystemCode::EndOfSession);
  const SeqNo end_seq = publisher.next_seq() - 1;
  publisher.flush();
  // Give a handler that lost the final packet a chance to ask for it.
  for (int i = 0; i < 30; ++i) {
    pfds.clear();
    pfds.push_back({retrans_listener.fd(), POLLIN, 0});
    for (const RetransConn& rc : rconns) pfds.push_back({rc.fd.get(), POLLIN, 0});
    ::poll(pfds.data(), pfds.size(), 50);
    if (pfds[0].revents & POLLIN) {
      auto a = retrans_listener.accept();
      if (a && *a) {
        RetransConn rc;
        rc.fd = std::move(*a);
        rconns.push_back(std::move(rc));
      }
    }
    for (std::size_t i = 0; i < rconns.size(); ++i) {
      if (pfds[i + 1].revents & POLLIN) {
        auto n = net::recv_some(rconns[i].fd.get(), rbuf);
        if (n && *n > 0) {
          rconns[i].inbuf.insert(rconns[i].inbuf.end(), rbuf.begin(), rbuf.begin() + *n);
          serve_retrans(rconns[i]);
        }
      }
    }
    publisher.heartbeat(now_ns());
    publisher.flush();
  }
  gateway.end_of_session_all();
  for (Conn& c : conns) {
    if (c.session == 0) continue;
    (void)net::send_all(c.fd.get(), gateway.session(c.session).outbuf);
    gateway.session(c.session).outbuf.clear();
  }
  sequencer.close_file();

  const double elapsed_s = static_cast<double>(now_ns() - t_start) / 1e9;
  const auto& st = engine.stats();
  const auto& fs = publisher.stats();
  const std::string json = std::format(
      "{{\n  \"elapsed_s\": {:.3f},\n  \"symbols\": {},\n  \"sessions\": {},\n  \"frames_in\": {},\n"
      "  \"records\": {},\n  \"orders\": {},\n  \"fills\": {},\n  \"volume\": {},\n  \"cancels\": {},\n"
      "  \"replaces\": {},\n  \"rejects\": {},\n  \"feed_messages\": {},\n  \"feed_packets_built\": {},\n"
      "  \"feed_packets_sent\": {},\n  \"feed_packets_dropped\": {},\n  \"feed_bytes\": {},\n"
      "  \"retrans_served\": {},\n  \"retrans_packets\": {},\n  \"snapshots_served\": {},\n  \"feed_next_seq\": {},\n"
      "  \"feed_end_of_session_seq\": {},\n  \"end_after_records\": {},\n  \"sessions_connected_at_close\": {},\n"
      "  \"inbound_processing_ns\": {},\n  \"books_hash\": \"{:016x}\",\n  \"state_hash\": \"{:016x}\",\n"
      "  \"books\": {}\n}}\n",
      elapsed_s, n_symbols, sessions_ever, frames_in, st.records, st.orders, st.fills, st.volume, st.cancels,
      st.replaces, st.rejects, fs.messages, fs.packets, feed_packets_sent - feed_packets_dropped,
      feed_packets_dropped, fs.bytes, retrans_served, fs.retrans_packets, snapshots_served, publisher.next_seq(),
      end_seq, end_after, conns.size(), inbound_ns.json(), engine.books_hash(), engine.state_hash(), json_books(engine));
  if (!state_out.empty()) write_text(state_out, json);
  if (!quiet) std::print("{}", json);
  return 0;
}
