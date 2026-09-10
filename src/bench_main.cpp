// In-process benchmarks. No sockets here; this measures the data structures
// and the codecs on their own so the numbers are about the code, not the
// kernel. Every figure in the README comes from this program or the e2e run.

#include "args.hpp"
#include "tapeline/book.hpp"
#include "tapeline/clock.hpp"
#include "tapeline/engine.hpp"
#include "tapeline/feed.hpp"
#include "tapeline/replica.hpp"
#include "tapeline/sequencer.hpp"
#include "tapeline/wire.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <format>
#include <print>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

using namespace tapeline;

namespace {

struct Counter : EventSink {
  std::uint64_t n = 0;
  void on_event(const Event&) override { ++n; }
};

std::string bench_book(std::uint64_t n) {
  std::mt19937 rng(1);
  std::uniform_int_distribution<Price> bid_px(9800, 9999);
  std::uniform_int_distribution<Price> ask_px(10001, 10200);
  std::uniform_int_distribution<Qty> qty(1, 100);
  struct Op {
    Side side;
    Price price;
    Qty qty;
  };
  std::vector<Op> ops;
  ops.reserve(n);
  for (std::uint64_t i = 0; i < n; ++i) {
    const Side s = (i & 1) ? Side::Buy : Side::Sell;
    ops.push_back({s, s == Side::Buy ? bid_px(rng) : ask_px(rng), qty(rng)});
  }
  Book book(0, n);
  std::vector<std::uint32_t> slots(n);
  Histogram add_h(n), cancel_h(n);
  const Timestamp t0 = now_ns();
  for (std::uint64_t i = 0; i < n; ++i) {
    const Timestamp a = now_ns();
    slots[i] = book.rest(i + 1, ops[i].side, ops[i].price, ops[i].qty, 1, i + 1);
    add_h.add(now_ns() - a);
  }
  const Timestamp t1 = now_ns();
  std::shuffle(slots.begin(), slots.end(), rng);
  for (std::uint64_t i = 0; i < n; ++i) {
    const Timestamp a = now_ns();
    book.remove(slots[i]);
    cancel_h.add(now_ns() - a);
  }
  const Timestamp t2 = now_ns();
  std::string why;
  const bool ok = book.check_invariants(&why) && book.live_orders() == 0;
  return std::format(
      "{{\"orders\": {}, \"levels_per_side\": 200, \"mean_add_ns\": {:.1f}, \"mean_cancel_ns\": {:.1f}, "
      "\"add_ns\": {}, \"cancel_ns\": {}, \"add_per_s\": {:.0f}, \"cancel_per_s\": {:.0f}, \"invariants_ok\": {}}}",
      n, static_cast<double>(t1 - t0) / static_cast<double>(n), static_cast<double>(t2 - t1) / static_cast<double>(n),
      add_h.json(), cancel_h.json(), static_cast<double>(n) * 1e9 / static_cast<double>(t1 - t0),
      static_cast<double>(n) * 1e9 / static_cast<double>(t2 - t1), ok);
}

// Tracks which client order ids are still resting so the generator only
// cancels orders that exist; the workload is then free of spurious rejects.
struct LiveTracker : EventSink {
  std::vector<ClientOrderId> live;
  std::unordered_map<ClientOrderId, std::size_t> where;
  void add(ClientOrderId cl) {
    where[cl] = live.size();
    live.push_back(cl);
  }
  void drop(ClientOrderId cl) {
    auto it = where.find(cl);
    if (it == where.end()) return;
    const std::size_t i = it->second;
    const ClientOrderId last = live.back();
    live[i] = last;
    where[last] = i;
    live.pop_back();
    where.erase(it);
  }
  void on_event(const Event& ev) override {
    switch (ev.type) {
      case EventType::Rested: add(ev.cl_id); break;
      case EventType::Executed:
        if (ev.resting_leaves == 0) drop(ev.resting_cl_id);
        break;
      case EventType::Canceled:
        if (ev.leaves == 0) drop(ev.cl_id);
        break;
      case EventType::Replaced: drop(ev.old_cl_id); break;
      default: break;
    }
  }
};

std::string bench_matching(std::uint64_t n, Sequencer& seq_out) {
  // Pass one builds the workload against a live engine so cancels and
  // replaces always target resting orders. Pass two is what gets timed.
  std::mt19937 rng(2);
  std::uniform_int_distribution<int> op(0, 9);
  std::uniform_int_distribution<Price> px(9990, 10010);
  std::uniform_int_distribution<Qty> qty(1, 100);
  std::vector<Inbound> recs;
  recs.reserve(n + 1);
  LiveTracker tracker;
  Engine gen(1, &tracker);
  Inbound open;
  open.kind = InboundKind::SessionOpen;
  open.session = 1;
  open.seq = 1;
  recs.push_back(open);
  gen.apply(open);
  ClientOrderId next_cl = 1;
  for (std::uint64_t i = 0; i < n; ++i) {
    Inbound r;
    r.session = 1;
    r.seq = i + 2;
    const int o = op(rng);
    if (o < 7 || tracker.live.empty()) {
      r.kind = InboundKind::NewOrder;
      r.new_order = NewOrder{next_cl++, 0, o % 2 ? Side::Buy : Side::Sell, OrderType::Limit,
                             o == 6 ? TimeInForce::IOC : TimeInForce::Day, px(rng), qty(rng)};
    } else if (o < 9) {
      std::uniform_int_distribution<std::size_t> pick(0, tracker.live.size() - 1);
      r.kind = InboundKind::Cancel;
      r.cancel = CancelOrder{tracker.live[pick(rng)], 0};
    } else {
      std::uniform_int_distribution<std::size_t> pick(0, tracker.live.size() - 1);
      r.kind = InboundKind::Replace;
      r.replace = ReplaceOrder{tracker.live[pick(rng)], next_cl++, px(rng), qty(rng)};
    }
    recs.push_back(r);
    gen.apply(r);
  }

  Counter sink;
  Engine engine(1, &sink);
  engine.reserve(n);
  seq_out.reserve(n + 1);
  Histogram h(n);
  const Timestamp t0 = now_ns();
  for (const Inbound& r : recs) {
    const Timestamp a = now_ns();
    engine.apply(seq_out.append(r, a));
    h.add(now_ns() - a);
  }
  const Timestamp t1 = now_ns();
  std::string why;
  const bool ok = engine.check_invariants(&why) && engine.state_hash() == gen.state_hash();
  const auto& st = engine.stats();
  return std::format(
      "{{\"records\": {}, \"orders\": {}, \"fills\": {}, \"cancels\": {}, \"replaces\": {}, \"rejects\": {}, "
      "\"events\": {}, \"resting_at_end\": {}, \"records_per_s\": {:.0f}, \"mean_ns_per_record\": {:.1f}, "
      "\"apply_ns\": {}, \"invariants_ok\": {}}}",
      st.records, st.orders, st.fills, st.cancels, st.replaces, st.rejects, sink.n, engine.book(0).live_orders(),
      static_cast<double>(recs.size()) * 1e9 / static_cast<double>(t1 - t0),
      static_cast<double>(t1 - t0) / static_cast<double>(recs.size()), h.json(), ok);
}

std::string bench_replay(const Sequencer& seq, std::uint64_t primary_hash) {
  Replica rep(1);
  const Timestamp t0 = now_ns();
  const auto n = rep.follow(seq.log());
  const Timestamp t1 = now_ns();
  return std::format("{{\"records\": {}, \"records_per_s\": {:.0f}, \"state_matches_primary\": {}}}", n,
                     static_cast<double>(n) * 1e9 / static_cast<double>(t1 - t0), rep.state_hash() == primary_hash);
}

std::string bench_feed_codec(std::uint64_t n) {
  std::vector<std::byte> buf(n * 40);
  wire::Writer w(buf);
  const Timestamp t0 = now_ns();
  for (std::uint64_t i = 0; i < n; ++i) {
    wire::feed::Msg m;
    if (i % 3 == 0) {
      m.type = wire::feed::MsgType::AddOrder;
      m.oid = i;
      m.symbol = static_cast<SymbolId>(i % 8);
      m.side = i & 1 ? Side::Buy : Side::Sell;
      m.price = 10000 + static_cast<Price>(i % 50);
      m.qty = static_cast<Qty>(1 + i % 100);
    } else if (i % 3 == 1) {
      m.type = wire::feed::MsgType::Executed;
      m.oid = i;
      m.qty = 7;
      m.match_id = i;
      m.price = 10000;
    } else {
      m.type = wire::feed::MsgType::Delete;
      m.oid = i;
    }
    m.ts = i;
    wire::feed::encode(m, w);
  }
  const Timestamp t1 = now_ns();
  wire::Reader r(w.written());
  std::uint64_t decoded = 0;
  std::uint64_t checksum = 0;
  while (r.remaining() > 0) {
    auto m = wire::feed::decode(r);
    if (!m) break;
    ++decoded;
    checksum += m->oid + m->qty;
  }
  const Timestamp t2 = now_ns();
  return std::format(
      "{{\"messages\": {}, \"bytes\": {}, \"encode_ns_per_msg\": {:.2f}, \"decode_ns_per_msg\": {:.2f}, "
      "\"decode_msgs_per_s\": {:.0f}, \"all_decoded\": {}, \"checksum\": {}}}",
      n, w.size(), static_cast<double>(t1 - t0) / static_cast<double>(n),
      static_cast<double>(t2 - t1) / static_cast<double>(n), static_cast<double>(decoded) * 1e9 / static_cast<double>(t2 - t1),
      decoded == n, checksum);
}

std::string bench_gateway_codec(std::uint64_t n) {
  std::vector<std::byte> buf(n * 64);
  wire::Writer w(buf);
  const Timestamp t0 = now_ns();
  for (std::uint64_t i = 0; i < n; ++i) {
    wire::gw::OutMsg m;
    m.type = i & 1 ? wire::gw::OutType::Accepted : wire::gw::OutType::Executed;
    m.seq = i;
    m.ts = i;
    m.cl_id = i;
    m.oid = i;
    m.price = 10000;
    m.qty = 5;
    m.leaves = 1;
    m.match_id = i;
    wire::gw::encode(m, w);
  }
  const Timestamp t1 = now_ns();
  wire::Reader r(w.written());
  std::uint64_t decoded = 0;
  while (r.remaining() > 0) {
    auto m = wire::gw::decode_out(r);
    if (!m) break;
    ++decoded;
  }
  const Timestamp t2 = now_ns();
  return std::format(
      "{{\"frames\": {}, \"bytes\": {}, \"encode_ns_per_frame\": {:.2f}, \"decode_ns_per_frame\": {:.2f}, \"all_decoded\": {}}}",
      n, w.size(), static_cast<double>(t1 - t0) / static_cast<double>(n),
      static_cast<double>(t2 - t1) / static_cast<double>(n), decoded == n);
}

std::string bench_feed_pipeline(const Sequencer& seq) {
  // Engine -> publisher -> packets -> handler, all in process. Measures what
  // a feed handler has to keep up with, minus the network.
  FeedPublisher pub(1);
  Engine engine(1, &pub);
  std::vector<std::vector<std::byte>> packets;
  pub.set_sink([&](std::span<const std::byte> p) { packets.emplace_back(p.begin(), p.end()); });
  for (const Inbound& r : seq.log()) engine.apply(r);
  pub.flush();
  FeedHandler handler(1);
  const Timestamp t0 = now_ns();
  for (const auto& p : packets) handler.on_packet(p);
  const Timestamp t1 = now_ns();
  const auto& st = handler.stats();
  return std::format(
      "{{\"messages\": {}, \"packets\": {}, \"bytes\": {}, \"avg_msgs_per_packet\": {:.1f}, \"handler_msgs_per_s\": {:.0f}, "
      "\"handler_ns_per_msg\": {:.1f}, \"book_matches_engine\": {}}}",
      st.messages, packets.size(), pub.stats().bytes, static_cast<double>(st.messages) / static_cast<double>(packets.size()),
      static_cast<double>(st.messages) * 1e9 / static_cast<double>(t1 - t0),
      static_cast<double>(t1 - t0) / static_cast<double>(st.messages), handler.books_hash() == engine.books_hash());
}

} // namespace

int main(int argc, char** argv) {
  Args args(argc, argv);
  const std::string out = argc > 1 && argv[1][0] != '-' ? argv[1] : args.get("--out", "");
  const auto n = static_cast<std::uint64_t>(args.get_int("--n", 1'000'000));
  const auto timer_probe = [] {
    Histogram h(100000);
    for (int i = 0; i < 100000; ++i) {
      const Timestamp a = now_ns();
      h.add(now_ns() - a);
    }
    return h.json();
  }();

  std::println("book ...");
  const std::string book = bench_book(n);
  std::println("matching ...");
  Sequencer seq;
  const std::string matching = bench_matching(n, seq);
  Engine primary(1);
  for (const Inbound& r : seq.log()) primary.apply(r);
  std::println("replay ...");
  const std::string replay = bench_replay(seq, primary.state_hash());
  std::println("feed codec ...");
  const std::string feed_codec = bench_feed_codec(n);
  std::println("gateway codec ...");
  const std::string gw_codec = bench_gateway_codec(n);
  std::println("feed pipeline ...");
  const std::string pipeline = bench_feed_pipeline(seq);

  const std::string json = std::format(
      "{{\n  \"n\": {},\n  \"timer_overhead_ns\": {},\n  \"book\": {},\n  \"matching\": {},\n  \"replay\": {},\n"
      "  \"feed_codec\": {},\n  \"gateway_codec\": {},\n  \"feed_pipeline\": {}\n}}\n",
      n, timer_probe, book, matching, replay, feed_codec, gw_codec, pipeline);
  if (!out.empty()) write_text(out, json);
  std::print("{}", json);
  return 0;
}
