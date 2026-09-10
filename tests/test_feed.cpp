#include "tapeline/engine.hpp"
#include "tapeline/feed.hpp"
#include "tapeline/sequencer.hpp"
#include "test.hpp"

#include <random>
#include <string>
#include <vector>

using namespace tapeline;

namespace {

using Packet = std::vector<std::byte>;

// Drives an engine with a random but crossing-heavy order flow and captures
// every feed packet the publisher produces, so the tests can deliver them to
// a handler any way they like.
struct Scenario {
  static constexpr std::uint32_t kSymbols = 3;
  Sequencer seq;
  FeedPublisher pub{77};
  Engine engine{kSymbols, &pub};
  std::vector<Packet> packets;
  Timestamp t = 1;

  Scenario() {
    pub.set_sink([this](std::span<const std::byte> p) { packets.emplace_back(p.begin(), p.end()); });
    Inbound r;
    r.kind = InboundKind::SessionOpen;
    r.session = 1;
    engine.apply(seq.append(r, t++));
    pub.system_event(t, wire::feed::SystemCode::StartOfSession);
  }

  void run(unsigned seed, int n, bool flush_each = true) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> op(0, 9);
    std::uniform_int_distribution<Price> px(95, 105);
    std::uniform_int_distribution<Qty> qty(1, 20);
    std::uniform_int_distribution<SymbolId> sym(0, kSymbols - 1);
    std::vector<ClientOrderId> live;
    ClientOrderId next_cl = 1;
    for (int i = 0; i < n; ++i) {
      Inbound r;
      r.session = 1;
      const int o = op(rng);
      if (o < 6 || live.empty()) {
        r.kind = InboundKind::NewOrder;
        r.new_order = NewOrder{next_cl, sym(rng), o % 2 ? Side::Buy : Side::Sell, OrderType::Limit,
                               o == 5 ? TimeInForce::IOC : TimeInForce::Day, px(rng), qty(rng)};
        live.push_back(next_cl++);
      } else if (o < 8) {
        std::uniform_int_distribution<std::size_t> pick(0, live.size() - 1);
        const auto idx = pick(rng);
        r.kind = InboundKind::Cancel;
        r.cancel = CancelOrder{live[idx], o == 7 ? 1u : 0u};
        if (o == 6) live.erase(live.begin() + static_cast<std::ptrdiff_t>(idx));
      } else {
        std::uniform_int_distribution<std::size_t> pick(0, live.size() - 1);
        const auto idx = pick(rng);
        r.kind = InboundKind::Replace;
        r.replace = ReplaceOrder{live[idx], next_cl, px(rng), qty(rng)};
        live[idx] = next_cl++;
      }
      engine.apply(seq.append(r, t++));
      if (flush_each) pub.flush();
    }
    pub.system_event(t, wire::feed::SystemCode::EndOfSession);
    pub.flush();
  }
};

} // namespace

TEST(feed_in_order_delivery_rebuilds_book) {
  Scenario s;
  s.run(1, 5000);
  FeedHandler h(Scenario::kSymbols);
  int requests = 0;
  h.set_retrans_requester([&](SeqNo, std::uint16_t) { ++requests; });
  for (const auto& p : s.packets) h.on_packet(p);
  CHECK_EQ(requests, 0);
  CHECK(h.end_of_session());
  CHECK_EQ(h.next_seq(), s.pub.next_seq());
  CHECK_EQ(h.books_hash(), s.engine.books_hash());
  CHECK_EQ(h.stats().gaps, std::uint64_t{0});
  CHECK(s.engine.stats().fills > 100);
  for (std::uint32_t i = 0; i < Scenario::kSymbols; ++i) {
    CHECK_EQ(h.book(i).live_orders(), s.engine.book(i).live_orders());
    std::string why;
    CHECK_MSG(h.book(i).check_invariants(&why), why);
  }
}

TEST(feed_lost_packets_recovered_by_retransmission) {
  Scenario s;
  s.run(2, 5000);
  FeedHandler h(Scenario::kSymbols);
  std::vector<Packet> retrans;
  h.set_retrans_requester([&](SeqNo start, std::uint16_t count) {
    CHECK(s.pub.retransmit(start, count, [&](std::span<const std::byte> p) { retrans.emplace_back(p.begin(), p.end()); }));
  });
  std::size_t dropped = 0;
  for (std::size_t i = 0; i < s.packets.size(); ++i) {
    if (i % 7 == 3) {
      ++dropped;
      continue;
    }
    h.on_packet(s.packets[i]);
    // Serve any retransmissions the handler asked for.
    std::vector<Packet> pending;
    pending.swap(retrans);
    for (const auto& p : pending) h.on_packet(p);
  }
  CHECK(dropped > 100);
  CHECK(h.end_of_session());
  CHECK(!h.gap_outstanding());
  CHECK_EQ(h.stats().gaps, std::uint64_t(dropped));
  CHECK(h.stats().retrans_requests >= dropped);
  CHECK(h.stats().recovered >= dropped);
  CHECK_EQ(h.next_seq(), s.pub.next_seq());
  CHECK_EQ(h.books_hash(), s.engine.books_hash());
}

TEST(feed_reordered_and_duplicated_packets) {
  Scenario s;
  s.run(3, 3000);
  FeedHandler h(Scenario::kSymbols);
  int requests = 0;
  h.set_retrans_requester([&](SeqNo, std::uint16_t) { ++requests; });
  // Deliver in pairs swapped, and every packet twice.
  for (std::size_t i = 0; i + 1 < s.packets.size(); i += 2) {
    h.on_packet(s.packets[i + 1]);
    h.on_packet(s.packets[i]);
    h.on_packet(s.packets[i + 1]);
    h.on_packet(s.packets[i]);
  }
  if (s.packets.size() % 2) h.on_packet(s.packets.back());
  CHECK(!h.gap_outstanding());
  CHECK(h.stats().duplicates > 0);
  CHECK_EQ(h.next_seq(), s.pub.next_seq());
  CHECK_EQ(h.books_hash(), s.engine.books_hash());
  // Requests may have gone out for the transient gaps; they were satisfied by
  // the swapped packet before any reply was needed.
  CHECK(requests <= static_cast<int>(s.packets.size()));
}

TEST(feed_snapshot_when_behind_the_ring) {
  Scenario s;
  s.run(4, 4000);
  // Publisher keeps only the last few hundred messages, so a handler that
  // joins late cannot be served by retransmission.
  FeedPublisher small(77, 300);
  Engine replay(Scenario::kSymbols, &small);
  for (const Inbound& r : s.seq.log()) replay.apply(r);
  small.flush();
  CHECK(small.oldest_retained() > 1);

  FeedHandler h(Scenario::kSymbols);
  int snapshots_served = 0;
  h.set_retrans_requester([&](SeqNo start, std::uint16_t count) {
    std::vector<Packet> out;
    const bool ok = small.retransmit(start, count, [&](std::span<const std::byte> p) { out.emplace_back(p.begin(), p.end()); });
    if (!ok) {
      ++snapshots_served;
      small.snapshot(replay, 999, [&](std::span<const std::byte> p) { out.emplace_back(p.begin(), p.end()); });
    }
    for (const auto& p : out) h.on_packet(p);
  });
  // The handler only sees the very last live packet, far past what the ring
  // still holds.
  h.on_packet(s.packets.back());
  CHECK_EQ(snapshots_served, 1);
  CHECK_EQ(h.stats().snapshots, std::uint64_t{1});
  CHECK(!h.gap_outstanding());
  CHECK_EQ(h.next_seq(), small.next_seq());
  CHECK_EQ(h.books_hash(), replay.books_hash());
  CHECK_EQ(replay.books_hash(), s.engine.books_hash());
}

TEST(feed_packets_fill_to_mtu_when_not_flushed) {
  Scenario s;
  s.run(5, 2000, false);
  CHECK(s.packets.size() < 200);
  std::size_t max_len = 0;
  for (const auto& p : s.packets) max_len = std::max(max_len, p.size());
  CHECK(max_len <= wire::feed::kMaxPacket);
  CHECK(max_len > wire::feed::kMaxPacket - 40);
  FeedHandler h(Scenario::kSymbols);
  for (const auto& p : s.packets) h.on_packet(p);
  CHECK_EQ(h.books_hash(), s.engine.books_hash());
}

TEST(feed_timer_reissues_unanswered_request) {
  Scenario s;
  s.run(6, 200);
  FeedHandler h(Scenario::kSymbols, 1000);
  int requests = 0;
  h.set_retrans_requester([&](SeqNo, std::uint16_t) { ++requests; });
  h.on_packet(s.packets[0], 10);
  h.on_packet(s.packets[2], 20); // packet 1 missing
  CHECK_EQ(requests, 1);
  h.on_timer(500);
  CHECK_EQ(requests, 1);
  h.on_timer(2000);
  CHECK_EQ(requests, 2);
  h.on_packet(s.packets[1], 2100);
  CHECK(!h.gap_outstanding());
  h.on_timer(9000);
  CHECK_EQ(requests, 2);
}
