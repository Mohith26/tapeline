#include "tapeline/book.hpp"
#include "tapeline/engine.hpp"
#include "tapeline/replica.hpp"
#include "tapeline/sequencer.hpp"
#include "test.hpp"

#include <algorithm>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace tapeline;

namespace {

std::vector<Inbound> random_flow(unsigned seed, int n, std::uint32_t symbols, SessionId sessions) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> op(0, 11);
  std::uniform_int_distribution<Price> px(90, 110);
  std::uniform_int_distribution<Qty> qty(1, 50);
  std::uniform_int_distribution<SymbolId> sym(0, symbols - 1);
  std::uniform_int_distribution<SessionId> ses(1, sessions);
  std::vector<Inbound> out;
  std::vector<std::vector<ClientOrderId>> live(sessions + 1);
  std::vector<ClientOrderId> next_cl(sessions + 1, 1);
  for (SessionId s = 1; s <= sessions; ++s) {
    Inbound r;
    r.kind = InboundKind::SessionOpen;
    r.session = s;
    out.push_back(r);
  }
  for (int i = 0; i < n; ++i) {
    Inbound r;
    r.session = ses(rng);
    auto& mine = live[r.session];
    const int o = op(rng);
    if (o < 6 || mine.empty()) {
      r.kind = InboundKind::NewOrder;
      const OrderType type = o == 4 ? OrderType::Market : OrderType::Limit;
      const TimeInForce tif = o == 5 ? TimeInForce::IOC : TimeInForce::Day;
      r.new_order = NewOrder{next_cl[r.session], sym(rng), o % 2 ? Side::Buy : Side::Sell, type, tif,
                             type == OrderType::Market ? 0 : px(rng), qty(rng)};
      if (type == OrderType::Limit && tif == TimeInForce::Day) mine.push_back(next_cl[r.session]);
      ++next_cl[r.session];
    } else if (o < 9) {
      std::uniform_int_distribution<std::size_t> pick(0, mine.size() - 1);
      const auto idx = pick(rng);
      r.kind = InboundKind::Cancel;
      r.cancel = CancelOrder{mine[idx], o == 8 ? 2u : 0u};
      if (o != 8) mine.erase(mine.begin() + static_cast<std::ptrdiff_t>(idx));
    } else if (o < 11) {
      std::uniform_int_distribution<std::size_t> pick(0, mine.size() - 1);
      const auto idx = pick(rng);
      r.kind = InboundKind::Replace;
      r.replace = ReplaceOrder{mine[idx], next_cl[r.session], px(rng), qty(rng)};
      mine[idx] = next_cl[r.session]++;
    } else {
      // Occasionally a session drops and reconnects.
      r.kind = InboundKind::SessionClose;
      out.push_back(r);
      mine.clear();
      r.kind = InboundKind::SessionOpen;
    }
    out.push_back(r);
  }
  return out;
}

struct Counter : EventSink {
  std::uint64_t n = 0;
  void on_event(const Event&) override { ++n; }
};

// A deliberately naive order book: a flat list of orders, matching by
// scanning for the best price and earliest arrival. Slow, obviously right,
// and independent of every data structure in book.hpp.
struct RefBook {
  struct O {
    OrderId id;
    Side side;
    Price price;
    Qty qty;
    std::uint64_t arrival;
  };
  std::vector<O> orders;
  std::uint64_t clock = 0;

  void rest(OrderId id, Side side, Price price, Qty qty) { orders.push_back({id, side, price, qty, clock++}); }

  template <class F>
  Qty match(Side side, Price limit, Qty qty, F&& on_fill) {
    while (qty > 0) {
      std::size_t best = orders.size();
      for (std::size_t i = 0; i < orders.size(); ++i) {
        const O& o = orders[i];
        if (o.side == side) continue;
        const bool crosses = side == Side::Buy ? o.price <= limit : o.price >= limit;
        if (!crosses) continue;
        if (best == orders.size()) {
          best = i;
          continue;
        }
        const O& b = orders[best];
        const bool better_price = side == Side::Buy ? o.price < b.price : o.price > b.price;
        if (better_price || (o.price == b.price && o.arrival < b.arrival)) best = i;
      }
      if (best == orders.size()) break;
      O& o = orders[best];
      const Qty fill = std::min(qty, o.qty);
      o.qty -= fill;
      qty -= fill;
      on_fill(o.id, fill, o.price, o.qty);
      if (o.qty == 0) orders.erase(orders.begin() + static_cast<std::ptrdiff_t>(best));
    }
    return qty;
  }

  void remove(OrderId id) {
    orders.erase(std::remove_if(orders.begin(), orders.end(), [id](const O& o) { return o.id == id; }), orders.end());
  }
  void reduce(OrderId id, Qty by) {
    for (O& o : orders) {
      if (o.id == id) o.qty -= by;
    }
  }
};

} // namespace

TEST(replay_two_engines_agree_on_events_and_state) {
  const auto flow = random_flow(11, 20000, 4, 5);
  Sequencer seq;
  EventHasher ha;
  Engine a(4, &ha);
  Timestamp t = 100;
  for (const Inbound& r : flow) a.apply(seq.append(r, t += 7));
  std::string why;
  CHECK_MSG(a.check_invariants(&why), why);
  CHECK(a.stats().fills > 1000);
  CHECK(a.stats().replaces > 500);

  Replica b(4);
  CHECK_EQ(b.follow(seq.log()), seq.size());
  CHECK_EQ(b.applied(), seq.size());
  CHECK(b.in_sync_with(a));
  CHECK_EQ(b.event_hash(), ha.value());
  CHECK_EQ(b.engine().stats().fills, a.stats().fills);

  // Feeding the log a second time is a no-op for the replica.
  CHECK_EQ(b.follow(seq.log()), std::size_t{0});
  CHECK(b.in_sync_with(a));
}

TEST(replay_replica_can_catch_up_midway_and_take_over) {
  const auto flow = random_flow(12, 6000, 2, 3);
  Sequencer seq;
  Counter c;
  Engine primary(2, &c);
  Replica standby(2);
  Timestamp t = 1;
  std::size_t i = 0;
  for (; i < flow.size() / 2; ++i) primary.apply(seq.append(flow[i], t++));
  standby.follow(seq.log());
  CHECK(standby.in_sync_with(primary));
  for (; i < flow.size(); ++i) primary.apply(seq.append(flow[i], t++));
  CHECK(!standby.in_sync_with(primary));
  standby.follow(seq.log());
  CHECK(standby.in_sync_with(primary));
  // Failover: route fresh records at the standby only.
  const auto more = random_flow(13, 1000, 2, 3);
  for (const Inbound& r : more) {
    if (r.kind == InboundKind::SessionOpen) continue; // sessions already exist
    standby.engine().apply(seq.append(r, t++));
  }
  CHECK(standby.engine().stats().records > primary.stats().records);
  std::string why;
  CHECK_MSG(standby.engine().check_invariants(&why), why);
}

TEST(replay_log_file_roundtrip) {
  const auto flow = random_flow(14, 3000, 3, 2);
  const std::string path = "/tmp/tapeline_test_log.bin";
  Sequencer seq;
  CHECK(seq.open_file(path));
  Engine a(3);
  Timestamp t = 5;
  for (const Inbound& r : flow) a.apply(seq.append(r, t++));
  seq.close_file();
  auto loaded = Sequencer::load(path);
  CHECK(loaded.has_value());
  if (!loaded) return;
  CHECK_EQ(loaded->size(), seq.size());
  Engine b(3);
  for (const Inbound& r : *loaded) b.apply(r);
  CHECK_EQ(a.state_hash(), b.state_hash());
  CHECK_EQ(loaded->front().seq, std::uint64_t{1});
  CHECK_EQ(loaded->back().seq, seq.size());
  std::remove(path.c_str());
  CHECK(!Sequencer::load("/tmp/definitely_missing_tapeline.bin").has_value());
}

TEST(replay_book_matches_naive_reference) {
  for (unsigned seed = 1; seed <= 20; ++seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> op(0, 9);
    std::uniform_int_distribution<Price> px(50, 60);
    std::uniform_int_distribution<Qty> qty(1, 9);
    Book book(0);
    RefBook ref;
    struct Live {
      OrderId id;
      std::uint32_t slot;
    };
    std::vector<Live> live;
    OrderId next = 1;
    std::uint64_t fills = 0;
    for (int i = 0; i < 4000; ++i) {
      const int o = op(rng);
      if (o < 6 || live.empty()) {
        const Side side = o % 2 ? Side::Buy : Side::Sell;
        const Price p = o == 5 ? (side == Side::Buy ? kMaxPrice : kMinPrice) : px(rng);
        const Qty q = qty(rng);
        std::vector<std::tuple<OrderId, Qty, Price, Qty>> a, b;
        const Qty la = book.match(side, p, q, [&](const Book::Order& r, Qty f, Price at) { a.emplace_back(r.id, f, at, r.qty); });
        const Qty lb = ref.match(side, p, q, [&](OrderId id, Qty f, Price at, Qty left) { b.emplace_back(id, f, at, left); });
        CHECK(a == b);
        CHECK_EQ(la, lb);
        fills += a.size();
        for (const auto& [id, f, at, left] : a) {
          if (left == 0) live.erase(std::remove_if(live.begin(), live.end(), [id](const Live& l) { return l.id == id; }), live.end());
        }
        if (la > 0 && o != 5) {
          const OrderId id = next++;
          const auto slot = book.rest(id, side, p, la, 1, id);
          ref.rest(id, side, p, la);
          live.push_back({id, slot});
        }
      } else if (o < 8) {
        std::uniform_int_distribution<std::size_t> pick(0, live.size() - 1);
        const auto idx = pick(rng);
        book.remove(live[idx].slot);
        ref.remove(live[idx].id);
        live.erase(live.begin() + static_cast<std::ptrdiff_t>(idx));
      } else {
        std::uniform_int_distribution<std::size_t> pick(0, live.size() - 1);
        const auto idx = pick(rng);
        const Qty cur = book.at(live[idx].slot).qty;
        if (cur > 1) {
          book.reduce(live[idx].slot, 1);
          ref.reduce(live[idx].id, 1);
        }
      }
      if (i % 97 == 0) {
        std::string why;
        CHECK_MSG(book.check_invariants(&why), why);
      }
    }
    // Final state: same resting orders, same quantities, same order.
    std::vector<std::tuple<OrderId, Price, Qty>> mine;
    book.for_each_resting([&](const Book::Order& r) { mine.emplace_back(r.id, r.price, r.qty); });
    std::vector<std::tuple<OrderId, Price, Qty>> theirs;
    std::stable_sort(ref.orders.begin(), ref.orders.end(), [](const RefBook::O& x, const RefBook::O& y) {
      if (x.side != y.side) return x.side == Side::Buy;
      if (x.price != y.price) return x.side == Side::Buy ? x.price > y.price : x.price < y.price;
      return x.arrival < y.arrival;
    });
    for (const auto& r : ref.orders) theirs.emplace_back(r.id, r.price, r.qty);
    CHECK(mine == theirs);
    CHECK(fills > 500);
  }
}
