#pragma once

#include "hash.hpp"
#include "types.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <flat_map>
#include <format>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace tapeline {

// Price-time priority limit order book for one symbol.
//
// Orders live in a slab (a vector plus a free list) and are addressed by slot
// index, so the book never allocates per order once it has warmed up. Each
// price level is an intrusive FIFO of slots. Levels sit in std::flat_map so the
// ladder is one contiguous array with the best level at the back: bids are
// sorted ascending and asks descending, which makes "best level" a pop from the
// end and keeps the inserts that matter (near the top of book) cheap.
class Book {
 public:
  static constexpr std::uint32_t kNil = 0xffffffffu;

  struct Order {
    OrderId id{};
    Price price{};
    Qty qty{};
    Side side{Side::Buy};
    SessionId session{};
    ClientOrderId cl_id{};
    std::uint32_t prev{kNil};
    std::uint32_t next{kNil};
  };

  struct Level {
    Price price{};
    Qty total{};
    std::uint32_t count{};
    std::uint32_t head{kNil};
    std::uint32_t tail{kNil};
  };

  explicit Book(SymbolId symbol, std::size_t reserve_orders = std::size_t{1} << 14)
      : symbol_(symbol) {
    slab_.reserve(reserve_orders);
    free_.reserve(reserve_orders);
  }

  [[nodiscard]] SymbolId symbol() const noexcept { return symbol_; }

  // Put an order on the book at the tail of its price level. Returns the slot.
  std::uint32_t rest(OrderId id, Side side, Price price, Qty qty, SessionId session,
                     ClientOrderId cl_id) {
    assert(qty > 0);
    const std::uint32_t slot = alloc();
    Level& lvl = level_for_insert(side, price);
    Order& o = slab_[slot];
    o.id = id;
    o.price = price;
    o.qty = qty;
    o.side = side;
    o.session = session;
    o.cl_id = cl_id;
    o.prev = lvl.tail;
    o.next = kNil;
    if (lvl.tail == kNil) {
      lvl.head = slot;
    } else {
      slab_[lvl.tail].next = slot;
    }
    lvl.tail = slot;
    lvl.total += qty;
    lvl.count += 1;
    ++live_;
    return slot;
  }

  // Match an incoming order of `side` against the opposite side of the book,
  // walking levels from the best price outward and orders within a level in
  // time priority. `limit` is the worst price the aggressor accepts (use
  // kMaxPrice / kMinPrice for a market order). on_fill(resting, fill_qty, price)
  // is called for every fill with the resting order's qty already reduced; it
  // must not touch the book. Returns the aggressor's unfilled quantity.
  template <class F>
  Qty match(Side side, Price limit, Qty qty, F&& on_fill) {
    if (side == Side::Buy) {
      return match_side(asks_, [limit](Price p) { return p <= limit; }, qty, on_fill);
    }
    return match_side(bids_, [limit](Price p) { return p >= limit; }, qty, on_fill);
  }

  // Take `by` off a resting order without moving it in the queue. Returns what
  // is left. The caller must make sure `by` is strictly less than the resting
  // quantity; use remove() to take the whole order off.
  Qty reduce(std::uint32_t slot, Qty by) {
    Order& o = slab_[slot];
    assert(by < o.qty);
    o.qty -= by;
    find_level(o.side, o.price).total -= by;
    return o.qty;
  }

  // Take an order off the book entirely. Returns a copy of it.
  Order remove(std::uint32_t slot) {
    const Order o = slab_[slot];
    if (o.side == Side::Buy) {
      remove_from(bids_, slot, o);
    } else {
      remove_from(asks_, slot, o);
    }
    release(slot);
    return o;
  }

  [[nodiscard]] const Order& at(std::uint32_t slot) const { return slab_[slot]; }

  // A replace that only shrinks quantity keeps its place in the queue but
  // takes on the client's new order id.
  void rebind(std::uint32_t slot, ClientOrderId cl_id) { slab_[slot].cl_id = cl_id; }

  [[nodiscard]] std::optional<Price> best_bid() const {
    if (bids_.empty()) return std::nullopt;
    return (*std::prev(bids_.end())).first;
  }

  [[nodiscard]] std::optional<Price> best_ask() const {
    if (asks_.empty()) return std::nullopt;
    return (*std::prev(asks_.end())).first;
  }

  [[nodiscard]] const Level* best_level(Side side) const {
    if (side == Side::Buy) {
      if (bids_.empty()) return nullptr;
      return &(*std::prev(bids_.end())).second;
    }
    if (asks_.empty()) return nullptr;
    return &(*std::prev(asks_.end())).second;
  }

  [[nodiscard]] std::size_t live_orders() const noexcept { return live_; }
  [[nodiscard]] std::size_t levels(Side side) const noexcept {
    return side == Side::Buy ? bids_.size() : asks_.size();
  }

  // Top-of-book ladder, best price first.
  [[nodiscard]] std::vector<std::pair<Price, Qty>> depth(Side side, std::size_t n) const {
    std::vector<std::pair<Price, Qty>> out;
    auto walk = [&](const auto& m) {
      for (auto it = m.rbegin(); it != m.rend() && out.size() < n; ++it) {
        out.emplace_back((*it).first, (*it).second.total);
      }
    };
    if (side == Side::Buy) walk(bids_); else walk(asks_);
    return out;
  }

  // Visit every resting order: bids from best to worst, then asks from best to
  // worst, FIFO within each level. This is the canonical order for hashing.
  template <class F>
  void for_each_resting(F&& f) const {
    auto walk = [&](const auto& m) {
      for (auto it = m.rbegin(); it != m.rend(); ++it) {
        for (std::uint32_t s = (*it).second.head; s != kNil; s = slab_[s].next) f(slab_[s]);
      }
    };
    walk(bids_);
    walk(asks_);
  }

  // Deterministic fingerprint of the resting state. The Python decoder in
  // tools/feedtap.py computes the same value from the feed alone.
  [[nodiscard]] std::uint64_t hash() const {
    Fnv1a64 h;
    for_each_resting([&](const Order& o) {
      h.add(symbol_);
      h.add(o.side);
      h.add(o.price);
      h.add(o.id);
      h.add(o.qty);
    });
    return h.value();
  }

  // Structural self-check used by the tests and the fuzzer. Expensive.
  bool check_invariants(std::string* why = nullptr) const {
    auto fail = [&](std::string msg) {
      if (why) *why = std::move(msg);
      return false;
    };
    if (auto bb = best_bid(), ba = best_ask(); bb && ba && *bb >= *ba) {
      return fail(std::format("crossed book: bid {} >= ask {}", *bb, *ba));
    }
    std::size_t seen = 0;
    auto check_map = [&](const auto& m, Side side) {
      Price prev = 0;
      bool first = true;
      for (const auto& [px, lvl] : m) {
        if (!first) {
          const bool ok = side == Side::Buy ? px > prev : px < prev;
          if (!ok) return fail(std::format("ladder out of order at {}", px));
        }
        first = false;
        prev = px;
        if (lvl.price != px) return fail("level price mismatch");
        if (lvl.count == 0) return fail(std::format("empty level {} left behind", px));
        Qty total = 0;
        std::uint32_t count = 0;
        std::uint32_t last = kNil;
        for (std::uint32_t s = lvl.head; s != kNil; s = slab_[s].next) {
          const Order& o = slab_[s];
          if (o.prev != last) return fail("broken prev link");
          if (o.qty == 0) return fail("zero-qty resting order");
          if (o.price != px || o.side != side) return fail("order on wrong level");
          total += o.qty;
          ++count;
          last = s;
        }
        if (last != lvl.tail) return fail("tail mismatch");
        if (total != lvl.total) return fail(std::format("level {} total {} != {}", px, lvl.total, total));
        if (count != lvl.count) return fail("level count mismatch");
        seen += count;
      }
      return true;
    };
    if (!check_map(bids_, Side::Buy)) return false;
    if (!check_map(asks_, Side::Sell)) return false;
    if (seen != live_) return fail(std::format("live {} != counted {}", live_, seen));
    if (live_ + free_.size() != slab_.size()) return fail("slab accounting off");
    return true;
  }

 private:
  using BidMap = std::flat_map<Price, Level>;                       // best = back
  using AskMap = std::flat_map<Price, Level, std::greater<Price>>;  // best = back

  template <class Map, class Crosses, class F>
  Qty match_side(Map& m, Crosses crosses, Qty qty, F& on_fill) {
    while (qty > 0 && !m.empty()) {
      auto it = std::prev(m.end());
      const Price px = (*it).first;
      if (!crosses(px)) break;
      Level& lvl = (*it).second;
      while (qty > 0 && lvl.head != kNil) {
        const std::uint32_t slot = lvl.head;
        Order& o = slab_[slot];
        const Qty fill = std::min(qty, o.qty);
        o.qty -= fill;
        lvl.total -= fill;
        qty -= fill;
        on_fill(static_cast<const Order&>(o), fill, px);
        if (o.qty == 0) {
          unlink(lvl, slot);
          release(slot);
        }
      }
      if (lvl.count == 0) m.erase(it);
    }
    return qty;
  }

  Level& level_for_insert(Side side, Price price) {
    if (side == Side::Buy) {
      auto [it, inserted] = bids_.try_emplace(price);
      Level& lvl = (*it).second;
      if (inserted) lvl.price = price;
      return lvl;
    }
    auto [it, inserted] = asks_.try_emplace(price);
    Level& lvl = (*it).second;
    if (inserted) lvl.price = price;
    return lvl;
  }

  Level& find_level(Side side, Price price) {
    if (side == Side::Buy) {
      auto it = bids_.find(price);
      assert(it != bids_.end());
      return (*it).second;
    }
    auto it = asks_.find(price);
    assert(it != asks_.end());
    return (*it).second;
  }

  template <class Map>
  void remove_from(Map& m, std::uint32_t slot, const Order& o) {
    auto it = m.find(o.price);
    assert(it != m.end());
    Level& lvl = (*it).second;
    lvl.total -= o.qty;
    unlink(lvl, slot);
    if (lvl.count == 0) m.erase(it);
  }

  // Detach a slot from its level's FIFO. Does not touch lvl.total.
  void unlink(Level& lvl, std::uint32_t slot) {
    Order& o = slab_[slot];
    if (o.prev != kNil) slab_[o.prev].next = o.next; else lvl.head = o.next;
    if (o.next != kNil) slab_[o.next].prev = o.prev; else lvl.tail = o.prev;
    lvl.count -= 1;
  }

  std::uint32_t alloc() {
    if (!free_.empty()) {
      const std::uint32_t s = free_.back();
      free_.pop_back();
      return s;
    }
    slab_.emplace_back();
    return static_cast<std::uint32_t>(slab_.size() - 1);
  }

  void release(std::uint32_t slot) {
    slab_[slot] = Order{};
    free_.push_back(slot);
    --live_;
  }

  SymbolId symbol_;
  std::vector<Order> slab_;
  std::vector<std::uint32_t> free_;
  BidMap bids_;
  AskMap asks_;
  std::size_t live_ = 0;
};

} // namespace tapeline
