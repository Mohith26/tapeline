#pragma once

#include "book.hpp"
#include "hash.hpp"
#include "types.hpp"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace tapeline {

// The matching engine is a deterministic state machine over the sequenced
// inbound log: same records in, same events and same book state out. It never
// reads a clock, never allocates ids from anything but its own counters, and
// never sees a socket. That is what lets the standby replica in replica.hpp
// follow the primary and what makes the replay tests meaningful.
class Engine {
 public:
  struct Stats {
    std::uint64_t records = 0;
    std::uint64_t orders = 0;
    std::uint64_t fills = 0;
    std::uint64_t volume = 0;
    std::uint64_t cancels = 0;
    std::uint64_t replaces = 0;
    std::uint64_t rejects = 0;
  };

  explicit Engine(std::uint32_t n_symbols, EventSink* sink = nullptr) : sink_(sink) {
    books_.reserve(n_symbols);
    for (std::uint32_t s = 0; s < n_symbols; ++s) books_.emplace_back(s);
    refs_.emplace_back(); // OrderId 0 is never handed out
  }

  void set_sink(EventSink* sink) noexcept { sink_ = sink; }

  void apply(const Inbound& rec) {
    ++stats_.records;
    switch (rec.kind) {
      case InboundKind::SessionOpen: on_session_open(rec); break;
      case InboundKind::SessionClose: on_session_close(rec); break;
      case InboundKind::NewOrder: on_new(rec); break;
      case InboundKind::Cancel: on_cancel(rec); break;
      case InboundKind::Replace: on_replace(rec); break;
      case InboundKind::EndOfSession: break;
    }
  }

  [[nodiscard]] const Book& book(SymbolId s) const { return books_[s]; }
  [[nodiscard]] std::uint32_t symbols() const noexcept {
    return static_cast<std::uint32_t>(books_.size());
  }
  [[nodiscard]] const Stats& stats() const noexcept { return stats_; }
  [[nodiscard]] OrderId next_order_id() const noexcept { return next_oid_; }

  [[nodiscard]] std::optional<Book::Order> find(OrderId oid) const {
    if (oid >= refs_.size() || !refs_[oid].live) return std::nullopt;
    return books_[refs_[oid].symbol].at(refs_[oid].slot);
  }

  [[nodiscard]] bool session_open(SessionId s) const {
    return s < sessions_.size() && sessions_[s].open;
  }

  [[nodiscard]] std::size_t live_orders(SessionId s) const {
    return s < sessions_.size() ? sessions_[s].live.size() : 0;
  }

  // Fingerprint of the resting state alone; a feed handler can compute the
  // same value from the market data it received.
  [[nodiscard]] std::uint64_t books_hash() const {
    Fnv1a64 h;
    for (const Book& b : books_) h.add(b.hash());
    return h.value();
  }

  // Fingerprint of everything that matters: every book plus the id counters.
  [[nodiscard]] std::uint64_t state_hash() const {
    Fnv1a64 h;
    for (const Book& b : books_) h.add(b.hash());
    h.add(next_oid_);
    h.add(next_match_);
    return h.value();
  }

  bool check_invariants(std::string* why = nullptr) const {
    for (const Book& b : books_) {
      if (!b.check_invariants(why)) return false;
    }
    return true;
  }

 private:
  struct Ref {
    SymbolId symbol{};
    std::uint32_t slot{Book::kNil};
    bool live{false};
  };

  struct Session {
    bool open = false;
    std::unordered_map<ClientOrderId, OrderId> live;
  };

  Event base(const Inbound& rec, EventType type) const {
    Event ev;
    ev.type = type;
    ev.seq = rec.seq;
    ev.ts = rec.ts;
    ev.session = rec.session;
    return ev;
  }

  void emit(const Event& ev) {
    if (sink_) sink_->on_event(ev);
  }

  void reject(const Inbound& rec, ClientOrderId cl_id, RejectReason why) {
    ++stats_.rejects;
    Event ev = base(rec, EventType::Rejected);
    ev.cl_id = cl_id;
    ev.reject = why;
    emit(ev);
  }

  Session& session_slot(SessionId s) {
    if (s >= sessions_.size()) sessions_.resize(static_cast<std::size_t>(s) + 1);
    return sessions_[s];
  }

  void on_session_open(const Inbound& rec) { session_slot(rec.session).open = true; }

  // Cancel-on-disconnect: every live order of the session comes off the book,
  // in exchange-id order so the replica produces the same event stream.
  void on_session_close(const Inbound& rec) {
    Session& s = session_slot(rec.session);
    std::vector<OrderId> ids;
    ids.reserve(s.live.size());
    for (const auto& [cl, oid] : s.live) ids.push_back(oid);
    std::sort(ids.begin(), ids.end());
    for (OrderId oid : ids) {
      const Ref ref = refs_[oid];
      Book::Order o = books_[ref.symbol].remove(ref.slot);
      refs_[oid].live = false;
      s.live.erase(o.cl_id);
      ++stats_.cancels;
      Event ev = base(rec, EventType::Canceled);
      fill_order_fields(ev, o, ref.symbol);
      ev.qty = o.qty;
      ev.leaves = 0;
      ev.was_resting = true;
      ev.cancel_reason = CancelReason::SessionClosed;
      emit(ev);
    }
    s.open = false;
  }

  static void fill_order_fields(Event& ev, const Book::Order& o, SymbolId symbol) {
    ev.cl_id = o.cl_id;
    ev.oid = o.id;
    ev.symbol = symbol;
    ev.side = o.side;
    ev.price = o.price;
  }

  void on_new(const Inbound& rec) {
    const NewOrder& n = rec.new_order;
    if (!session_open(rec.session)) return reject(rec, n.cl_id, RejectReason::NotLoggedIn);
    if (n.symbol >= books_.size()) return reject(rec, n.cl_id, RejectReason::UnknownSymbol);
    if (n.qty == 0) return reject(rec, n.cl_id, RejectReason::BadQuantity);
    if (n.type == OrderType::Limit && n.price < kMinPrice) {
      return reject(rec, n.cl_id, RejectReason::BadPrice);
    }
    Session& s = sessions_[rec.session];
    if (s.live.contains(n.cl_id)) return reject(rec, n.cl_id, RejectReason::DuplicateClientOrderId);

    const OrderId oid = next_oid_++;
    refs_.push_back(Ref{n.symbol, Book::kNil, false});
    ++stats_.orders;

    const Price price = n.type == OrderType::Market ? 0 : n.price;
    Event acc = base(rec, EventType::Accepted);
    acc.cl_id = n.cl_id;
    acc.oid = oid;
    acc.symbol = n.symbol;
    acc.side = n.side;
    acc.price = price;
    acc.qty = n.qty;
    acc.leaves = n.qty;
    acc.tif = n.tif;
    emit(acc);

    place(rec, n.cl_id, oid, n.symbol, n.side, n.type, n.tif, price, n.qty);
  }

  // Run an order through the book: match what crosses, then rest or cancel the
  // remainder depending on type and time in force.
  void place(const Inbound& rec, ClientOrderId cl_id, OrderId oid, SymbolId symbol, Side side,
             OrderType type, TimeInForce tif, Price price, Qty qty) {
    Book& book = books_[symbol];
    const Price limit = type == OrderType::Market ? (side == Side::Buy ? kMaxPrice : kMinPrice) : price;
    Qty running = qty;

    const Qty leaves = book.match(side, limit, qty, [&](const Book::Order& resting, Qty fill, Price px) {
      running -= fill;
      ++stats_.fills;
      stats_.volume += fill;
      Event ev = base(rec, EventType::Executed);
      ev.cl_id = cl_id;
      ev.oid = oid;
      ev.symbol = symbol;
      ev.side = side;
      ev.price = px;
      ev.qty = fill;
      ev.leaves = running;
      ev.resting_oid = resting.id;
      ev.resting_session = resting.session;
      ev.resting_cl_id = resting.cl_id;
      ev.resting_leaves = resting.qty;
      ev.match_id = next_match_++;
      if (resting.qty == 0) {
        refs_[resting.id].live = false;
        sessions_[resting.session].live.erase(resting.cl_id);
      }
      emit(ev);
    });

    if (leaves == 0) return;

    if (type == OrderType::Limit && tif == TimeInForce::Day) {
      const std::uint32_t slot = book.rest(oid, side, price, leaves, rec.session, cl_id);
      refs_[oid] = Ref{symbol, slot, true};
      sessions_[rec.session].live.emplace(cl_id, oid);
      Event ev = base(rec, EventType::Rested);
      ev.cl_id = cl_id;
      ev.oid = oid;
      ev.symbol = symbol;
      ev.side = side;
      ev.price = price;
      ev.qty = leaves;
      ev.leaves = leaves;
      ev.tif = tif;
      emit(ev);
      return;
    }

    ++stats_.cancels;
    Event ev = base(rec, EventType::Canceled);
    ev.cl_id = cl_id;
    ev.oid = oid;
    ev.symbol = symbol;
    ev.side = side;
    ev.price = price;
    ev.qty = leaves;
    ev.leaves = 0;
    ev.was_resting = false;
    ev.cancel_reason = type == OrderType::Market ? CancelReason::MarketNoLiquidity
                                                 : CancelReason::ImmediateOrCancel;
    emit(ev);
  }

  void on_cancel(const Inbound& rec) {
    const CancelOrder& c = rec.cancel;
    if (!session_open(rec.session)) return reject(rec, c.cl_id, RejectReason::NotLoggedIn);
    Session& s = sessions_[rec.session];
    auto it = s.live.find(c.cl_id);
    if (it == s.live.end()) return reject(rec, c.cl_id, RejectReason::UnknownOrder);
    const OrderId oid = it->second;
    const Ref ref = refs_[oid];
    Book& book = books_[ref.symbol];
    ++stats_.cancels;
    Event ev = base(rec, EventType::Canceled);
    ev.was_resting = true;
    ev.cancel_reason = CancelReason::UserRequested;
    const Book::Order& cur = book.at(ref.slot);
    if (c.qty == 0 || c.qty >= cur.qty) {
      const Book::Order o = book.remove(ref.slot);
      refs_[oid].live = false;
      s.live.erase(it);
      fill_order_fields(ev, o, ref.symbol);
      ev.qty = o.qty;
      ev.leaves = 0;
    } else {
      fill_order_fields(ev, cur, ref.symbol);
      ev.qty = c.qty;
      ev.leaves = book.reduce(ref.slot, c.qty);
    }
    emit(ev);
  }

  void on_replace(const Inbound& rec) {
    const ReplaceOrder& r = rec.replace;
    if (!session_open(rec.session)) return reject(rec, r.cl_id, RejectReason::NotLoggedIn);
    Session& s = sessions_[rec.session];
    auto it = s.live.find(r.cl_id);
    if (it == s.live.end()) return reject(rec, r.cl_id, RejectReason::UnknownOrder);
    if (r.qty == 0) return reject(rec, r.cl_id, RejectReason::BadQuantity);
    if (r.price < kMinPrice) return reject(rec, r.cl_id, RejectReason::BadPrice);
    if (r.new_cl_id != r.cl_id && s.live.contains(r.new_cl_id)) {
      return reject(rec, r.cl_id, RejectReason::DuplicateClientOrderId);
    }
    const OrderId oid = it->second;
    const Ref ref = refs_[oid];
    Book& book = books_[ref.symbol];
    ++stats_.replaces;

    const Book::Order old = book.at(ref.slot);
    Event ev = base(rec, EventType::Replaced);
    ev.old_oid = oid;
    ev.old_cl_id = r.cl_id;
    ev.cl_id = r.new_cl_id;
    ev.symbol = ref.symbol;
    ev.side = old.side;
    ev.price = r.price;
    ev.qty = r.qty;
    ev.leaves = r.qty;
    ev.prev_qty = old.qty;

    if (r.price == old.price && r.qty < old.qty) {
      // Shrinking in place keeps queue position; this is the one replace shape
      // that does not lose priority.
      book.reduce(ref.slot, old.qty - r.qty);
      book.rebind(ref.slot, r.new_cl_id);
      s.live.erase(it);
      s.live.emplace(r.new_cl_id, oid);
      ev.oid = oid;
      ev.kept_priority = true;
      emit(ev);
      return;
    }

    book.remove(ref.slot);
    refs_[oid].live = false;
    s.live.erase(it);
    const OrderId new_oid = next_oid_++;
    refs_.push_back(Ref{ref.symbol, Book::kNil, false});
    ev.oid = new_oid;
    ev.kept_priority = false;
    emit(ev);
    place(rec, r.new_cl_id, new_oid, ref.symbol, old.side, OrderType::Limit, TimeInForce::Day,
          r.price, r.qty);
  }

  std::vector<Book> books_;
  std::vector<Ref> refs_;
  std::vector<Session> sessions_;
  OrderId next_oid_ = 1;
  MatchId next_match_ = 1;
  EventSink* sink_;
  Stats stats_;
};

} // namespace tapeline
