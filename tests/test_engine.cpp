#include "tapeline/engine.hpp"
#include "tapeline/sequencer.hpp"
#include "test.hpp"

#include <algorithm>
#include <string>
#include <vector>

using namespace tapeline;

namespace {

struct Recorder : EventSink {
  std::vector<Event> events;
  void on_event(const Event& ev) override { events.push_back(ev); }
  std::size_t count(EventType t) const {
    return static_cast<std::size_t>(std::count_if(events.begin(), events.end(), [t](const Event& e) { return e.type == t; }));
  }
  const Event* last(EventType t) const {
    for (auto it = events.rbegin(); it != events.rend(); ++it) {
      if (it->type == t) return &*it;
    }
    return nullptr;
  }
};

struct Fixture {
  Recorder rec;
  Sequencer seq;
  Engine engine{4, &rec};
  Timestamp t = 1;

  void open(SessionId s) {
    Inbound r;
    r.kind = InboundKind::SessionOpen;
    r.session = s;
    engine.apply(seq.append(r, t++));
  }
  void close(SessionId s) {
    Inbound r;
    r.kind = InboundKind::SessionClose;
    r.session = s;
    engine.apply(seq.append(r, t++));
  }
  void order(SessionId s, ClientOrderId cl, Side side, Price px, Qty q, OrderType type = OrderType::Limit,
             TimeInForce tif = TimeInForce::Day, SymbolId sym = 0) {
    Inbound r;
    r.kind = InboundKind::NewOrder;
    r.session = s;
    r.new_order = NewOrder{cl, sym, side, type, tif, px, q};
    engine.apply(seq.append(r, t++));
  }
  void cancel(SessionId s, ClientOrderId cl, Qty q = 0) {
    Inbound r;
    r.kind = InboundKind::Cancel;
    r.session = s;
    r.cancel = CancelOrder{cl, q};
    engine.apply(seq.append(r, t++));
  }
  void replace(SessionId s, ClientOrderId cl, ClientOrderId ncl, Price px, Qty q) {
    Inbound r;
    r.kind = InboundKind::Replace;
    r.session = s;
    r.replace = ReplaceOrder{cl, ncl, px, q};
    engine.apply(seq.append(r, t++));
  }
};

} // namespace

TEST(engine_accept_rest_execute) {
  Fixture f;
  f.open(1);
  f.open(2);
  f.order(1, 10, Side::Buy, 100, 10);
  CHECK_EQ(f.rec.count(EventType::Accepted), std::size_t{1});
  CHECK_EQ(f.rec.count(EventType::Rested), std::size_t{1});
  f.order(2, 20, Side::Sell, 99, 4);
  const Event* ex = f.rec.last(EventType::Executed);
  CHECK(ex != nullptr);
  if (ex) {
    CHECK_EQ(ex->price, 100); // trades at the resting price
    CHECK_EQ(ex->qty, 4u);
    CHECK_EQ(ex->leaves, 0u);
    CHECK_EQ(ex->resting_leaves, 6u);
    CHECK_EQ(ex->session, 2u);
    CHECK_EQ(ex->resting_session, 1u);
    CHECK_EQ(ex->resting_cl_id, 10u);
    CHECK_EQ(ex->match_id, 1u);
  }
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{1});
  CHECK_EQ(f.engine.stats().volume, 4u);
  CHECK_EQ(f.engine.live_orders(2), std::size_t{0});
  CHECK_EQ(f.engine.live_orders(1), std::size_t{1});
}

TEST(engine_ioc_never_rests) {
  Fixture f;
  f.open(1);
  f.order(1, 1, Side::Sell, 100, 5);
  f.order(1, 2, Side::Buy, 100, 8, OrderType::Limit, TimeInForce::IOC);
  const Event* c = f.rec.last(EventType::Canceled);
  CHECK(c != nullptr);
  if (c) {
    CHECK_EQ(c->qty, 3u);
    CHECK(c->cancel_reason == CancelReason::ImmediateOrCancel);
    CHECK(!c->was_resting);
  }
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{0});
  CHECK_EQ(f.rec.count(EventType::Rested), std::size_t{1});
}

TEST(engine_market_order_no_liquidity) {
  Fixture f;
  f.open(1);
  f.order(1, 1, Side::Buy, 0, 5, OrderType::Market);
  const Event* c = f.rec.last(EventType::Canceled);
  CHECK(c != nullptr);
  if (c) CHECK(c->cancel_reason == CancelReason::MarketNoLiquidity);
  f.order(1, 2, Side::Sell, 200, 2);
  f.order(1, 3, Side::Sell, 210, 2);
  f.order(1, 4, Side::Buy, 0, 3, OrderType::Market);
  CHECK_EQ(f.rec.count(EventType::Executed), std::size_t{2});
  const Event* ex = f.rec.last(EventType::Executed);
  if (ex) CHECK_EQ(ex->price, 210);
  CHECK_EQ(f.engine.book(0).best_level(Side::Sell)->total, 1u);
}

TEST(engine_cancel_partial_and_full) {
  Fixture f;
  f.open(1);
  f.order(1, 1, Side::Buy, 100, 10);
  f.cancel(1, 1, 3);
  const Event* c = f.rec.last(EventType::Canceled);
  CHECK(c && c->qty == 3 && c->leaves == 7 && c->was_resting);
  f.cancel(1, 1);
  c = f.rec.last(EventType::Canceled);
  CHECK(c && c->qty == 7 && c->leaves == 0);
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{0});
  f.cancel(1, 1);
  const Event* r = f.rec.last(EventType::Rejected);
  CHECK(r && r->reject == RejectReason::UnknownOrder);
}

TEST(engine_replace_keeps_priority_when_shrinking) {
  Fixture f;
  f.open(1);
  f.open(2);
  f.order(1, 1, Side::Sell, 100, 10);
  f.order(2, 2, Side::Sell, 100, 10);
  f.replace(1, 1, 11, 100, 4);
  const Event* rp = f.rec.last(EventType::Replaced);
  CHECK(rp && rp->kept_priority && rp->oid == 1 && rp->prev_qty == 10 && rp->qty == 4);
  f.order(2, 3, Side::Buy, 100, 4);
  const Event* ex = f.rec.last(EventType::Executed);
  CHECK(ex && ex->resting_oid == 1 && ex->resting_cl_id == 11);
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{1});
}

TEST(engine_replace_loses_priority_on_price_change) {
  Fixture f;
  f.open(1);
  f.open(2);
  f.order(1, 1, Side::Sell, 100, 10);
  f.order(2, 2, Side::Sell, 101, 10);
  f.replace(1, 1, 11, 101, 10); // moves to 101 behind order 2
  const Event* rp = f.rec.last(EventType::Replaced);
  CHECK(rp && !rp->kept_priority && rp->oid == 3 && rp->old_oid == 1);
  f.order(2, 3, Side::Buy, 101, 10);
  const Event* ex = f.rec.last(EventType::Executed);
  CHECK(ex && ex->resting_oid == 2);
  CHECK(f.engine.find(3).has_value());
  CHECK(!f.engine.find(1).has_value());
}

TEST(engine_replace_can_cross) {
  Fixture f;
  f.open(1);
  f.open(2);
  f.order(1, 1, Side::Buy, 100, 5);
  f.order(2, 2, Side::Sell, 105, 5);
  f.replace(1, 1, 11, 105, 5);
  CHECK_EQ(f.rec.count(EventType::Executed), std::size_t{1});
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{0});
}

TEST(engine_rejects) {
  Fixture f;
  f.order(1, 1, Side::Buy, 100, 5);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::NotLoggedIn);
  f.open(1);
  f.order(1, 1, Side::Buy, 100, 5, OrderType::Limit, TimeInForce::Day, 99);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::UnknownSymbol);
  f.order(1, 1, Side::Buy, 100, 0);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::BadQuantity);
  f.order(1, 1, Side::Buy, 0, 5);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::BadPrice);
  f.order(1, 1, Side::Buy, 100, 5);
  f.order(1, 1, Side::Buy, 100, 5);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::DuplicateClientOrderId);
  f.replace(1, 1, 2, 100, 0);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::BadQuantity);
  CHECK_EQ(f.engine.stats().rejects, std::uint64_t{6});
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{1});
}

TEST(engine_cancel_on_disconnect) {
  Fixture f;
  f.open(1);
  f.open(2);
  f.order(1, 1, Side::Buy, 100, 5);
  f.order(1, 2, Side::Buy, 99, 5);
  f.order(2, 3, Side::Sell, 110, 5);
  f.close(1);
  CHECK_EQ(f.rec.count(EventType::Canceled), std::size_t{2});
  const Event* c = f.rec.last(EventType::Canceled);
  CHECK(c && c->cancel_reason == CancelReason::SessionClosed);
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{1});
  CHECK(!f.engine.session_open(1));
  f.order(1, 4, Side::Buy, 100, 5);
  CHECK(f.rec.last(EventType::Rejected)->reject == RejectReason::NotLoggedIn);
  std::string why;
  CHECK_MSG(f.engine.check_invariants(&why), why);
}

TEST(engine_symbols_are_independent) {
  Fixture f;
  f.open(1);
  f.order(1, 1, Side::Sell, 100, 5, OrderType::Limit, TimeInForce::Day, 0);
  f.order(1, 2, Side::Buy, 100, 5, OrderType::Limit, TimeInForce::Day, 1);
  CHECK_EQ(f.rec.count(EventType::Executed), std::size_t{0});
  CHECK_EQ(f.engine.book(0).live_orders(), std::size_t{1});
  CHECK_EQ(f.engine.book(1).live_orders(), std::size_t{1});
  CHECK(f.engine.books_hash() != Engine(4).books_hash());
}
