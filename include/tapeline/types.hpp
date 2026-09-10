#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>

namespace tapeline {

// Prices are integer ticks. One tick is whatever the venue says it is (this
// code never needs to know); the point is that no floating point touches the
// book, the feed, or the ledger of fills.
using Price = std::int64_t;
using Qty = std::uint32_t;
using OrderId = std::uint64_t;       // assigned by the exchange, dense from 1
using ClientOrderId = std::uint64_t; // chosen by the client, unique per session
using SymbolId = std::uint32_t;
using SeqNo = std::uint64_t;
using Timestamp = std::uint64_t;     // nanoseconds, stamped by the sequencer
using SessionId = std::uint32_t;
using MatchId = std::uint64_t;

inline constexpr Price kMaxPrice = std::numeric_limits<Price>::max();
inline constexpr Price kMinPrice = 1;

enum class Side : std::uint8_t { Buy = 1, Sell = 2 };
enum class OrderType : std::uint8_t { Limit = 0, Market = 1 };
enum class TimeInForce : std::uint8_t { Day = 0, IOC = 1 };

enum class RejectReason : std::uint8_t {
  None = 0,
  UnknownSymbol = 1,
  BadQuantity = 2,
  BadPrice = 3,
  DuplicateClientOrderId = 4,
  UnknownOrder = 5,
  NotLoggedIn = 6,
  SessionClosed = 7,
};

enum class CancelReason : std::uint8_t {
  UserRequested = 0,
  ImmediateOrCancel = 1,
  MarketNoLiquidity = 2,
  Replaced = 3,
  SessionClosed = 4,
};

constexpr Side opposite(Side s) noexcept {
  return s == Side::Buy ? Side::Sell : Side::Buy;
}

constexpr std::string_view to_string(Side s) noexcept {
  return s == Side::Buy ? "buy" : "sell";
}

constexpr std::string_view to_string(RejectReason r) noexcept {
  switch (r) {
    case RejectReason::None: return "none";
    case RejectReason::UnknownSymbol: return "unknown symbol";
    case RejectReason::BadQuantity: return "bad quantity";
    case RejectReason::BadPrice: return "bad price";
    case RejectReason::DuplicateClientOrderId: return "duplicate client order id";
    case RejectReason::UnknownOrder: return "unknown order";
    case RejectReason::NotLoggedIn: return "not logged in";
    case RejectReason::SessionClosed: return "session closed";
  }
  return "?";
}

constexpr std::string_view to_string(CancelReason r) noexcept {
  switch (r) {
    case CancelReason::UserRequested: return "user";
    case CancelReason::ImmediateOrCancel: return "ioc";
    case CancelReason::MarketNoLiquidity: return "no liquidity";
    case CancelReason::Replaced: return "replaced";
    case CancelReason::SessionClosed: return "session closed";
  }
  return "?";
}

// What a client asks the exchange to do. These are the payloads of the
// sequenced inbound log, so they stay trivially copyable on purpose.
struct NewOrder {
  ClientOrderId cl_id{};
  SymbolId symbol{};
  Side side{Side::Buy};
  OrderType type{OrderType::Limit};
  TimeInForce tif{TimeInForce::Day};
  Price price{};
  Qty qty{};
};

struct CancelOrder {
  ClientOrderId cl_id{};
  Qty qty{}; // 0 means cancel everything that is left
};

struct ReplaceOrder {
  ClientOrderId cl_id{};
  ClientOrderId new_cl_id{};
  Price price{};
  Qty qty{};
};

enum class InboundKind : std::uint8_t {
  SessionOpen = 1,
  SessionClose = 2,
  NewOrder = 3,
  Cancel = 4,
  Replace = 5,
  EndOfSession = 6,
};

// One record of the sequenced log. Everything the matching engine does is a
// pure function of the sequence of these records, which is what makes the
// standby replica and the replay tests possible.
struct Inbound {
  SeqNo seq{};
  Timestamp ts{};
  SessionId session{};
  InboundKind kind{InboundKind::NewOrder};
  NewOrder new_order{};
  CancelOrder cancel{};
  ReplaceOrder replace{};
};

static_assert(std::is_trivially_copyable_v<Inbound>);

// What the engine tells the rest of the system. The feed publisher turns these
// into market-data messages and the gateway turns them into session replies.
enum class EventType : std::uint8_t {
  Accepted = 1,  // order passed validation and got an exchange id
  Rested = 2,    // order (or what is left of it) is now on the book
  Executed = 3,  // a fill between an aggressor and a resting order
  Canceled = 4,  // quantity removed from an order
  Replaced = 5,  // old order retired in favour of a new exchange id
  Rejected = 6,
};

struct Event {
  EventType type{};
  SeqNo seq{};       // inbound record that caused this event
  Timestamp ts{};
  SessionId session{};
  ClientOrderId cl_id{};
  OrderId oid{};
  SymbolId symbol{};
  Side side{Side::Buy};
  Price price{};
  Qty qty{};         // accepted qty / fill qty / canceled qty / new qty
  Qty leaves{};      // what remains of the order this event is about
  TimeInForce tif{TimeInForce::Day};
  bool was_resting{false}; // Canceled: whether the removed qty was on the book

  // Executed only: the passive side of the fill.
  OrderId resting_oid{};
  SessionId resting_session{};
  ClientOrderId resting_cl_id{};
  Qty resting_leaves{};
  MatchId match_id{};

  // Replaced only.
  OrderId old_oid{};
  ClientOrderId old_cl_id{};
  Qty prev_qty{};
  bool kept_priority{false};

  RejectReason reject{RejectReason::None};
  CancelReason cancel_reason{CancelReason::UserRequested};
};

struct EventSink {
  virtual ~EventSink() = default;
  virtual void on_event(const Event& ev) = 0;
};

} // namespace tapeline
