#pragma once

#include "hash.hpp"
#include "types.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <string_view>
#include <type_traits>

// Binary wire formats. Everything is little-endian with explicit field
// layouts written and read through memcpy, so the same code is correct on any
// host and never relies on struct packing. There are three protocols:
//
//   feed:    market data, MoldUDP-style packets carrying ITCH-style messages,
//            numbered by a session-wide sequence.
//   gw:      order entry over TCP, SoupBin-style length-prefixed frames with a
//            per-session outbound sequence so a client can resume after a drop.
//   retrans: the TCP side channel a feed handler uses to fill gaps or ask for a
//            full snapshot.
namespace tapeline::wire {

enum class Error : std::uint8_t {
  Truncated = 1,
  BadMagic = 2,
  BadVersion = 3,
  BadType = 4,
  BadLength = 5,
  Overflow = 6,
};

constexpr std::string_view to_string(Error e) noexcept {
  switch (e) {
    case Error::Truncated: return "truncated";
    case Error::BadMagic: return "bad magic";
    case Error::BadVersion: return "bad version";
    case Error::BadType: return "bad type";
    case Error::BadLength: return "bad length";
    case Error::Overflow: return "overflow";
  }
  return "?";
}

template <class T>
concept WireScalar = std::is_integral_v<T> || std::is_enum_v<T>;

template <WireScalar T>
using Unsigned = std::make_unsigned_t<raw_integer_t<T>>;

class Writer {
 public:
  explicit Writer(std::span<std::byte> buf) noexcept : buf_(buf) {}

  template <WireScalar T>
  void put(T v) noexcept {
    using U = Unsigned<T>;
    if (pos_ + sizeof(U) > buf_.size()) {
      overflow_ = true;
      return;
    }
    U u = static_cast<U>(v);
    if constexpr (std::endian::native == std::endian::big) u = std::byteswap(u);
    std::memcpy(buf_.data() + pos_, &u, sizeof(U));
    pos_ += sizeof(U);
  }

  void put_bytes(std::span<const std::byte> bytes) noexcept {
    if (pos_ + bytes.size() > buf_.size()) {
      overflow_ = true;
      return;
    }
    if (!bytes.empty()) std::memcpy(buf_.data() + pos_, bytes.data(), bytes.size());
    pos_ += bytes.size();
  }

  // Fixed-width text field, space padded, never NUL terminated.
  void put_text(std::string_view s, std::size_t width) noexcept {
    if (pos_ + width > buf_.size()) {
      overflow_ = true;
      return;
    }
    std::memset(buf_.data() + pos_, ' ', width);
    std::memcpy(buf_.data() + pos_, s.data(), s.size() < width ? s.size() : width);
    pos_ += width;
  }

  template <WireScalar T>
  void patch(std::size_t at, T v) noexcept {
    using U = Unsigned<T>;
    if (at + sizeof(U) > buf_.size()) {
      overflow_ = true;
      return;
    }
    U u = static_cast<U>(v);
    if constexpr (std::endian::native == std::endian::big) u = std::byteswap(u);
    std::memcpy(buf_.data() + at, &u, sizeof(U));
  }

  [[nodiscard]] std::size_t size() const noexcept { return pos_; }
  [[nodiscard]] std::size_t capacity() const noexcept { return buf_.size(); }
  [[nodiscard]] bool ok() const noexcept { return !overflow_; }
  [[nodiscard]] std::span<const std::byte> written() const noexcept { return buf_.first(pos_); }
  void reset() noexcept { pos_ = 0; overflow_ = false; }

 private:
  std::span<std::byte> buf_;
  std::size_t pos_ = 0;
  bool overflow_ = false;
};

class Reader {
 public:
  explicit Reader(std::span<const std::byte> buf) noexcept : buf_(buf) {}

  template <WireScalar T>
  [[nodiscard]] T get() noexcept {
    using U = Unsigned<T>;
    if (pos_ + sizeof(U) > buf_.size()) {
      fail_ = true;
      return T{};
    }
    U u;
    std::memcpy(&u, buf_.data() + pos_, sizeof(U));
    if constexpr (std::endian::native == std::endian::big) u = std::byteswap(u);
    pos_ += sizeof(U);
    return static_cast<T>(u);
  }

  [[nodiscard]] std::span<const std::byte> take(std::size_t n) noexcept {
    if (pos_ + n > buf_.size()) {
      fail_ = true;
      return {};
    }
    auto s = buf_.subspan(pos_, n);
    pos_ += n;
    return s;
  }

  void skip(std::size_t n) noexcept { (void)take(n); }
  [[nodiscard]] std::size_t remaining() const noexcept { return buf_.size() - pos_; }
  [[nodiscard]] std::size_t pos() const noexcept { return pos_; }
  [[nodiscard]] bool ok() const noexcept { return !fail_; }

 private:
  std::span<const std::byte> buf_;
  std::size_t pos_ = 0;
  bool fail_ = false;
};

// ---------------------------------------------------------------------------
// Market data feed
// ---------------------------------------------------------------------------
namespace feed {

inline constexpr std::uint16_t kMagic = 0x4C54; // bytes 'T','L' on the wire
inline constexpr std::uint8_t kVersion = 1;
inline constexpr std::size_t kHeaderSize = 20;
inline constexpr std::size_t kMaxPacket = 1400; // fits in one Ethernet frame

enum class PacketKind : std::uint8_t { Live = 1, Retrans = 2, Snapshot = 3 };

struct PacketHeader {
  PacketKind kind{PacketKind::Live};
  std::uint32_t session{};
  SeqNo first_seq{};   // sequence number of the first message in the packet
  std::uint16_t count{};
  std::uint16_t length{}; // whole packet, header included
};

enum class MsgType : std::uint8_t {
  SystemEvent = 'S',
  AddOrder = 'A',
  Executed = 'E',
  Cancel = 'X',
  Delete = 'D',
  Heartbeat = 'H',
};

enum class SystemCode : std::uint8_t {
  StartOfSession = 'O',
  EndOfSession = 'C',
  SnapshotEnd = 'Q',
};

struct Msg {
  MsgType type{MsgType::Heartbeat};
  Timestamp ts{};
  OrderId oid{};
  SymbolId symbol{};
  Side side{Side::Buy};
  Price price{};
  Qty qty{};
  MatchId match_id{};
  SystemCode code{SystemCode::StartOfSession};
};

constexpr std::size_t payload_size(MsgType t) noexcept {
  switch (t) {
    case MsgType::SystemEvent: return 8 + 1;
    case MsgType::AddOrder: return 8 + 8 + 4 + 1 + 8 + 4;
    case MsgType::Executed: return 8 + 8 + 4 + 8 + 8;
    case MsgType::Cancel: return 8 + 8 + 4;
    case MsgType::Delete: return 8 + 8;
    case MsgType::Heartbeat: return 8;
  }
  return 0;
}

constexpr bool valid_type(std::uint8_t t) noexcept {
  switch (static_cast<MsgType>(t)) {
    case MsgType::SystemEvent:
    case MsgType::AddOrder:
    case MsgType::Executed:
    case MsgType::Cancel:
    case MsgType::Delete:
    case MsgType::Heartbeat:
      return true;
  }
  return false;
}

constexpr std::size_t encoded_size(MsgType t) noexcept { return 2 + payload_size(t); }

inline void write_header(Writer& w, const PacketHeader& h) noexcept {
  w.put(kMagic);
  w.put(kVersion);
  w.put(h.kind);
  w.put(h.session);
  w.put(h.first_seq);
  w.put(h.count);
  w.put(h.length);
}

inline std::expected<PacketHeader, Error> read_header(Reader& r) noexcept {
  if (r.remaining() < kHeaderSize) return std::unexpected(Error::Truncated);
  if (r.get<std::uint16_t>() != kMagic) return std::unexpected(Error::BadMagic);
  if (r.get<std::uint8_t>() != kVersion) return std::unexpected(Error::BadVersion);
  PacketHeader h;
  const auto kind = r.get<std::uint8_t>();
  if (kind < 1 || kind > 3) return std::unexpected(Error::BadType);
  h.kind = static_cast<PacketKind>(kind);
  h.session = r.get<std::uint32_t>();
  h.first_seq = r.get<SeqNo>();
  h.count = r.get<std::uint16_t>();
  h.length = r.get<std::uint16_t>();
  if (h.length < kHeaderSize) return std::unexpected(Error::BadLength);
  return h;
}

inline void encode(const Msg& m, Writer& w) noexcept {
  w.put(m.type);
  w.put(static_cast<std::uint8_t>(payload_size(m.type)));
  w.put(m.ts);
  switch (m.type) {
    case MsgType::SystemEvent:
      w.put(m.code);
      break;
    case MsgType::AddOrder:
      w.put(m.oid);
      w.put(m.symbol);
      w.put(m.side);
      w.put(m.price);
      w.put(m.qty);
      break;
    case MsgType::Executed:
      w.put(m.oid);
      w.put(m.qty);
      w.put(m.match_id);
      w.put(m.price);
      break;
    case MsgType::Cancel:
      w.put(m.oid);
      w.put(m.qty);
      break;
    case MsgType::Delete:
      w.put(m.oid);
      break;
    case MsgType::Heartbeat:
      break;
  }
}

inline std::expected<Msg, Error> decode(Reader& r) noexcept {
  if (r.remaining() < 2) return std::unexpected(Error::Truncated);
  const auto t = r.get<std::uint8_t>();
  const auto len = r.get<std::uint8_t>();
  if (!valid_type(t)) return std::unexpected(Error::BadType);
  Msg m;
  m.type = static_cast<MsgType>(t);
  if (len != payload_size(m.type)) return std::unexpected(Error::BadLength);
  if (r.remaining() < len) return std::unexpected(Error::Truncated);
  m.ts = r.get<Timestamp>();
  switch (m.type) {
    case MsgType::SystemEvent:
      m.code = r.get<SystemCode>();
      break;
    case MsgType::AddOrder:
      m.oid = r.get<OrderId>();
      m.symbol = r.get<SymbolId>();
      m.side = r.get<Side>();
      m.price = r.get<Price>();
      m.qty = r.get<Qty>();
      break;
    case MsgType::Executed:
      m.oid = r.get<OrderId>();
      m.qty = r.get<Qty>();
      m.match_id = r.get<MatchId>();
      m.price = r.get<Price>();
      break;
    case MsgType::Cancel:
      m.oid = r.get<OrderId>();
      m.qty = r.get<Qty>();
      break;
    case MsgType::Delete:
      m.oid = r.get<OrderId>();
      break;
    case MsgType::Heartbeat:
      break;
  }
  if (!r.ok()) return std::unexpected(Error::Truncated);
  return m;
}

} // namespace feed

// ---------------------------------------------------------------------------
// Order entry gateway
// ---------------------------------------------------------------------------
namespace gw {

inline constexpr std::size_t kUserWidth = 8;
inline constexpr std::size_t kMaxFrame = 128;

enum class InType : std::uint8_t {
  Login = 'L',
  EnterOrder = 'O',
  Cancel = 'X',
  Replace = 'U',
  Heartbeat = 'R',
  Logout = 'Z',
};

enum class OutType : std::uint8_t {
  LoginAccepted = 'A',
  LoginRejected = 'J',
  Accepted = 'S',
  Executed = 'E',
  Canceled = 'C',
  Replaced = 'R',
  Rejected = 'N',
  Heartbeat = 'H',
  EndOfSession = 'Z',
};

struct Login {
  std::array<char, kUserWidth> user{};
  SessionId session{};      // 0 asks for a fresh session
  SeqNo next_expected{};    // first outbound seq the client has not seen
};

struct InMsg {
  InType type{InType::Heartbeat};
  Login login{};
  NewOrder order{};
  CancelOrder cancel{};
  ReplaceOrder replace{};
};

struct OutMsg {
  OutType type{OutType::Heartbeat};
  SeqNo seq{};
  Timestamp ts{};
  SessionId session{};
  ClientOrderId cl_id{};
  ClientOrderId cl_id2{}; // Replaced: the new client order id
  OrderId oid{};
  SymbolId symbol{};
  Side side{Side::Buy};
  Price price{};
  Qty qty{};
  Qty leaves{};
  TimeInForce tif{TimeInForce::Day};
  MatchId match_id{};
  std::uint8_t reason{};
};

constexpr std::size_t in_payload_size(InType t) noexcept {
  switch (t) {
    case InType::Login: return kUserWidth + 4 + 8;
    case InType::EnterOrder: return 8 + 4 + 1 + 1 + 1 + 8 + 4;
    case InType::Cancel: return 8 + 4;
    case InType::Replace: return 8 + 8 + 8 + 4;
    case InType::Heartbeat: return 0;
    case InType::Logout: return 0;
  }
  return 0;
}

constexpr bool valid_in_type(std::uint8_t t) noexcept {
  switch (static_cast<InType>(t)) {
    case InType::Login:
    case InType::EnterOrder:
    case InType::Cancel:
    case InType::Replace:
    case InType::Heartbeat:
    case InType::Logout:
      return true;
  }
  return false;
}

constexpr std::size_t out_payload_size(OutType t) noexcept {
  switch (t) {
    case OutType::LoginAccepted: return 4 + 8;
    case OutType::LoginRejected: return 1;
    case OutType::Accepted: return 8 + 8 + 8 + 8 + 4 + 1 + 8 + 4 + 1;
    case OutType::Executed: return 8 + 8 + 8 + 4 + 8 + 8 + 4;
    case OutType::Canceled: return 8 + 8 + 8 + 4 + 4 + 1;
    case OutType::Replaced: return 8 + 8 + 8 + 8 + 8 + 8 + 4;
    case OutType::Rejected: return 8 + 8 + 8 + 1;
    case OutType::Heartbeat: return 0;
    case OutType::EndOfSession: return 0;
  }
  return 0;
}

constexpr bool valid_out_type(std::uint8_t t) noexcept {
  switch (static_cast<OutType>(t)) {
    case OutType::LoginAccepted:
    case OutType::LoginRejected:
    case OutType::Accepted:
    case OutType::Executed:
    case OutType::Canceled:
    case OutType::Replaced:
    case OutType::Rejected:
    case OutType::Heartbeat:
    case OutType::EndOfSession:
      return true;
  }
  return false;
}

// Frame layout: u16 length (bytes that follow), u8 type, payload.
constexpr std::size_t in_frame_size(InType t) noexcept { return 2 + 1 + in_payload_size(t); }
constexpr std::size_t out_frame_size(OutType t) noexcept { return 2 + 1 + out_payload_size(t); }

inline void encode(const InMsg& m, Writer& w) noexcept {
  w.put(static_cast<std::uint16_t>(1 + in_payload_size(m.type)));
  w.put(m.type);
  switch (m.type) {
    case InType::Login:
      w.put_text(std::string_view(m.login.user.data(), m.login.user.size()), kUserWidth);
      w.put(m.login.session);
      w.put(m.login.next_expected);
      break;
    case InType::EnterOrder:
      w.put(m.order.cl_id);
      w.put(m.order.symbol);
      w.put(m.order.side);
      w.put(m.order.type);
      w.put(m.order.tif);
      w.put(m.order.price);
      w.put(m.order.qty);
      break;
    case InType::Cancel:
      w.put(m.cancel.cl_id);
      w.put(m.cancel.qty);
      break;
    case InType::Replace:
      w.put(m.replace.cl_id);
      w.put(m.replace.new_cl_id);
      w.put(m.replace.price);
      w.put(m.replace.qty);
      break;
    case InType::Heartbeat:
    case InType::Logout:
      break;
  }
}

// Decodes one frame if a whole one is available. Leaves the reader untouched
// (and returns Truncated) when the buffer holds only part of a frame, so a
// stream reader can simply wait for more bytes.
inline std::expected<InMsg, Error> decode(Reader& r) noexcept {
  if (r.remaining() < 3) return std::unexpected(Error::Truncated);
  Reader peek = r;
  const auto len = peek.get<std::uint16_t>();
  if (len < 1) return std::unexpected(Error::BadLength);
  if (peek.remaining() < len) return std::unexpected(Error::Truncated);
  const auto t = peek.get<std::uint8_t>();
  if (!valid_in_type(t)) return std::unexpected(Error::BadType);
  InMsg m;
  m.type = static_cast<InType>(t);
  if (len != 1 + in_payload_size(m.type)) return std::unexpected(Error::BadLength);
  switch (m.type) {
    case InType::Login: {
      auto text = peek.take(kUserWidth);
      if (text.size() == kUserWidth) std::memcpy(m.login.user.data(), text.data(), kUserWidth);
      m.login.session = peek.get<SessionId>();
      m.login.next_expected = peek.get<SeqNo>();
      break;
    }
    case InType::EnterOrder:
      m.order.cl_id = peek.get<ClientOrderId>();
      m.order.symbol = peek.get<SymbolId>();
      m.order.side = peek.get<Side>();
      m.order.type = peek.get<OrderType>();
      m.order.tif = peek.get<TimeInForce>();
      m.order.price = peek.get<Price>();
      m.order.qty = peek.get<Qty>();
      break;
    case InType::Cancel:
      m.cancel.cl_id = peek.get<ClientOrderId>();
      m.cancel.qty = peek.get<Qty>();
      break;
    case InType::Replace:
      m.replace.cl_id = peek.get<ClientOrderId>();
      m.replace.new_cl_id = peek.get<ClientOrderId>();
      m.replace.price = peek.get<Price>();
      m.replace.qty = peek.get<Qty>();
      break;
    case InType::Heartbeat:
    case InType::Logout:
      break;
  }
  if (!peek.ok()) return std::unexpected(Error::Truncated);
  r = peek;
  return m;
}

inline void encode(const OutMsg& m, Writer& w) noexcept {
  w.put(static_cast<std::uint16_t>(1 + out_payload_size(m.type)));
  w.put(m.type);
  switch (m.type) {
    case OutType::LoginAccepted:
      w.put(m.session);
      w.put(m.seq);
      break;
    case OutType::LoginRejected:
      w.put(m.reason);
      break;
    case OutType::Accepted:
      w.put(m.seq);
      w.put(m.ts);
      w.put(m.cl_id);
      w.put(m.oid);
      w.put(m.symbol);
      w.put(m.side);
      w.put(m.price);
      w.put(m.qty);
      w.put(m.tif);
      break;
    case OutType::Executed:
      w.put(m.seq);
      w.put(m.ts);
      w.put(m.cl_id);
      w.put(m.qty);
      w.put(m.price);
      w.put(m.match_id);
      w.put(m.leaves);
      break;
    case OutType::Canceled:
      w.put(m.seq);
      w.put(m.ts);
      w.put(m.cl_id);
      w.put(m.qty);
      w.put(m.leaves);
      w.put(m.reason);
      break;
    case OutType::Replaced:
      w.put(m.seq);
      w.put(m.ts);
      w.put(m.cl_id);
      w.put(m.cl_id2);
      w.put(m.oid);
      w.put(m.price);
      w.put(m.qty);
      break;
    case OutType::Rejected:
      w.put(m.seq);
      w.put(m.ts);
      w.put(m.cl_id);
      w.put(m.reason);
      break;
    case OutType::Heartbeat:
    case OutType::EndOfSession:
      break;
  }
}

inline std::expected<OutMsg, Error> decode_out(Reader& r) noexcept {
  if (r.remaining() < 3) return std::unexpected(Error::Truncated);
  Reader peek = r;
  const auto len = peek.get<std::uint16_t>();
  if (len < 1) return std::unexpected(Error::BadLength);
  if (peek.remaining() < len) return std::unexpected(Error::Truncated);
  const auto t = peek.get<std::uint8_t>();
  if (!valid_out_type(t)) return std::unexpected(Error::BadType);
  OutMsg m;
  m.type = static_cast<OutType>(t);
  if (len != 1 + out_payload_size(m.type)) return std::unexpected(Error::BadLength);
  switch (m.type) {
    case OutType::LoginAccepted:
      m.session = peek.get<SessionId>();
      m.seq = peek.get<SeqNo>();
      break;
    case OutType::LoginRejected:
      m.reason = peek.get<std::uint8_t>();
      break;
    case OutType::Accepted:
      m.seq = peek.get<SeqNo>();
      m.ts = peek.get<Timestamp>();
      m.cl_id = peek.get<ClientOrderId>();
      m.oid = peek.get<OrderId>();
      m.symbol = peek.get<SymbolId>();
      m.side = peek.get<Side>();
      m.price = peek.get<Price>();
      m.qty = peek.get<Qty>();
      m.tif = peek.get<TimeInForce>();
      break;
    case OutType::Executed:
      m.seq = peek.get<SeqNo>();
      m.ts = peek.get<Timestamp>();
      m.cl_id = peek.get<ClientOrderId>();
      m.qty = peek.get<Qty>();
      m.price = peek.get<Price>();
      m.match_id = peek.get<MatchId>();
      m.leaves = peek.get<Qty>();
      break;
    case OutType::Canceled:
      m.seq = peek.get<SeqNo>();
      m.ts = peek.get<Timestamp>();
      m.cl_id = peek.get<ClientOrderId>();
      m.qty = peek.get<Qty>();
      m.leaves = peek.get<Qty>();
      m.reason = peek.get<std::uint8_t>();
      break;
    case OutType::Replaced:
      m.seq = peek.get<SeqNo>();
      m.ts = peek.get<Timestamp>();
      m.cl_id = peek.get<ClientOrderId>();
      m.cl_id2 = peek.get<ClientOrderId>();
      m.oid = peek.get<OrderId>();
      m.price = peek.get<Price>();
      m.qty = peek.get<Qty>();
      break;
    case OutType::Rejected:
      m.seq = peek.get<SeqNo>();
      m.ts = peek.get<Timestamp>();
      m.cl_id = peek.get<ClientOrderId>();
      m.reason = peek.get<std::uint8_t>();
      break;
    case OutType::Heartbeat:
    case OutType::EndOfSession:
      break;
  }
  if (!peek.ok()) return std::unexpected(Error::Truncated);
  r = peek;
  return m;
}

} // namespace gw

// ---------------------------------------------------------------------------
// Feed recovery side channel (TCP). Requests are tiny frames; replies are
// ordinary feed packets streamed back to back, so the handler reuses its
// packet parser.
// ---------------------------------------------------------------------------
namespace retrans {

enum class ReqType : std::uint8_t { Retransmit = 'R', Snapshot = 'S' };

struct Request {
  ReqType type{ReqType::Retransmit};
  std::uint32_t session{};
  SeqNo start{};
  std::uint16_t count{};
};

inline constexpr std::size_t kFrameSize = 2 + 1 + 4 + 8 + 2;

inline void encode(const Request& q, Writer& w) noexcept {
  w.put(static_cast<std::uint16_t>(kFrameSize - 2));
  w.put(q.type);
  w.put(q.session);
  w.put(q.start);
  w.put(q.count);
}

inline std::expected<Request, Error> decode(Reader& r) noexcept {
  if (r.remaining() < kFrameSize) return std::unexpected(Error::Truncated);
  Reader peek = r;
  if (peek.get<std::uint16_t>() != kFrameSize - 2) return std::unexpected(Error::BadLength);
  const auto t = peek.get<std::uint8_t>();
  if (t != 'R' && t != 'S') return std::unexpected(Error::BadType);
  Request q;
  q.type = static_cast<ReqType>(t);
  q.session = peek.get<std::uint32_t>();
  q.start = peek.get<SeqNo>();
  q.count = peek.get<std::uint16_t>();
  if (!peek.ok()) return std::unexpected(Error::Truncated);
  r = peek;
  return q;
}

} // namespace retrans

} // namespace tapeline::wire
