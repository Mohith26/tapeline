#include "tapeline/wire.hpp"
#include "test.hpp"

#include <array>
#include <cstring>
#include <vector>

using namespace tapeline;
using namespace tapeline::wire;

TEST(wire_feed_message_roundtrip) {
  const feed::MsgType types[] = {feed::MsgType::SystemEvent, feed::MsgType::AddOrder, feed::MsgType::Executed,
                                 feed::MsgType::Cancel, feed::MsgType::Delete, feed::MsgType::Heartbeat};
  for (auto t : types) {
    feed::Msg m;
    m.type = t;
    m.ts = 0x0102030405060708ull;
    m.oid = 987654321ull;
    m.symbol = 42;
    m.side = Side::Sell;
    m.price = -1234567;
    m.qty = 777;
    m.match_id = 55;
    m.code = feed::SystemCode::EndOfSession;
    std::array<std::byte, 64> buf{};
    Writer w(buf);
    feed::encode(m, w);
    CHECK(w.ok());
    CHECK_EQ(w.size(), feed::encoded_size(t));
    Reader r(w.written());
    auto d = feed::decode(r);
    CHECK(d.has_value());
    if (!d) continue;
    CHECK(d->type == t);
    CHECK_EQ(d->ts, m.ts);
    switch (t) {
      case feed::MsgType::AddOrder:
        CHECK_EQ(d->oid, m.oid);
        CHECK_EQ(d->symbol, m.symbol);
        CHECK(d->side == Side::Sell);
        CHECK_EQ(d->price, m.price);
        CHECK_EQ(d->qty, m.qty);
        break;
      case feed::MsgType::Executed:
        CHECK_EQ(d->oid, m.oid);
        CHECK_EQ(d->qty, m.qty);
        CHECK_EQ(d->match_id, m.match_id);
        CHECK_EQ(d->price, m.price);
        break;
      case feed::MsgType::Cancel:
        CHECK_EQ(d->oid, m.oid);
        CHECK_EQ(d->qty, m.qty);
        break;
      case feed::MsgType::Delete:
        CHECK_EQ(d->oid, m.oid);
        break;
      case feed::MsgType::SystemEvent:
        CHECK(d->code == feed::SystemCode::EndOfSession);
        break;
      case feed::MsgType::Heartbeat:
        break;
    }
    CHECK_EQ(r.remaining(), std::size_t{0});
  }
}

TEST(wire_feed_header_roundtrip_and_layout) {
  std::array<std::byte, 64> buf{};
  Writer w(buf);
  feed::PacketHeader h;
  h.kind = feed::PacketKind::Retrans;
  h.session = 0xAABBCCDD;
  h.first_seq = 1234567890123ull;
  h.count = 17;
  h.length = 300;
  feed::write_header(w, h);
  CHECK_EQ(w.size(), feed::kHeaderSize);
  CHECK(buf[0] == std::byte{'T'} && buf[1] == std::byte{'L'});
  Reader r(w.written());
  auto d = feed::read_header(r);
  CHECK(d.has_value());
  if (d) {
    CHECK(d->kind == feed::PacketKind::Retrans);
    CHECK_EQ(d->session, h.session);
    CHECK_EQ(d->first_seq, h.first_seq);
    CHECK_EQ(d->count, h.count);
    CHECK_EQ(d->length, h.length);
  }
  buf[0] = std::byte{'X'};
  Reader bad(std::span<const std::byte>(buf.data(), feed::kHeaderSize));
  auto e = feed::read_header(bad);
  CHECK(!e && e.error() == Error::BadMagic);
}

TEST(wire_feed_decode_rejects_garbage) {
  std::array<std::byte, 8> buf{};
  Reader empty(std::span<const std::byte>(buf.data(), 1));
  CHECK(!feed::decode(empty) && feed::decode(empty).error() == Error::Truncated);
  buf[0] = std::byte{'Z'};
  buf[1] = std::byte{9};
  Reader badtype(buf);
  CHECK(feed::decode(badtype).error() == Error::BadType);
  buf[0] = std::byte{'D'};
  buf[1] = std::byte{3};
  Reader badlen(buf);
  CHECK(feed::decode(badlen).error() == Error::BadLength);
  buf[1] = std::byte{16};
  Reader trunc(buf);
  CHECK(feed::decode(trunc).error() == Error::Truncated);
}

TEST(wire_writer_reports_overflow) {
  std::array<std::byte, 4> buf{};
  Writer w(buf);
  w.put(std::uint32_t{1});
  CHECK(w.ok());
  w.put(std::uint8_t{1});
  CHECK(!w.ok());
  CHECK_EQ(w.size(), std::size_t{4});
}

TEST(wire_gateway_inbound_roundtrip) {
  gw::InMsg in;
  in.type = gw::InType::Login;
  std::memcpy(in.login.user.data(), "trader01", 8);
  in.login.session = 9;
  in.login.next_expected = 44;
  std::array<std::byte, 128> buf{};
  Writer w(buf);
  gw::encode(in, w);
  CHECK_EQ(w.size(), gw::in_frame_size(gw::InType::Login));
  Reader r(w.written());
  auto d = gw::decode(r);
  CHECK(d && d->type == gw::InType::Login);
  if (d) {
    CHECK(std::memcmp(d->login.user.data(), "trader01", 8) == 0);
    CHECK_EQ(d->login.session, 9u);
    CHECK_EQ(d->login.next_expected, 44u);
  }

  in = gw::InMsg{};
  in.type = gw::InType::EnterOrder;
  in.order = NewOrder{77, 3, Side::Sell, OrderType::Market, TimeInForce::IOC, 1010, 250};
  w.reset();
  gw::encode(in, w);
  Reader r2(w.written());
  auto d2 = gw::decode(r2);
  CHECK(d2 && d2->type == gw::InType::EnterOrder);
  if (d2) {
    CHECK_EQ(d2->order.cl_id, 77u);
    CHECK_EQ(d2->order.symbol, 3u);
    CHECK(d2->order.side == Side::Sell && d2->order.type == OrderType::Market && d2->order.tif == TimeInForce::IOC);
    CHECK_EQ(d2->order.price, 1010);
    CHECK_EQ(d2->order.qty, 250u);
  }

  in = gw::InMsg{};
  in.type = gw::InType::Replace;
  in.replace = ReplaceOrder{1, 2, 3, 4};
  w.reset();
  gw::encode(in, w);
  Reader r3(w.written());
  auto d3 = gw::decode(r3);
  CHECK(d3 && d3->replace.cl_id == 1 && d3->replace.new_cl_id == 2 && d3->replace.price == 3 && d3->replace.qty == 4);
}

TEST(wire_gateway_partial_frame_does_not_consume) {
  gw::InMsg in;
  in.type = gw::InType::Cancel;
  in.cancel = CancelOrder{5, 6};
  std::array<std::byte, 64> buf{};
  Writer w(buf);
  gw::encode(in, w);
  // Feed the decoder one byte short of a whole frame, then the rest.
  Reader r(w.written().first(w.size() - 1));
  auto d = gw::decode(r);
  CHECK(!d && d.error() == Error::Truncated);
  CHECK_EQ(r.pos(), std::size_t{0});
  Reader r2(w.written());
  auto d2 = gw::decode(r2);
  CHECK(d2 && d2->cancel.cl_id == 5 && d2->cancel.qty == 6);
  CHECK_EQ(r2.remaining(), std::size_t{0});
}

TEST(wire_gateway_outbound_roundtrip) {
  const gw::OutType types[] = {gw::OutType::LoginAccepted, gw::OutType::LoginRejected, gw::OutType::Accepted,
                               gw::OutType::Executed, gw::OutType::Canceled, gw::OutType::Replaced,
                               gw::OutType::Rejected, gw::OutType::Heartbeat, gw::OutType::EndOfSession};
  for (auto t : types) {
    gw::OutMsg m;
    m.type = t;
    m.seq = 99;
    m.ts = 123456789;
    m.session = 4;
    m.cl_id = 10;
    m.cl_id2 = 11;
    m.oid = 12;
    m.symbol = 2;
    m.side = Side::Sell;
    m.price = 500;
    m.qty = 30;
    m.leaves = 20;
    m.tif = TimeInForce::IOC;
    m.match_id = 8;
    m.reason = 3;
    std::array<std::byte, 128> buf{};
    Writer w(buf);
    gw::encode(m, w);
    CHECK_EQ(w.size(), gw::out_frame_size(t));
    Reader r(w.written());
    auto d = gw::decode_out(r);
    CHECK(d && d->type == t);
    if (!d) continue;
    switch (t) {
      case gw::OutType::LoginAccepted:
        CHECK(d->session == 4 && d->seq == 99);
        break;
      case gw::OutType::LoginRejected:
        CHECK(d->reason == 3);
        break;
      case gw::OutType::Accepted:
        CHECK(d->seq == 99 && d->ts == 123456789 && d->cl_id == 10 && d->oid == 12 && d->symbol == 2 &&
              d->side == Side::Sell && d->price == 500 && d->qty == 30 && d->tif == TimeInForce::IOC);
        break;
      case gw::OutType::Executed:
        CHECK(d->cl_id == 10 && d->qty == 30 && d->price == 500 && d->match_id == 8 && d->leaves == 20);
        break;
      case gw::OutType::Canceled:
        CHECK(d->cl_id == 10 && d->qty == 30 && d->leaves == 20 && d->reason == 3);
        break;
      case gw::OutType::Replaced:
        CHECK(d->cl_id == 10 && d->cl_id2 == 11 && d->oid == 12 && d->price == 500 && d->qty == 30);
        break;
      case gw::OutType::Rejected:
        CHECK(d->cl_id == 10 && d->reason == 3);
        break;
      case gw::OutType::Heartbeat:
      case gw::OutType::EndOfSession:
        break;
    }
  }
}

TEST(wire_retrans_request_roundtrip) {
  retrans::Request q;
  q.type = retrans::ReqType::Snapshot;
  q.session = 3;
  q.start = 1'000'000;
  q.count = 500;
  std::array<std::byte, 32> buf{};
  Writer w(buf);
  retrans::encode(q, w);
  CHECK_EQ(w.size(), retrans::kFrameSize);
  Reader r(w.written());
  auto d = retrans::decode(r);
  CHECK(d && d->type == retrans::ReqType::Snapshot && d->session == 3 && d->start == 1'000'000 && d->count == 500);
}
