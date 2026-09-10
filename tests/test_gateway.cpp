#include "tapeline/engine.hpp"
#include "tapeline/gateway.hpp"
#include "tapeline/sequencer.hpp"
#include "test.hpp"

#include <cstring>
#include <vector>

using namespace tapeline;

namespace {

std::vector<wire::gw::OutMsg> drain(Gateway::Session& s) {
  std::vector<wire::gw::OutMsg> out;
  wire::Reader r(s.outbuf);
  while (true) {
    auto m = wire::gw::decode_out(r);
    if (!m) break;
    out.push_back(*m);
  }
  s.outbuf.clear();
  return out;
}

wire::gw::Login login_frame(const char* user, SessionId session = 0, SeqNo next = 0) {
  wire::gw::Login l;
  std::memcpy(l.user.data(), user, 8);
  l.session = session;
  l.next_expected = next;
  return l;
}

} // namespace

TEST(gateway_login_creates_sessions) {
  Gateway gw;
  auto a = gw.login(login_frame("alpha   "));
  auto b = gw.login(login_frame("bravo   "));
  CHECK(a && *a == 1);
  CHECK(b && *b == 2);
  CHECK_EQ(gw.session_count(), std::size_t{2});
  auto msgs = drain(gw.session(1));
  CHECK_EQ(msgs.size(), std::size_t{1});
  CHECK(msgs[0].type == wire::gw::OutType::LoginAccepted && msgs[0].session == 1 && msgs[0].seq == 1);
  auto bad = gw.login(login_frame("nobody  ", 9, 1));
  CHECK(!bad && bad.error() == Gateway::LoginReject::UnknownSession);
  auto dup = gw.login(login_frame("alpha   ", 1, 1));
  CHECK(!dup && dup.error() == Gateway::LoginReject::AlreadyConnected);
}

TEST(gateway_routes_fill_to_both_sides) {
  Gateway gw;
  Sequencer seq;
  Engine engine(1, &gw);
  gw.login(login_frame("maker   "));
  gw.login(login_frame("taker   "));
  Inbound r;
  r.kind = InboundKind::SessionOpen;
  r.session = 1;
  engine.apply(seq.append(r, 1));
  r.session = 2;
  engine.apply(seq.append(r, 2));
  r.kind = InboundKind::NewOrder;
  r.session = 1;
  r.new_order = NewOrder{100, 0, Side::Sell, OrderType::Limit, TimeInForce::Day, 50, 10};
  engine.apply(seq.append(r, 3));
  r.session = 2;
  r.new_order = NewOrder{200, 0, Side::Buy, OrderType::Limit, TimeInForce::Day, 55, 4};
  engine.apply(seq.append(r, 4));

  auto maker = drain(gw.session(1));
  auto taker = drain(gw.session(2));
  // maker: LoginAccepted, Accepted, Executed
  CHECK_EQ(maker.size(), std::size_t{3});
  CHECK(maker[1].type == wire::gw::OutType::Accepted && maker[1].cl_id == 100 && maker[1].oid == 1 && maker[1].seq == 1);
  CHECK(maker[2].type == wire::gw::OutType::Executed && maker[2].cl_id == 100 && maker[2].qty == 4 &&
        maker[2].price == 50 && maker[2].leaves == 6 && maker[2].seq == 2);
  // taker: LoginAccepted, Accepted, Executed
  CHECK_EQ(taker.size(), std::size_t{3});
  CHECK(taker[2].type == wire::gw::OutType::Executed && taker[2].cl_id == 200 && taker[2].leaves == 0 &&
        taker[2].match_id == maker[2].match_id);
  CHECK_EQ(gw.session(1).out_seq, std::uint64_t{3});
}

TEST(gateway_resume_replays_missed_frames) {
  Gateway gw;
  Sequencer seq;
  Engine engine(1, &gw);
  gw.login(login_frame("resume  "));
  Inbound r;
  r.kind = InboundKind::SessionOpen;
  r.session = 1;
  engine.apply(seq.append(r, 1));
  r.kind = InboundKind::NewOrder;
  for (ClientOrderId cl = 1; cl <= 5; ++cl) {
    r.new_order = NewOrder{cl, 0, Side::Buy, OrderType::Limit, TimeInForce::Day, 10, 1};
    engine.apply(seq.append(r, 1 + cl));
  }
  auto first = drain(gw.session(1));
  CHECK_EQ(first.size(), std::size_t{6}); // login + 5 accepts
  CHECK_EQ(first.back().seq, std::uint64_t{5});

  gw.disconnect(1);
  // Two more accepts while the client is away; they go to the log only.
  for (ClientOrderId cl = 6; cl <= 7; ++cl) {
    r.new_order = NewOrder{cl, 0, Side::Buy, OrderType::Limit, TimeInForce::Day, 10, 1};
    engine.apply(seq.append(r, 10 + cl));
  }
  CHECK(gw.session(1).outbuf.empty());

  // The client saw up to seq 4 before the drop and asks for 5 onward.
  auto again = gw.login(login_frame("resume  ", 1, 5));
  CHECK(again && *again == 1);
  auto replayed = drain(gw.session(1));
  CHECK_EQ(replayed.size(), std::size_t{4}); // LoginAccepted + seq 5, 6, 7
  CHECK(replayed[0].type == wire::gw::OutType::LoginAccepted && replayed[0].seq == 8);
  CHECK(replayed[1].seq == 5 && replayed[1].cl_id == 5);
  CHECK(replayed[3].seq == 7 && replayed[3].cl_id == 7);
  CHECK_EQ(gw.session(1).replayed, std::uint64_t{3});

  gw.disconnect(1);
  auto bad = gw.login(login_frame("resume  ", 1, 99));
  CHECK(!bad && bad.error() == Gateway::LoginReject::BadSequence);
}

TEST(gateway_rejects_and_cancels_reach_the_session) {
  Gateway gw;
  Sequencer seq;
  Engine engine(1, &gw);
  gw.login(login_frame("solo    "));
  Inbound r;
  r.kind = InboundKind::SessionOpen;
  r.session = 1;
  engine.apply(seq.append(r, 1));
  r.kind = InboundKind::Cancel;
  r.cancel = CancelOrder{42, 0};
  engine.apply(seq.append(r, 2));
  r.kind = InboundKind::NewOrder;
  r.new_order = NewOrder{7, 0, Side::Sell, OrderType::Limit, TimeInForce::IOC, 10, 3};
  engine.apply(seq.append(r, 3));
  auto msgs = drain(gw.session(1));
  CHECK_EQ(msgs.size(), std::size_t{4});
  CHECK(msgs[1].type == wire::gw::OutType::Rejected && msgs[1].cl_id == 42 &&
        msgs[1].reason == static_cast<std::uint8_t>(RejectReason::UnknownOrder));
  CHECK(msgs[2].type == wire::gw::OutType::Accepted);
  CHECK(msgs[3].type == wire::gw::OutType::Canceled && msgs[3].qty == 3 &&
        msgs[3].reason == static_cast<std::uint8_t>(CancelReason::ImmediateOrCancel));
  gw.end_of_session_all();
  auto tail = drain(gw.session(1));
  CHECK(tail.size() == 1 && tail[0].type == wire::gw::OutType::EndOfSession);
}
