#pragma once

#include "types.hpp"
#include "wire.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <vector>

namespace tapeline {

// Order-entry sessions. The gateway owns the per-session outbound sequence and
// a log of every sequenced frame it has produced, so a client that drops and
// reconnects can log back in with the sequence number it last saw and have the
// rest replayed. It also routes engine events to the right session: a fill
// produces one Executed frame for the aggressor and one for the resting side.
class Gateway : public EventSink {
 public:
  enum class LoginReject : std::uint8_t {
    UnknownSession = 1,
    AlreadyConnected = 2,
    BadSequence = 3,
  };

  struct Session {
    SessionId id{};
    std::string user;
    bool connected = false;
    SeqNo out_seq = 1;                          // next sequenced frame number
    std::vector<std::vector<std::byte>> log;    // frame with seq n lives at log[n-1]
    std::vector<std::byte> outbuf;              // queued for the socket
    std::uint64_t frames_in = 0;
    std::uint64_t frames_out = 0;
    std::uint64_t replayed = 0;
  };

  Gateway() { sessions_.emplace_back(); } // id 0 is never a session

  // Handle a Login frame. A session id of 0 creates a session; otherwise the
  // client is resuming and gets everything from `next_expected` replayed.
  std::expected<SessionId, LoginReject> login(const wire::gw::Login& l) {
    if (l.session == 0) {
      Session s;
      s.id = static_cast<SessionId>(sessions_.size());
      s.user.assign(l.user.data(), l.user.size());
      s.connected = true;
      sessions_.push_back(std::move(s));
      Session& ns = sessions_.back();
      send_unsequenced(ns, login_accepted(ns));
      return ns.id;
    }
    if (l.session >= sessions_.size()) return std::unexpected(LoginReject::UnknownSession);
    Session& s = sessions_[l.session];
    if (s.connected) return std::unexpected(LoginReject::AlreadyConnected);
    if (l.next_expected == 0 || l.next_expected > s.out_seq) {
      return std::unexpected(LoginReject::BadSequence);
    }
    s.connected = true;
    send_unsequenced(s, login_accepted(s));
    for (SeqNo seq = l.next_expected; seq < s.out_seq; ++seq) {
      const auto& frame = s.log[seq - 1];
      s.outbuf.insert(s.outbuf.end(), frame.begin(), frame.end());
      ++s.replayed;
    }
    return s.id;
  }

  static std::vector<std::byte> login_rejected_frame(LoginReject why) {
    wire::gw::OutMsg m;
    m.type = wire::gw::OutType::LoginRejected;
    m.reason = static_cast<std::uint8_t>(why);
    return encode(m);
  }

  void disconnect(SessionId id) {
    if (id == 0 || id >= sessions_.size()) return;
    sessions_[id].connected = false;
    sessions_[id].outbuf.clear();
  }

  [[nodiscard]] Session& session(SessionId id) { return sessions_[id]; }
  [[nodiscard]] const Session& session(SessionId id) const { return sessions_[id]; }
  [[nodiscard]] std::size_t session_count() const noexcept { return sessions_.size() - 1; }
  [[nodiscard]] bool valid(SessionId id) const noexcept { return id > 0 && id < sessions_.size(); }

  void heartbeat_all() {
    wire::gw::OutMsg m;
    m.type = wire::gw::OutType::Heartbeat;
    for (std::size_t i = 1; i < sessions_.size(); ++i) {
      if (sessions_[i].connected) send_unsequenced(sessions_[i], m);
    }
  }

  void end_of_session_all() {
    wire::gw::OutMsg m;
    m.type = wire::gw::OutType::EndOfSession;
    for (std::size_t i = 1; i < sessions_.size(); ++i) send_sequenced(sessions_[i], m);
  }

  void on_event(const Event& ev) override {
    using wire::gw::OutType;
    wire::gw::OutMsg m;
    m.ts = ev.ts;
    switch (ev.type) {
      case EventType::Accepted:
        m.type = OutType::Accepted;
        m.cl_id = ev.cl_id;
        m.oid = ev.oid;
        m.symbol = ev.symbol;
        m.side = ev.side;
        m.price = ev.price;
        m.qty = ev.qty;
        m.tif = ev.tif;
        route(ev.session, m);
        break;
      case EventType::Rested:
        break;
      case EventType::Executed: {
        m.type = OutType::Executed;
        m.qty = ev.qty;
        m.price = ev.price;
        m.match_id = ev.match_id;
        m.cl_id = ev.cl_id;
        m.leaves = ev.leaves;
        route(ev.session, m);
        wire::gw::OutMsg p = m;
        p.cl_id = ev.resting_cl_id;
        p.leaves = ev.resting_leaves;
        route(ev.resting_session, p);
        break;
      }
      case EventType::Canceled:
        m.type = OutType::Canceled;
        m.cl_id = ev.cl_id;
        m.qty = ev.qty;
        m.leaves = ev.leaves;
        m.reason = static_cast<std::uint8_t>(ev.cancel_reason);
        route(ev.session, m);
        break;
      case EventType::Replaced:
        m.type = OutType::Replaced;
        m.cl_id = ev.old_cl_id;
        m.cl_id2 = ev.cl_id;
        m.oid = ev.oid;
        m.price = ev.price;
        m.qty = ev.qty;
        route(ev.session, m);
        break;
      case EventType::Rejected:
        m.type = OutType::Rejected;
        m.cl_id = ev.cl_id;
        m.reason = static_cast<std::uint8_t>(ev.reject);
        route(ev.session, m);
        break;
    }
  }

  static std::vector<std::byte> encode(const wire::gw::OutMsg& m) {
    std::array<std::byte, wire::gw::kMaxFrame> buf{};
    wire::Writer w(buf);
    wire::gw::encode(m, w);
    return std::vector<std::byte>(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(w.size()));
  }

 private:
  static wire::gw::OutMsg login_accepted(const Session& s) {
    wire::gw::OutMsg m;
    m.type = wire::gw::OutType::LoginAccepted;
    m.session = s.id;
    m.seq = s.out_seq;
    return m;
  }

  void route(SessionId id, wire::gw::OutMsg& m) {
    if (!valid(id)) return;
    send_sequenced(sessions_[id], m);
  }

  void send_sequenced(Session& s, wire::gw::OutMsg& m) {
    m.seq = s.out_seq++;
    std::vector<std::byte> frame = encode(m);
    if (s.connected) s.outbuf.insert(s.outbuf.end(), frame.begin(), frame.end());
    s.log.push_back(std::move(frame));
    ++s.frames_out;
  }

  void send_unsequenced(Session& s, const wire::gw::OutMsg& m) {
    std::vector<std::byte> frame = encode(m);
    s.outbuf.insert(s.outbuf.end(), frame.begin(), frame.end());
    ++s.frames_out;
  }

  std::vector<Session> sessions_;
};

} // namespace tapeline
