#pragma once

#include "book.hpp"
#include "hash.hpp"
#include "types.hpp"
#include "wire.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tapeline {

// Turns engine events into the market-data feed: ITCH-style messages packed
// into MoldUDP-style packets with one session-wide sequence number. Every
// message it has ever sent (up to the ring capacity) is kept so a handler that
// missed a packet can ask for exactly the messages it lost, and a handler that
// is too far behind can ask for a snapshot instead.
class FeedPublisher : public EventSink {
 public:
  using PacketSink = std::function<void(std::span<const std::byte>)>;

  struct Stats {
    std::uint64_t packets = 0;
    std::uint64_t messages = 0;
    std::uint64_t bytes = 0;
    std::uint64_t retrans_packets = 0;
    std::uint64_t snapshot_packets = 0;
  };

  explicit FeedPublisher(std::uint32_t session, std::size_t ring_capacity = std::size_t{1} << 20)
      : session_(session), ring_capacity_(ring_capacity) {
    staging_.resize(wire::feed::kMaxPacket);
    open_packet();
  }

  void set_sink(PacketSink sink) { sink_ = std::move(sink); }
  [[nodiscard]] std::uint32_t session() const noexcept { return session_; }
  [[nodiscard]] SeqNo next_seq() const noexcept { return next_seq_; }
  [[nodiscard]] const Stats& stats() const noexcept { return stats_; }
  [[nodiscard]] SeqNo oldest_retained() const noexcept { return ring_base_; }

  void on_event(const Event& ev) override {
    wire::feed::Msg m;
    m.ts = ev.ts;
    switch (ev.type) {
      case EventType::Rested:
        m.type = wire::feed::MsgType::AddOrder;
        m.oid = ev.oid;
        m.symbol = ev.symbol;
        m.side = ev.side;
        m.price = ev.price;
        m.qty = ev.qty;
        append(m);
        break;
      case EventType::Executed:
        // The feed only ever talks about resting orders. The aggressor's fill
        // is implied; if anything is left of it, a Rested event follows.
        m.type = wire::feed::MsgType::Executed;
        m.oid = ev.resting_oid;
        m.qty = ev.qty;
        m.match_id = ev.match_id;
        m.price = ev.price;
        append(m);
        break;
      case EventType::Canceled:
        if (!ev.was_resting) break; // an IOC remainder never showed up on the feed
        m.oid = ev.oid;
        if (ev.leaves == 0) {
          m.type = wire::feed::MsgType::Delete;
        } else {
          m.type = wire::feed::MsgType::Cancel;
          m.qty = ev.qty;
        }
        append(m);
        break;
      case EventType::Replaced:
        if (ev.kept_priority) {
          m.type = wire::feed::MsgType::Cancel;
          m.oid = ev.oid;
          m.qty = ev.prev_qty - ev.qty;
        } else {
          m.type = wire::feed::MsgType::Delete;
          m.oid = ev.old_oid;
        }
        append(m);
        break;
      case EventType::Accepted:
      case EventType::Rejected:
        break;
    }
  }

  void system_event(Timestamp ts, wire::feed::SystemCode code) {
    wire::feed::Msg m;
    m.type = wire::feed::MsgType::SystemEvent;
    m.ts = ts;
    m.code = code;
    append(m);
  }

  void heartbeat(Timestamp ts) {
    wire::feed::Msg m;
    m.type = wire::feed::MsgType::Heartbeat;
    m.ts = ts;
    append(m);
  }

  // Send whatever has been staged since the last flush as one packet.
  void flush() {
    if (count_ == 0) return;
    wire::Writer w(staging_);
    wire::feed::PacketHeader h;
    h.kind = wire::feed::PacketKind::Live;
    h.session = session_;
    h.first_seq = packet_first_seq_;
    h.count = count_;
    h.length = static_cast<std::uint16_t>(size_);
    wire::feed::write_header(w, h);
    ++stats_.packets;
    stats_.bytes += size_;
    if (sink_) sink_(std::span<const std::byte>(staging_.data(), size_));
    open_packet();
  }

  // Re-encode messages [start, start + count) into Retrans packets handed to
  // `out`. Returns false when `start` has already fallen out of the ring.
  template <class Out>
  bool retransmit(SeqNo start, std::uint16_t count, Out&& out) {
    if (start < ring_base_ || start >= next_seq_) return false;
    const SeqNo end = std::min<SeqNo>(start + count, next_seq_);
    std::array<std::byte, wire::feed::kMaxPacket> buf{};
    SeqNo seq = start;
    while (seq < end) {
      wire::Writer w(buf);
      w.put_bytes(std::span<const std::byte>(buf.data(), wire::feed::kHeaderSize)); // placeholder
      std::uint16_t n = 0;
      const SeqNo first = seq;
      while (seq < end) {
        const RingEntry& e = ring_[seq - ring_base_];
        if (w.size() + e.len > wire::feed::kMaxPacket) break;
        w.put_bytes(std::span<const std::byte>(e.bytes.data(), e.len));
        ++n;
        ++seq;
      }
      finish_packet(buf, w.size(), wire::feed::PacketKind::Retrans, first, n);
      ++stats_.retrans_packets;
      out(std::span<const std::byte>(buf.data(), w.size()));
    }
    return true;
  }

  // Serialise the current resting state of every book as Snapshot packets.
  // The header's first_seq tells the handler which live sequence number to
  // expect once it has applied the whole snapshot.
  template <class Books, class Out>
  void snapshot(const Books& books, Timestamp ts, Out&& out) {
    std::array<std::byte, wire::feed::kMaxPacket> buf{};
    wire::Writer w(buf);
    std::uint16_t n = 0;
    auto begin_packet = [&] {
      w.reset();
      w.put_bytes(std::span<const std::byte>(buf.data(), wire::feed::kHeaderSize));
      n = 0;
    };
    auto emit_packet = [&] {
      finish_packet(buf, w.size(), wire::feed::PacketKind::Snapshot, next_seq_, n);
      ++stats_.snapshot_packets;
      out(std::span<const std::byte>(buf.data(), w.size()));
    };
    auto push = [&](const wire::feed::Msg& m) {
      if (w.size() + wire::feed::encoded_size(m.type) > wire::feed::kMaxPacket) {
        emit_packet();
        begin_packet();
      }
      wire::feed::encode(m, w);
      ++n;
    };
    begin_packet();
    for (std::uint32_t s = 0; s < books.symbols(); ++s) {
      books.book(s).for_each_resting([&](const Book::Order& o) {
        wire::feed::Msg m;
        m.type = wire::feed::MsgType::AddOrder;
        m.ts = ts;
        m.oid = o.id;
        m.symbol = s;
        m.side = o.side;
        m.price = o.price;
        m.qty = o.qty;
        push(m);
      });
    }
    wire::feed::Msg done;
    done.type = wire::feed::MsgType::SystemEvent;
    done.ts = ts;
    done.code = wire::feed::SystemCode::SnapshotEnd;
    push(done);
    emit_packet();
  }

 private:
  struct RingEntry {
    std::uint8_t len{};
    std::array<std::byte, 40> bytes{};
  };

  void open_packet() {
    size_ = wire::feed::kHeaderSize;
    count_ = 0;
    packet_first_seq_ = next_seq_;
  }

  void append(const wire::feed::Msg& m) {
    const std::size_t need = wire::feed::encoded_size(m.type);
    if (size_ + need > wire::feed::kMaxPacket) flush();
    wire::Writer w(std::span<std::byte>(staging_.data() + size_, staging_.size() - size_));
    wire::feed::encode(m, w);
    RingEntry e;
    e.len = static_cast<std::uint8_t>(w.size());
    std::memcpy(e.bytes.data(), staging_.data() + size_, w.size());
    ring_.push_back(e);
    if (ring_.size() > ring_capacity_) {
      ring_.pop_front();
      ++ring_base_;
    }
    size_ += w.size();
    ++count_;
    ++next_seq_;
    ++stats_.messages;
  }

  void finish_packet(std::span<std::byte> buf, std::size_t size, wire::feed::PacketKind kind,
                     SeqNo first, std::uint16_t count) {
    wire::Writer hw(buf);
    wire::feed::PacketHeader h;
    h.kind = kind;
    h.session = session_;
    h.first_seq = first;
    h.count = count;
    h.length = static_cast<std::uint16_t>(size);
    wire::feed::write_header(hw, h);
  }

  std::uint32_t session_;
  std::size_t ring_capacity_;
  PacketSink sink_;
  std::vector<std::byte> staging_;
  std::size_t size_ = 0;
  std::uint16_t count_ = 0;
  SeqNo packet_first_seq_ = 1;
  SeqNo next_seq_ = 1;
  std::deque<RingEntry> ring_;
  SeqNo ring_base_ = 1;
  Stats stats_;
};

// Consumes feed packets, in any order and with any amount of loss, and keeps a
// copy of every book. Gaps are detected from the sequence numbers, buffered
// packets are held until the gap is filled, and the recovery requests go out
// through the two callbacks so the same class works over real sockets and in
// the unit tests.
class FeedHandler {
 public:
  using RetransRequester = std::function<void(SeqNo start, std::uint16_t count)>;

  struct Stats {
    std::uint64_t packets = 0;
    std::uint64_t messages = 0;
    std::uint64_t applied = 0;
    std::uint64_t gaps = 0;
    std::uint64_t retrans_requests = 0;
    std::uint64_t recovered = 0;
    std::uint64_t duplicates = 0;
    std::uint64_t buffered = 0;
    std::uint64_t snapshots = 0;
    std::uint64_t decode_errors = 0;
    std::uint64_t unknown_orders = 0;
    std::uint64_t heartbeats = 0;
  };

  explicit FeedHandler(std::uint32_t n_symbols, std::uint64_t retry_ns = 50'000'000) : retry_ns_(retry_ns) {
    books_.reserve(n_symbols);
    for (std::uint32_t s = 0; s < n_symbols; ++s) books_.emplace_back(s);
  }

  void set_retrans_requester(RetransRequester f) { request_ = std::move(f); }

  [[nodiscard]] SeqNo next_seq() const noexcept { return next_seq_; }
  [[nodiscard]] bool gap_outstanding() const noexcept { return !pending_.empty(); }
  [[nodiscard]] bool end_of_session() const noexcept { return ended_; }
  [[nodiscard]] bool started() const noexcept { return started_; }
  [[nodiscard]] const Stats& stats() const noexcept { return stats_; }
  [[nodiscard]] const Book& book(SymbolId s) const { return books_[s]; }
  [[nodiscard]] std::uint32_t symbols() const noexcept {
    return static_cast<std::uint32_t>(books_.size());
  }
  [[nodiscard]] std::uint32_t session() const noexcept { return session_; }

  [[nodiscard]] std::uint64_t books_hash() const {
    Fnv1a64 h;
    for (const Book& b : books_) h.add(b.hash());
    return h.value();
  }

  // Feed one packet (live UDP, retransmitted, or snapshot). `now` is only used
  // to pace repeat retransmission requests.
  void on_packet(std::span<const std::byte> bytes, Timestamp now = 0) {
    ++stats_.packets;
    wire::Reader r(bytes);
    auto hdr = wire::feed::read_header(r);
    if (!hdr || hdr->length > bytes.size()) {
      ++stats_.decode_errors;
      return;
    }
    session_ = hdr->session;
    const auto payload = bytes.subspan(wire::feed::kHeaderSize, hdr->length - wire::feed::kHeaderSize);

    if (hdr->kind == wire::feed::PacketKind::Snapshot) {
      on_snapshot(*hdr, payload);
      return;
    }
    if (hdr->kind == wire::feed::PacketKind::Retrans) ++stats_.recovered;

    const SeqNo first = hdr->first_seq;
    const SeqNo last_plus_one = first + hdr->count;
    if (last_plus_one <= next_seq_) {
      ++stats_.duplicates;
      return;
    }
    if (first > next_seq_) {
      // Future packet: hold on to it and ask for what is missing.
      if (pending_.empty()) ++stats_.gaps;
      auto [it, inserted] = pending_.try_emplace(first);
      if (inserted) {
        it->second.assign(bytes.begin(), bytes.begin() + hdr->length);
        ++stats_.buffered;
      }
      request_missing(now, true);
      return;
    }
    apply_packet(*hdr, payload);
    drain(now);
  }

  // Call periodically; re-issues a retransmission request if an earlier one
  // went unanswered for longer than the retry interval.
  void on_timer(Timestamp now) {
    if (pending_.empty()) return;
    request_missing(now, false);
  }

 private:
  void request_missing(Timestamp now, bool fresh) {
    if (pending_.empty()) return;
    const SeqNo want_end = pending_.begin()->first;
    if (!fresh && now != 0 && last_request_ != 0 && now - last_request_ < retry_ns_) return;
    if (fresh && last_request_ != 0 && requested_start_ == next_seq_ && requested_end_ == want_end &&
        (now == 0 || now - last_request_ < retry_ns_)) {
      return; // same request already in flight
    }
    const SeqNo count = std::min<SeqNo>(want_end - next_seq_, 65535);
    requested_start_ = next_seq_;
    requested_end_ = want_end;
    last_request_ = now == 0 ? 1 : now;
    ++stats_.retrans_requests;
    if (request_) request_(next_seq_, static_cast<std::uint16_t>(count));
  }

  void drain(Timestamp now) {
    while (!pending_.empty()) {
      auto it = pending_.begin();
      if (it->first > next_seq_) break;
      std::vector<std::byte> packet = std::move(it->second);
      pending_.erase(it);
      wire::Reader r(packet);
      auto hdr = wire::feed::read_header(r);
      if (!hdr) continue;
      const SeqNo end = hdr->first_seq + hdr->count;
      if (end <= next_seq_) {
        ++stats_.duplicates;
        continue;
      }
      apply_packet(*hdr, std::span<const std::byte>(packet).subspan(wire::feed::kHeaderSize, hdr->length - wire::feed::kHeaderSize));
    }
    if (pending_.empty()) {
      last_request_ = 0;
    } else {
      request_missing(now, true);
    }
  }

  // Apply the messages of a packet whose first_seq <= next_seq_, skipping any
  // the handler has already seen.
  void apply_packet(const wire::feed::PacketHeader& hdr, std::span<const std::byte> payload) {
    wire::Reader r(payload);
    SeqNo seq = hdr.first_seq;
    for (std::uint16_t i = 0; i < hdr.count; ++i, ++seq) {
      auto m = wire::feed::decode(r);
      if (!m) {
        ++stats_.decode_errors;
        return;
      }
      ++stats_.messages;
      if (seq < next_seq_) {
        ++stats_.duplicates;
        continue;
      }
      apply(*m);
      next_seq_ = seq + 1;
    }
  }

  void on_snapshot(const wire::feed::PacketHeader& hdr, std::span<const std::byte> payload) {
    if (!in_snapshot_) {
      in_snapshot_ = true;
      ++stats_.snapshots;
      for (Book& b : books_) b = Book(b.symbol());
      index_.clear();
    }
    wire::Reader r(payload);
    for (std::uint16_t i = 0; i < hdr.count; ++i) {
      auto m = wire::feed::decode(r);
      if (!m) {
        ++stats_.decode_errors;
        return;
      }
      ++stats_.messages;
      if (m->type == wire::feed::MsgType::SystemEvent && m->code == wire::feed::SystemCode::SnapshotEnd) {
        in_snapshot_ = false;
        next_seq_ = hdr.first_seq;
        started_ = true;
        // Anything buffered from before the snapshot point is now stale.
        while (!pending_.empty()) {
          auto it = pending_.begin();
          wire::Reader pr(it->second);
          auto ph = wire::feed::read_header(pr);
          if (ph && ph->first_seq + ph->count > next_seq_) break;
          pending_.erase(it);
        }
        drain(0);
        return;
      }
      apply(*m);
    }
  }

  void apply(const wire::feed::Msg& m) {
    using wire::feed::MsgType;
    ++stats_.applied;
    switch (m.type) {
      case MsgType::AddOrder: {
        if (m.symbol >= books_.size()) return;
        const std::uint32_t slot = books_[m.symbol].rest(m.oid, m.side, m.price, m.qty, 0, 0);
        index_[m.oid] = {m.symbol, slot};
        break;
      }
      case MsgType::Executed:
      case MsgType::Cancel: {
        auto it = index_.find(m.oid);
        if (it == index_.end()) {
          ++stats_.unknown_orders;
          return;
        }
        Book& b = books_[it->second.first];
        if (m.qty >= b.at(it->second.second).qty) {
          b.remove(it->second.second);
          index_.erase(it);
        } else {
          b.reduce(it->second.second, m.qty);
        }
        break;
      }
      case MsgType::Delete: {
        auto it = index_.find(m.oid);
        if (it == index_.end()) {
          ++stats_.unknown_orders;
          return;
        }
        books_[it->second.first].remove(it->second.second);
        index_.erase(it);
        break;
      }
      case MsgType::SystemEvent:
        if (m.code == wire::feed::SystemCode::StartOfSession) started_ = true;
        if (m.code == wire::feed::SystemCode::EndOfSession) ended_ = true;
        break;
      case MsgType::Heartbeat:
        ++stats_.heartbeats;
        break;
    }
  }

  std::uint64_t retry_ns_;
  RetransRequester request_;
  std::vector<Book> books_;
  std::unordered_map<OrderId, std::pair<SymbolId, std::uint32_t>> index_;
  std::map<SeqNo, std::vector<std::byte>> pending_;
  SeqNo next_seq_ = 1;
  SeqNo requested_start_ = 0;
  SeqNo requested_end_ = 0;
  Timestamp last_request_ = 0;
  std::uint32_t session_ = 0;
  bool in_snapshot_ = false;
  bool started_ = false;
  bool ended_ = false;
  Stats stats_;
};

} // namespace tapeline
