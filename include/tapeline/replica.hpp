#pragma once

#include "engine.hpp"
#include "hash.hpp"
#include "types.hpp"

#include <cstdint>
#include <span>

namespace tapeline {

// Hashes the event stream an engine produces. Two engines fed the same log
// must produce the same value, which is a much stronger check than comparing
// book state alone: it covers fills, rejects, and cancel reasons too.
class EventHasher : public EventSink {
 public:
  void on_event(const Event& ev) override {
    h_.add(ev.type);
    h_.add(ev.seq);
    h_.add(ev.session);
    h_.add(ev.cl_id);
    h_.add(ev.oid);
    h_.add(ev.symbol);
    h_.add(ev.side);
    h_.add(ev.price);
    h_.add(ev.qty);
    h_.add(ev.leaves);
    h_.add(ev.resting_oid);
    h_.add(ev.resting_cl_id);
    h_.add(ev.resting_leaves);
    h_.add(ev.match_id);
    h_.add(ev.old_oid);
    h_.add(ev.prev_qty);
    h_.add(static_cast<std::uint8_t>(ev.kept_priority));
    h_.add(ev.reject);
    h_.add(ev.cancel_reason);
    ++count_;
  }

  [[nodiscard]] std::uint64_t value() const noexcept { return h_.value(); }
  [[nodiscard]] std::uint64_t count() const noexcept { return count_; }

 private:
  Fnv1a64 h_;
  std::uint64_t count_ = 0;
};

// A hot standby: an engine that follows the sequenced log and can be compared
// against the primary at any sequence number. Failover is nothing more than
// pointing new records at this engine, since it already holds the full state.
class Replica {
 public:
  explicit Replica(std::uint32_t n_symbols) : engine_(n_symbols, &hasher_) {}

  // Apply every record in `log` newer than what has been applied so far.
  std::size_t follow(std::span<const Inbound> log) {
    std::size_t n = 0;
    for (const Inbound& rec : log) {
      if (rec.seq <= applied_) continue;
      engine_.apply(rec);
      applied_ = rec.seq;
      ++n;
    }
    return n;
  }

  [[nodiscard]] SeqNo applied() const noexcept { return applied_; }
  [[nodiscard]] const Engine& engine() const noexcept { return engine_; }
  [[nodiscard]] Engine& engine() noexcept { return engine_; }
  [[nodiscard]] std::uint64_t state_hash() const { return engine_.state_hash(); }
  [[nodiscard]] std::uint64_t event_hash() const noexcept { return hasher_.value(); }

  [[nodiscard]] bool in_sync_with(const Engine& primary) const {
    return engine_.state_hash() == primary.state_hash();
  }

 private:
  EventHasher hasher_;
  Engine engine_;
  SeqNo applied_ = 0;
};

} // namespace tapeline
