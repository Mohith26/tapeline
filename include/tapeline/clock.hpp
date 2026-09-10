#pragma once

#include "types.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <format>
#include <string>
#include <vector>

namespace tapeline {

inline Timestamp now_ns() noexcept {
  return static_cast<Timestamp>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline Timestamp wall_ns() noexcept {
  return static_cast<Timestamp>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

// Collects raw samples and reports percentiles by sorting at the end. Good
// enough for a few million samples; it is not a streaming estimator.
class Histogram {
 public:
  explicit Histogram(std::size_t reserve = 0) { samples_.reserve(reserve); }

  void add(std::uint64_t v) {
    samples_.push_back(v);
    sorted_ = false;
  }
  [[nodiscard]] std::size_t count() const noexcept { return samples_.size(); }

  [[nodiscard]] std::uint64_t percentile(double p) {
    if (samples_.empty()) return 0;
    sort();
    const double rank = p / 100.0 * static_cast<double>(samples_.size() - 1);
    const auto idx = static_cast<std::size_t>(rank + 0.5);
    return samples_[std::min(idx, samples_.size() - 1)];
  }

  [[nodiscard]] std::uint64_t max() {
    if (samples_.empty()) return 0;
    sort();
    return samples_.back();
  }

  [[nodiscard]] std::uint64_t min() {
    if (samples_.empty()) return 0;
    sort();
    return samples_.front();
  }

  [[nodiscard]] double mean() const {
    if (samples_.empty()) return 0;
    long double sum = 0;
    for (auto v : samples_) sum += static_cast<long double>(v);
    return static_cast<double>(sum / static_cast<long double>(samples_.size()));
  }

  [[nodiscard]] std::string json(const char* unit = "ns") {
    return std::format(
        "{{\"unit\": \"{}\", \"count\": {}, \"min\": {}, \"mean\": {:.1f}, \"p50\": {}, \"p90\": {}, "
        "\"p99\": {}, \"p99_9\": {}, \"max\": {}}}",
        unit, count(), min(), mean(), percentile(50), percentile(90), percentile(99),
        percentile(99.9), max());
  }

 private:
  void sort() {
    if (!sorted_) {
      std::sort(samples_.begin(), samples_.end());
      sorted_ = true;
    }
  }

  std::vector<std::uint64_t> samples_;
  bool sorted_ = false;
};

} // namespace tapeline
