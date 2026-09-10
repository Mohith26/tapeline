#pragma once

#include "types.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <expected>
#include <span>
#include <string>
#include <vector>

namespace tapeline {

// The sequencer is the one place where time and ordering enter the system.
// Every inbound record gets the next sequence number and a timestamp, is
// appended to an in-memory log, and (optionally) to an append-only file. The
// engine consumes the log and nothing else, so a standby that reads the same
// log ends up in the same state.
class Sequencer {
 public:
  Sequencer() = default;
  Sequencer(const Sequencer&) = delete;
  Sequencer& operator=(const Sequencer&) = delete;
  ~Sequencer() { close_file(); }

  static constexpr char kMagic[8] = {'T', 'A', 'P', 'E', 'L', 'O', 'G', '1'};

  bool open_file(const std::string& path) {
    close_file();
    file_ = std::fopen(path.c_str(), "wb");
    if (!file_) return false;
    std::fwrite(kMagic, 1, sizeof(kMagic), file_);
    const std::uint32_t rec_size = sizeof(Inbound);
    std::fwrite(&rec_size, sizeof(rec_size), 1, file_);
    return true;
  }

  void close_file() {
    if (file_) {
      std::fflush(file_);
      std::fclose(file_);
      file_ = nullptr;
    }
  }

  const Inbound& append(Inbound rec, Timestamp now) {
    rec.seq = next_++;
    rec.ts = now;
    log_.push_back(rec);
    if (file_) std::fwrite(&log_.back(), sizeof(Inbound), 1, file_);
    return log_.back();
  }

  void flush() {
    if (file_) std::fflush(file_);
  }

  void reserve(std::size_t records) { log_.reserve(records); }

  [[nodiscard]] SeqNo next() const noexcept { return next_; }
  [[nodiscard]] std::size_t size() const noexcept { return log_.size(); }
  [[nodiscard]] std::span<const Inbound> log() const noexcept { return log_; }

  // Read a log file written by open_file/append. The record layout is the
  // host's, which is fine for a standby on the same platform and is the
  // documented limitation of this format.
  static std::expected<std::vector<Inbound>, std::string> load(const std::string& path) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return std::unexpected("cannot open " + path);
    char magic[8];
    std::uint32_t rec_size = 0;
    if (std::fread(magic, 1, 8, f) != 8 || std::memcmp(magic, kMagic, 8) != 0) {
      std::fclose(f);
      return std::unexpected("bad log magic");
    }
    if (std::fread(&rec_size, sizeof(rec_size), 1, f) != 1 || rec_size != sizeof(Inbound)) {
      std::fclose(f);
      return std::unexpected("record size mismatch");
    }
    std::vector<Inbound> out;
    Inbound rec;
    while (std::fread(&rec, sizeof(Inbound), 1, f) == 1) out.push_back(rec);
    std::fclose(f);
    return out;
  }

 private:
  std::vector<Inbound> log_;
  SeqNo next_ = 1;
  std::FILE* file_ = nullptr;
};

} // namespace tapeline
