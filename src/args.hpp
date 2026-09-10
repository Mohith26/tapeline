#pragma once

#include <charconv>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

// Tiny --flag value parser shared by the programs in src/.
class Args {
 public:
  Args(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) argv_.emplace_back(argv[i]);
  }

  [[nodiscard]] bool has(std::string_view flag) const {
    for (const auto& a : argv_) {
      if (a == flag) return true;
    }
    return false;
  }

  [[nodiscard]] std::string get(std::string_view flag, std::string def) const {
    for (std::size_t i = 0; i + 1 < argv_.size(); ++i) {
      if (argv_[i] == flag) return argv_[i + 1];
    }
    return def;
  }

  [[nodiscard]] std::int64_t get_int(std::string_view flag, std::int64_t def) const {
    const std::string v = get(flag, "");
    if (v.empty()) return def;
    std::int64_t out = def;
    std::from_chars(v.data(), v.data() + v.size(), out);
    return out;
  }

  [[nodiscard]] double get_double(std::string_view flag, double def) const {
    const std::string v = get(flag, "");
    if (v.empty()) return def;
    return std::stod(v);
  }

 private:
  std::vector<std::string> argv_;
};

inline bool write_text(const std::string& path, const std::string& text) {
  std::FILE* f = std::fopen(path.c_str(), "w");
  if (!f) return false;
  std::fwrite(text.data(), 1, text.size(), f);
  std::fclose(f);
  return true;
}
