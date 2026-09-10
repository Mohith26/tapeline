#pragma once

// A very small test registry. No dependencies, one binary, exit code is the
// number of failed tests.

#include <cstdint>
#include <cstdio>
#include <format>
#include <functional>
#include <string>
#include <vector>

namespace tt {

struct Case {
  const char* name;
  std::function<void()> fn;
};

inline std::vector<Case>& cases() {
  static std::vector<Case> v;
  return v;
}

inline std::uint64_t& checks() {
  static std::uint64_t n = 0;
  return n;
}

inline std::vector<std::string>& failures() {
  static std::vector<std::string> v;
  return v;
}

struct Register {
  Register(const char* name, std::function<void()> fn) { cases().push_back({name, std::move(fn)}); }
};

inline void check(bool ok, const char* expr, const char* file, int line, const std::string& msg = {}) {
  ++checks();
  if (!ok) {
    failures().push_back(std::format("{}:{}: CHECK({}) {}", file, line, expr, msg));
  }
}

} // namespace tt

#define TEST(name)                                                                  \
  static void test_##name();                                                        \
  static tt::Register reg_##name(#name, test_##name);                               \
  static void test_##name()

#define CHECK(expr) tt::check(static_cast<bool>(expr), #expr, __FILE__, __LINE__)
#define CHECK_MSG(expr, msg) tt::check(static_cast<bool>(expr), #expr, __FILE__, __LINE__, (msg))
#define CHECK_EQ(a, b)                                                                     \
  tt::check((a) == (b), #a " == " #b, __FILE__, __LINE__, std::format("({} vs {})", (a), (b)))
