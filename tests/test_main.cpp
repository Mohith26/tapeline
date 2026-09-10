#include "test.hpp"

#include <chrono>
#include <cstdio>
#include <print>
#include <string>

int main(int argc, char** argv) {
  const std::string filter = argc > 1 ? argv[1] : "";
  const char* json_path = argc > 2 ? argv[2] : nullptr;
  int failed = 0;
  int ran = 0;
  const auto t0 = std::chrono::steady_clock::now();
  for (const auto& c : tt::cases()) {
    if (!filter.empty() && std::string(c.name).find(filter) == std::string::npos) continue;
    const auto before = tt::failures().size();
    const auto checks_before = tt::checks();
    c.fn();
    ++ran;
    const bool ok = tt::failures().size() == before;
    if (!ok) ++failed;
    std::println("{} {} ({} checks)", ok ? "PASS" : "FAIL", c.name, tt::checks() - checks_before);
  }
  for (const auto& f : tt::failures()) std::println("  {}", f);
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  std::println("{} tests, {} checks, {} failed, {} ms", ran, tt::checks(), failed, ms);
  if (json_path) {
    if (std::FILE* f = std::fopen(json_path, "w")) {
      std::print(f, "{{\"tests\": {}, \"checks\": {}, \"failed\": {}, \"ms\": {}}}\n", ran, tt::checks(), failed, ms);
      std::fclose(f);
    }
  }
  return failed;
}
