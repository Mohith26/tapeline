// Standby replay: read the exchange's durable sequencer log and rebuild the
// engine from it. The state hash it prints should match the one the exchange
// wrote at shutdown; the e2e script checks exactly that.

#include "args.hpp"
#include "tapeline/clock.hpp"
#include "tapeline/engine.hpp"
#include "tapeline/replica.hpp"
#include "tapeline/sequencer.hpp"

#include <cstdint>
#include <format>
#include <print>
#include <string>

using namespace tapeline;

int main(int argc, char** argv) {
  Args args(argc, argv);
  if (args.has("--help") || !args.has("--log")) {
    std::println("replayer --log path [--symbols 8] [--out path]");
    return args.has("--help") ? 0 : 1;
  }
  const std::string log = args.get("--log", "");
  const auto n_symbols = static_cast<std::uint32_t>(args.get_int("--symbols", 8));
  const std::string out = args.get("--out", "");

  auto records = Sequencer::load(log);
  if (!records) {
    std::println(stderr, "load: {}", records.error());
    return 1;
  }
  Replica standby(n_symbols);
  const Timestamp t0 = now_ns();
  const auto applied = standby.follow(*records);
  const Timestamp t1 = now_ns();
  std::string why;
  const bool ok = standby.engine().check_invariants(&why);
  const auto& st = standby.engine().stats();
  const std::string json = std::format(
      "{{\n  \"log\": \"{}\",\n  \"records\": {},\n  \"applied\": {},\n  \"last_seq\": {},\n  \"records_per_s\": {:.0f},\n"
      "  \"orders\": {},\n  \"fills\": {},\n  \"cancels\": {},\n  \"replaces\": {},\n  \"rejects\": {},\n"
      "  \"events\": {},\n  \"event_hash\": \"{:016x}\",\n  \"books_hash\": \"{:016x}\",\n  \"state_hash\": \"{:016x}\",\n"
      "  \"invariants_ok\": {}\n}}\n",
      log, records->size(), applied, standby.applied(),
      static_cast<double>(applied) * 1e9 / static_cast<double>(std::max<Timestamp>(t1 - t0, 1)), st.orders, st.fills,
      st.cancels, st.replaces, st.rejects, st.records, standby.event_hash(), standby.engine().books_hash(),
      standby.state_hash(), ok);
  if (!out.empty()) write_text(out, json);
  std::print("{}", json);
  return ok ? 0 : 2;
}
