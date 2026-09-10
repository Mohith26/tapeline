# Results

Everything below is copied from the JSON files in `results/`, which the
programs write themselves. Machine: Apple Silicon laptop (arm64), macOS 26.4,
Apple clang 21 with libc++, `-O2`, nothing pinned or isolated, other things
running. Run-to-run variation on the in-process numbers is roughly 20 percent;
the matching benchmark came in between 9.4M and 14M records/s across the runs
I did while working on it, and the file holds the last one.

## Timer caveat

`std::chrono::steady_clock` on this machine advances in 41.67 ns steps. Timing
an empty region gives p50 = 0 ns and p90 = 42 ns. Any per-operation histogram
below is quantised to that grid, so "p50 42 ns" means one tick or less and
"p50 0 ns" means the operation usually finished inside a tick. The `mean`
columns labelled "batch" divide total elapsed time by the number of operations
and do not have that problem.

## In-process benchmarks (`results/bench.json`, `make bench`)

### Order book, 1,000,000 resting orders, 200 price levels a side

| operation | batch mean | throughput | p50 | p90 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rest` (add, non-crossing) | 35.7 ns | 28.0M/s | 0 | 42 | 42 | 916 | 29,500 |
| `remove` (cancel, random order) | 131.1 ns | 7.6M/s | 125 | 167 | 250 | 1,084 | 60,041 |

Invariants checked at the end: pass, zero orders left.

Cancels cost more than adds because the slots are visited in shuffled order,
so nearly every one is a cache miss, plus a binary search for the level.

### Matching engine, 1,000,001 sequenced records, one symbol

Workload: 70 percent new limit orders (one in seven IOC) priced uniformly in
a 21-tick band so most of them cross, 20 percent cancels and 10 percent
replaces, all aimed at orders that are actually resting (the generator runs a
shadow engine so there are no spurious rejects). Timing covers
`Sequencer::append` plus `Engine::apply` plus fan-out of every event to a
counting sink.

| | |
| --- | ---: |
| orders / fills / cancels / replaces / rejects | 704,027 / 424,974 / 274,768 / 98,278 / 0 |
| events emitted | 2,024,322 |
| throughput | 13.95M records/s |
| batch mean per record | 71.7 ns |
| p50 / p90 / p99 / p99.9 / max | 42 / 83 / 166 / 1,625 / 36,916 ns |

The p99.9 tail is level creation far from the touch (a `flat_map` insert that
shifts the ladder) and hash-map growth in the session's live-order index; the
max is a one-off, most likely a page fault. Before I reserved the sequencer's
log vector the max was 12.8 ms: a vector doubling inside the timed region.

### Replay

Feeding the same 1,000,001 records to a fresh `Replica`: 5.66M records/s,
state hash equal to the primary's.

### Codecs, 1,000,000 messages each

| | encode | decode | decode rate | bytes/msg |
| --- | ---: | ---: | ---: | ---: |
| feed (`A`/`E`/`D` mix) | 7.40 ns | 4.11 ns | 243M msgs/s | 30.3 |
| gateway out (`S`/`E` mix) | 15.61 ns | 4.24 ns | | 52.0 |

### Feed pipeline, in process

Engine to publisher to packets to handler with no sockets: 1,243,222 messages
in 29,162 packets (42.6 messages per packet, 40.3 MB). The handler rebuilt the
book at 29.6M messages/s, 33.7 ns per message, and its book hash matched the
engine's.

## End to end over loopback (`results/e2e_unicast.json`, `make e2e`)

Exchange with 8 symbols and `--drop-every 50`, a feed handler writing a
capture, a client with 32 messages in flight that drops its connection after
100k orders and resumes, the venue closing on schedule at 200k records with
the client still connected, then the replayer and the Python decoder. All 15
checks in `scripts/compare.py` passed.

### Exchange

| | |
| --- | ---: |
| records sequenced | 200,010 |
| orders / fills / volume | 140,027 / 79,010 / 2,046,363 |
| cancels / replaces / rejects | 58,572 / 14,889 / 273 |
| feed messages / packets built | 247,541 / 38,269 |
| feed packets deliberately dropped | 765 |
| retransmissions served / snapshots served | 765 / 0 |
| feed bytes | 8,603,023 |
| resting orders at close | 1,619 across 8 symbols |
| inbound processing per record (sequence + match + fan-out), mean / p50 / p90 / p99 / p99.9 | 422 / 208 / 459 / 4,625 / 13,000 ns |

The inbound figure is higher than the in-process benchmark because the fan-out
here encodes gateway frames and feed messages instead of counting events, and
the ring buffer and per-session logs are being appended to.

### Client

| | |
| --- | ---: |
| messages sent (orders / cancels / replaces) | 200,038 (140,053 / 45,021 / 14,964) |
| rate | 345,299 messages/s |
| replies: accepted / executed / canceled / replaced / rejected | 140,027 / 158,020 / 58,572 / 14,889 / 273 |
| sequence gaps seen | 0 |
| frames replayed after resume | 2,802 |
| round trip, send to first reply (199,999 samples): min / mean / p50 / p90 / p99 / p99.9 / max | 8.3 / 53.9 / 50.4 / 70.0 / 196.3 / 363.2 / 1,691.8 us |

The 2,802 replayed frames are the cancel-on-disconnect notices for everything
the client had resting when it dropped, which is exactly what a client should
learn on reconnect.

### Feed handler

| | |
| --- | ---: |
| UDP packets / TCP recovery packets | 37,474 / 765 |
| messages applied | 247,511 |
| gaps detected / requests sent / packets recovered | 765 / 765 / 765 |
| duplicates / unknown orders / decode errors | 0 / 0 / 0 |
| per-packet handling, mean / p50 / p99 / max | 903 / 583 / 6,334 / 84,667 ns |
| book hash | `00d0a674dae14128` (exchange: `00d0a674dae14128`) |

### Replayer and Python decoder

The replayer rebuilt the engine from the 200,010-record log at 5.19M
records/s with the exchange's exact state hash. `tools/feedtap.py` read the
38,239-packet capture, saw the same 765 gaps and 765 retransmissions, zero
duplicates, and produced book hash `00d0a674dae14128`.

## Multicast (`results/e2e_multicast.json`)

Same run with `FEED_ADDR=239.7.7.7`: all 15 checks passed, 686 packets dropped
and 686 recovered, hashes `c3c74e0497725407` on all three sides. The client's
median round trip was 144 us against 50 us over plain loopback and its send
rate 176k messages/s against 345k. The exchange thread is the same in both
runs, so the difference is the cost of the multicast send path on this
kernel; I have not dug further.

## Tests (`results/tests.json`)

43 C++ tests, 100,257 checks, 0 failures, 33 ms. The largest is a 20-seed
differential fuzz that drives `Book` and a deliberately naive reference book
(a flat vector, matching by linear scan) with the same 4,000-operation streams
and requires identical fill sequences and identical final resting state. The
feed tests deliver packets in order, with every seventh dropped, in swapped
pairs with each sent twice, and to a handler that joined so late the ring had
moved on and only a snapshot could help. Ten Python tests cover the decoder
including a gap, an overlapping retransmission and a snapshot.

## Bugs the tests and the e2e run found

- `CHECK_EQ` in my first test harness evaluated its arguments twice, so a
  test that called `reduce()` inside the macro reduced the order twice. The
  book was right and the test was wrong.
- The feed handler sent two recovery requests per gap over real sockets and
  one in the unit tests. The main loop passed a timestamp taken before the
  packets it had just processed, the subtraction went negative in unsigned
  arithmetic, and every request looked overdue. Fixed in the handler so a
  stale clock counts as "just requested", and the compare script now fails
  if any recovery reply is a duplicate.
- `EndOfSession` was given a per-session sequence number by the gateway but
  a zero-byte payload on the wire, so the client saw sequence 0 and reported
  a gap on the very last frame. The frame now carries its number.
- The first e2e run compared final books that were empty, because the
  client's logout cancelled everything and the hashes matched trivially. The
  venue now closes on schedule while the client is connected, leaving a
  1,619-order book for the three views to disagree about, which they do not.
