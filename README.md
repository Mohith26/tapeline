# tapeline

A small exchange core in C++23: a price-time matching engine, a sequenced
binary market-data feed over UDP with gap recovery over TCP, an order-entry
gateway with session resume, and a hot standby that rebuilds the engine from
the sequencer's log. There is also a Python decoder that reads a feed capture
and proves it arrived at the same book as the exchange.

I wrote it to understand how the pieces of a trading venue actually fit
together, not to be fast in the abstract. The numbers in [RESULTS.md](RESULTS.md)
are what it does on my laptop over loopback; every one of them is written by
the programs in `src/` into `results/`.

## How a message moves through it

```
 client ──TCP──▶ gateway ──▶ sequencer ──▶ engine ──▶ feed publisher ──UDP──▶ feed handler
   ▲                            │              │            │                      │
   └────── sequenced replies ───┘              │            └── ring + snapshot ──TCP──┘ (gaps)
                                               ▼
                                      append-only log ──▶ replayer / standby
```

1. The **gateway** frames bytes off each TCP session and decodes them into
   `NewOrder`, `CancelOrder`, `ReplaceOrder` or session events.
2. The **sequencer** stamps each record with the next sequence number and a
   timestamp and appends it to an in-memory log (and a file, if asked). This
   is the only place time or ordering enters the system.
3. The **engine** consumes records in sequence and emits events (`Accepted`,
   `Rested`, `Executed`, `Canceled`, `Replaced`, `Rejected`). It never reads a
   clock or a socket, so two engines fed the same log produce the same events
   and the same book. `Replica` in `replica.hpp` is exactly that: an engine
   following the log, comparable to the primary by state hash at any point.
4. The **feed publisher** turns events into ITCH-style messages, packs them
   into MoldUDP-style packets with one session-wide sequence number, keeps a
   ring of everything it has sent, and can serialise the whole resting book as
   a snapshot.
5. The **feed handler** rebuilds every book from the packets. Out-of-order
   packets are held, a missing range is requested over TCP, and if the
   publisher's ring no longer has it the reply is a snapshot instead. The
   handler uses the same `Book` class as the engine, so the two hash the same.
6. The gateway assigns every reply a per-session sequence number and keeps
   the frames, so a client that reconnects with the last number it saw gets
   the rest replayed. Disconnecting cancels the session's resting orders.

Everything on the exchange side runs on one thread. The sequencer is the
serialisation point, so there is nothing to lock, and a `poll` loop over the
listening sockets, the sessions and the recovery connections is the whole
event loop.

## Wire formats

All three protocols are little-endian with explicit layouts, written and read
through `memcpy` in `wire.hpp` (no packed structs, no host-order assumptions).

| Protocol | Framing | Messages |
| --- | --- | --- |
| feed (UDP, or TCP for recovery) | 20-byte packet header: magic `TL`, version, kind (live / retrans / snapshot), session, first sequence number, message count, length | `S` system event, `A` add order, `E` order executed, `X` partial cancel, `D` delete, `H` heartbeat; each is `type, length, payload` |
| gateway (TCP) | `u16 length, u8 type, payload` | in: `L` login, `O` enter order, `X` cancel, `U` replace, `R` heartbeat, `Z` logout; out: `A` login accepted, `J` login rejected, `S` accepted, `E` executed, `C` canceled, `R` replaced, `N` rejected, `H` heartbeat, `Z` end of session |
| recovery (TCP) | 17-byte request | `R` retransmit `[start, start + count)`, `S` snapshot; replies are ordinary feed packets |

Prices are integer ticks (`int64_t`), quantities `uint32_t`. No floating point
is involved anywhere in matching, the feed or the fills.

Feed messages describe resting orders only. An aggressor's fills are implied by
the `E` messages against the orders it hit; if anything is left of it, an `A`
follows. A replace that only shrinks quantity keeps queue priority and shows up
as an `X`; any other replace is a `D` for the old id followed by whatever the
new order does.

## Building and running it

You need a compiler with `std::print`, `std::expected` and `std::flat_map`
(Apple clang 21 with its libc++ is what I used; GCC 15 or Clang 20 with a
matching standard library should work) and Python 3 for the tools.

```
make            # build/exchange, client, feed, bench, replayer, tests
make test       # 43 C++ tests, ~100k checks, including a 20-seed differential fuzz
make bench      # in-process numbers -> results/bench.json
make e2e        # the real thing over loopback -> results/e2e.json
python3 -m unittest tests/test_feedtap.py
```

`make e2e` starts the exchange with `--drop-every 50` (one feed packet in
fifty is built, logged, and deliberately not sent), starts a feed handler with
a capture file, runs a client that sends 200k messages with 32 in flight and
drops its connection halfway to exercise resume, lets the venue close on
schedule with the client still connected, replays the durable log, decodes the
capture in Python, and checks that all of these agree:

```
ok   feed_handler_book_matches_exchange
ok   python_decoder_book_matches_exchange
ok   replayer_state_matches_exchange
ok   every_gap_was_recovered
ok   client_saw_no_sequence_gaps
ok   client_resumed_and_got_replay
...
all_ok
```

`FEED_ADDR=239.7.7.7 make e2e` runs the same thing over a multicast group.

By hand:

```
./build/exchange --symbols 8 --drop-every 50 --log results/exchange.log --state-out results/exchange_state.json
./build/feed --symbols 8 --capture results/feed.cap --out results/feed_state.json
./build/client --orders 200000 --window 32 --resume-at 100000
./build/replayer --log results/exchange.log --symbols 8
python3 tools/feedtap.py results/feed.cap --symbols 8 --depth 5
```

`feedtap.py` prints gaps, retransmissions, duplicates, the rebuilt top of book
per symbol and the same FNV-1a book hash the C++ side prints, so the two can be
compared by eye or by `scripts/compare.py`.

## What I measured

Short version, from `results/` (see [RESULTS.md](RESULTS.md) for the full
tables and the caveats):

- Matching engine, in process: 1,000,001 records (704k orders, 425k fills,
  275k cancels, 98k replaces) at 13.9M records/s, 72 ns mean per record
  including event fan-out; p99 166 ns, p99.9 1.6 us.
- Book alone: 36 ns per add, 131 ns per cancel at one million resting orders
  across 200 levels a side.
- Feed codec: 7.4 ns to encode and 4.1 ns to decode a message; the handler
  rebuilds a book at 29.6M messages/s from packets averaging 43 messages.
- Over loopback with everything running: 200k order-entry messages at 345k
  messages/s with 32 in flight, 50 us median round trip from the client's
  send to the exchange's first reply, p99 196 us; 765 feed packets dropped on
  purpose, 765 recovered, zero duplicates, zero unknown orders, and the feed
  handler, the Python decoder and the log replayer all landed on the same
  1,619-order book as the exchange.
- Replaying the 200k-record durable log rebuilds the exchange's exact state
  at 5.2M records/s.

## Decisions I made on purpose

- **Integer prices, no floats.** Ticks are `int64_t`. The venue decides what a
  tick is; the engine does not care.
- **Slab plus intrusive lists.** Orders live in a vector with a free list and
  each price level is a doubly linked FIFO of slot indices, so after warm-up
  the book allocates nothing per order and a cancel is O(1) once the slot is
  known.
- **`std::flat_map` for the ladder.** Bids ascending and asks descending, so
  the best level is always the last element: taking it off is a pop from the
  back and inserting near the top moves almost nothing. This is the trade I
  wanted for a book where most activity is near the touch; a deep ladder with
  frequent inserts far from the touch would prefer a tree.
- **The engine is a pure function of the log.** Timestamps come from the
  sequencer record, ids from the engine's own counters, and cancel-on-
  disconnect walks orders in id order so the standby emits identical events.
  `EventHasher` folds every event into one number and the tests require the
  primary and the replica to agree on it, not just on the book.
- **Feed handler and engine share `Book`.** The handler is not a second
  implementation of the book that could drift; if the feed is complete and
  correct, the hashes match by construction.
- **One outstanding recovery request at a time**, re-issued on a timer. The
  first version sent one request per arriving packet during a gap, which is
  the kind of storm a real venue rate-limits you for.
- **Snapshots are just feed packets.** A snapshot is a run of `A` messages
  plus a `Q` system event, tagged with the live sequence number to resume
  from, so the handler needs no second parser.

## What is missing

- No self-trade prevention, no auctions, no halts, no order types beyond
  limit and market with day or IOC. Replace assumes a day order.
- The durable log writes `Inbound` records in the host's memory layout. It is
  fine for a standby on the same platform, which is what it is for, and it is
  not an interchange format. The feed and gateway formats are portable.
- Gateway replay logs live in memory for the life of the process; a restart
  loses them. Real venues persist these.
- The clock on the machine I measured on ticks every 41.67 ns, so single-
  operation histograms are quantised (a p50 of 42 ns means "one tick or
  less"). Mean figures are derived from batch timings and are the ones to
  read; the histograms are there for the tails.
- I have only run this on macOS. The socket code is plain POSIX and I avoided
  anything Darwin-specific, but Linux is untested by me.
- The `test-asan` target exists but I could not get sanitizer builds to run
  on this machine (the system kills the instrumented binary), so treat it as
  a target I wrote and did not exercise.

## Layout

```
include/tapeline/
  types.hpp      ids, enums, the Inbound log record and the Event struct
  hash.hpp       FNV-1a used for state fingerprints
  book.hpp       the order book
  engine.hpp     matching engine and session state
  wire.hpp       the three wire formats
  feed.hpp       FeedPublisher and FeedHandler
  gateway.hpp    sessions, sequenced replies, resume
  sequencer.hpp  the log
  replica.hpp    EventHasher and Replica
  net.hpp        POSIX socket wrappers
  clock.hpp      timing and a percentile histogram
src/             exchange, client, feed, bench, replayer
tests/           C++ tests (custom registry, no dependencies) and test_feedtap.py
tools/feedtap.py capture decoder and book rebuilder
scripts/         e2e.sh and compare.py
results/         everything the programs measured
```

MIT licensed.
