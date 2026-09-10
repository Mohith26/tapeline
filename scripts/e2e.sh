#!/usr/bin/env bash
# End-to-end run over real loopback sockets:
#   exchange  (TCP order entry + UDP feed + TCP recovery, drops every Nth feed packet on purpose)
#   feed      (rebuilds the books from UDP, recovers the gaps over TCP, writes a capture)
#   client    (sends orders, drops its connection halfway and resumes the session, stays until the close)
#   replayer  (rebuilds the engine from the durable log after the fact)
# then scripts/compare.py checks that all three views of the book agree.
set -euo pipefail
cd "$(dirname "$0")/.."

GW=${GW:-9101}
FEED_PORT=${FEED_PORT:-9102}
RT=${RT:-9103}
SYMBOLS=${SYMBOLS:-8}
DROP=${DROP:-50}
RECORDS=${RECORDS:-200000}
WINDOW=${WINDOW:-32}
FEED_ADDR=${FEED_ADDR:-127.0.0.1}

mkdir -p results
rm -f results/exchange_state.json results/feed_state.json results/client.json results/replayer.json results/feed.cap results/exchange.log

./build/exchange --gw-port "$GW" --feed-addr "$FEED_ADDR" --feed-port "$FEED_PORT" --retrans-port "$RT" \
  --symbols "$SYMBOLS" --drop-every "$DROP" --log results/exchange.log \
  --state-out results/exchange_state.json --end-after-records "$RECORDS" --idle-exit-ms 1500 --quiet &
EX=$!
sleep 0.3
./build/feed --addr "$FEED_ADDR" --port "$FEED_PORT" --retrans-port "$RT" --symbols "$SYMBOLS" \
  --out results/feed_state.json --capture results/feed.cap --quiet &
FD=$!
sleep 0.2
# The client asks for more than the venue will process; the venue closes on
# schedule while the client is still connected and the client stops on the
# EndOfSession frame.
./build/client --port "$GW" --orders $((RECORDS * 2)) --symbols "$SYMBOLS" --window "$WINDOW" \
  --resume-at $((RECORDS / 2)) --out results/client.json > /dev/null
wait "$EX"
wait "$FD"
./build/replayer --log results/exchange.log --symbols "$SYMBOLS" --out results/replayer.json > /dev/null
python3 scripts/compare.py
