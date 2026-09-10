#!/usr/bin/env python3
"""Decode a feed capture written by build/feed --capture and rebuild the books.

The capture is a flat file of [u32 length][packet bytes] records holding every
packet the handler received, live UDP and TCP recovery alike, in arrival order.
This script speaks the same wire format as include/tapeline/wire.hpp, applies
the same gap and duplicate handling as FeedHandler, and hashes the rebuilt
books with the same FNV-1a scheme, so its books_hash can be compared directly
with the exchange's own state file. No dependencies beyond the standard library.
"""

import argparse
import json
import struct
import sys

MAGIC = 0x4C54
VERSION = 1
HEADER = struct.Struct("<HBBIQHH")  # magic, version, kind, session, first_seq, count, length
KIND_LIVE, KIND_RETRANS, KIND_SNAPSHOT = 1, 2, 3

PAYLOAD = {
    ord("S"): struct.Struct("<QB"),        # ts, code
    ord("A"): struct.Struct("<QQIBqI"),    # ts, oid, symbol, side, price, qty
    ord("E"): struct.Struct("<QQIQq"),     # ts, oid, qty, match_id, price
    ord("X"): struct.Struct("<QQI"),       # ts, oid, qty
    ord("D"): struct.Struct("<QQ"),        # ts, oid
    ord("H"): struct.Struct("<Q"),         # ts
}

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK = (1 << 64) - 1


def fnv1a(data, h=FNV_OFFSET):
    for b in data:
        h ^= b
        h = (h * FNV_PRIME) & MASK
    return h


class Book:
    """One symbol. Each side maps price -> list of [oid, qty] in arrival order."""

    def __init__(self, symbol):
        self.symbol = symbol
        self.bids = {}
        self.asks = {}
        self.where = {}  # oid -> (side, price)
        self.live = 0

    def add(self, oid, side, price, qty):
        book = self.bids if side == 1 else self.asks
        book.setdefault(price, []).append([oid, qty])
        self.where[oid] = (side, price)
        self.live += 1

    def _entry(self, oid):
        side, price = self.where[oid]
        book = self.bids if side == 1 else self.asks
        level = book[price]
        for e in level:
            if e[0] == oid:
                return book, price, level, e
        raise KeyError(oid)

    def reduce(self, oid, qty):
        book, price, level, e = self._entry(oid)
        if qty >= e[1]:
            level.remove(e)
            if not level:
                del book[price]
            del self.where[oid]
            self.live -= 1
        else:
            e[1] -= qty

    def delete(self, oid):
        book, price, level, e = self._entry(oid)
        level.remove(e)
        if not level:
            del book[price]
        del self.where[oid]
        self.live -= 1

    def resting(self):
        """Bids best to worst, then asks best to worst, FIFO within a level."""
        for price in sorted(self.bids, reverse=True):
            for oid, qty in self.bids[price]:
                yield 1, price, oid, qty
        for price in sorted(self.asks):
            for oid, qty in self.asks[price]:
                yield 2, price, oid, qty

    def hash(self):
        h = FNV_OFFSET
        for side, price, oid, qty in self.resting():
            h = fnv1a(struct.pack("<IBqQI", self.symbol, side, price, oid, qty), h)
        return h

    def best_bid(self):
        return max(self.bids) if self.bids else None

    def best_ask(self):
        return min(self.asks) if self.asks else None

    def depth(self, side, n):
        book = self.bids if side == 1 else self.asks
        prices = sorted(book, reverse=(side == 1))[:n]
        return [(p, sum(q for _, q in book[p]), len(book[p])) for p in prices]


class Handler:
    def __init__(self, symbols):
        self.symbols = symbols
        self.books = [Book(s) for s in range(symbols)]
        self.next_seq = 1
        self.pending = {}  # first_seq -> packet bytes
        self.in_snapshot = False
        self.stats = dict(packets=0, messages=0, applied=0, gaps=0, duplicates=0, buffered=0,
                          snapshots=0, retrans_packets=0, decode_errors=0, unknown_orders=0,
                          heartbeats=0, system_events=[])
        self.ended = False

    def on_packet(self, pkt):
        self.stats["packets"] += 1
        if len(pkt) < HEADER.size:
            self.stats["decode_errors"] += 1
            return
        magic, version, kind, session, first_seq, count, length = HEADER.unpack_from(pkt)
        if magic != MAGIC or version != VERSION or length > len(pkt):
            self.stats["decode_errors"] += 1
            return
        payload = pkt[HEADER.size:length]
        if kind == KIND_SNAPSHOT:
            self._snapshot(first_seq, count, payload)
            return
        if kind == KIND_RETRANS:
            self.stats["retrans_packets"] += 1
        if first_seq + count <= self.next_seq:
            self.stats["duplicates"] += 1
            return
        if first_seq > self.next_seq:
            if not self.pending:
                self.stats["gaps"] += 1
            if first_seq not in self.pending:
                self.pending[first_seq] = pkt[:length]
                self.stats["buffered"] += 1
            return
        self._apply_packet(first_seq, count, payload)
        self._drain()

    def _drain(self):
        while self.pending:
            first = min(self.pending)
            if first > self.next_seq:
                break
            pkt = self.pending.pop(first)
            _, _, _, _, first_seq, count, length = HEADER.unpack_from(pkt)
            if first_seq + count <= self.next_seq:
                self.stats["duplicates"] += 1
                continue
            self._apply_packet(first_seq, count, pkt[HEADER.size:length])

    def _messages(self, payload, count):
        pos = 0
        for _ in range(count):
            if pos + 2 > len(payload):
                self.stats["decode_errors"] += 1
                return
            mtype, mlen = payload[pos], payload[pos + 1]
            fmt = PAYLOAD.get(mtype)
            if fmt is None or fmt.size != mlen or pos + 2 + mlen > len(payload):
                self.stats["decode_errors"] += 1
                return
            yield mtype, fmt.unpack_from(payload, pos + 2)
            pos += 2 + mlen

    def _apply_packet(self, first_seq, count, payload):
        seq = first_seq
        for mtype, fields in self._messages(payload, count):
            self.stats["messages"] += 1
            if seq >= self.next_seq:
                self._apply(mtype, fields)
                self.next_seq = seq + 1
            else:
                self.stats["duplicates"] += 1
            seq += 1

    def _snapshot(self, first_seq, count, payload):
        if not self.in_snapshot:
            self.in_snapshot = True
            self.stats["snapshots"] += 1
            self.books = [Book(s) for s in range(self.symbols)]
        for mtype, fields in self._messages(payload, count):
            self.stats["messages"] += 1
            if mtype == ord("S") and fields[1] == ord("Q"):
                self.in_snapshot = False
                self.next_seq = first_seq
                for first in [f for f in self.pending]:
                    _, _, _, _, fs, c, _ = HEADER.unpack_from(self.pending[first])
                    if fs + c <= self.next_seq:
                        del self.pending[first]
                self._drain()
                return
            self._apply(mtype, fields)

    def _apply(self, mtype, f):
        self.stats["applied"] += 1
        try:
            if mtype == ord("A"):
                _, oid, symbol, side, price, qty = f
                if symbol < self.symbols:
                    self.books[symbol].add(oid, side, price, qty)
            elif mtype == ord("E"):
                _, oid, qty, _, _ = f
                self._book_of(oid).reduce(oid, qty)
            elif mtype == ord("X"):
                _, oid, qty = f
                self._book_of(oid).reduce(oid, qty)
            elif mtype == ord("D"):
                _, oid = f
                self._book_of(oid).delete(oid)
            elif mtype == ord("S"):
                code = chr(f[1])
                self.stats["system_events"].append(code)
                if code == "C":
                    self.ended = True
            elif mtype == ord("H"):
                self.stats["heartbeats"] += 1
        except KeyError:
            self.stats["unknown_orders"] += 1

    def _book_of(self, oid):
        for b in self.books:
            if oid in b.where:
                return b
        raise KeyError(oid)

    def books_hash(self):
        h = FNV_OFFSET
        for b in self.books:
            h = fnv1a(struct.pack("<Q", b.hash()), h)
        return h


def read_capture(path):
    with open(path, "rb") as f:
        data = f.read()
    pos = 0
    while pos + 4 <= len(data):
        (n,) = struct.unpack_from("<I", data, pos)
        pos += 4
        yield data[pos:pos + n]
        pos += n


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("capture")
    ap.add_argument("--symbols", type=int, default=8)
    ap.add_argument("--depth", type=int, default=3, help="levels per side to print")
    ap.add_argument("--json", action="store_true", help="print a machine-readable summary only")
    args = ap.parse_args(argv)

    h = Handler(args.symbols)
    for pkt in read_capture(args.capture):
        h.on_packet(pkt)

    summary = {
        **{k: v for k, v in h.stats.items() if k != "system_events"},
        "system_events": "".join(h.stats["system_events"]),
        "next_seq": h.next_seq,
        "gap_outstanding": bool(h.pending),
        "end_of_session": h.ended,
        "books_hash": "%016x" % h.books_hash(),
        "books": [
            {"symbol": b.symbol, "live_orders": b.live, "best_bid": b.best_bid(), "best_ask": b.best_ask(),
             "hash": "%016x" % b.hash()}
            for b in h.books
        ],
    }
    if args.json:
        print(json.dumps(summary))
        return 0

    print(f"packets {summary['packets']}  messages {summary['messages']}  gaps {summary['gaps']}  "
          f"retrans packets {summary['retrans_packets']}  duplicates {summary['duplicates']}  "
          f"snapshots {summary['snapshots']}  decode errors {summary['decode_errors']}")
    print(f"next seq {summary['next_seq']}  end of session {summary['end_of_session']}  "
          f"gap outstanding {summary['gap_outstanding']}")
    print(f"books hash {summary['books_hash']}")
    for b in h.books:
        bids = b.depth(1, args.depth)
        asks = b.depth(2, args.depth)
        print(f"symbol {b.symbol}: {b.live} resting, hash {b.hash():016x}")
        for i in range(max(len(bids), len(asks))):
            bid = f"{bids[i][1]:>7} @ {bids[i][0]:<6} ({bids[i][2]})" if i < len(bids) else " " * 22
            ask = f"{asks[i][1]:>7} @ {asks[i][0]:<6} ({asks[i][2]})" if i < len(asks) else ""
            print(f"    {bid}   |   {ask}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
