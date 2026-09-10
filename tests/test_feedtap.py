"""Unit tests for tools/feedtap.py. Run with: python3 -m unittest tests/test_feedtap.py"""

import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))

import feedtap  # noqa: E402


def msg(mtype, *fields):
    fmt = feedtap.PAYLOAD[ord(mtype)]
    return struct.pack("<BB", ord(mtype), fmt.size) + fmt.pack(*fields)


def packet(first_seq, msgs, kind=feedtap.KIND_LIVE, session=7):
    body = b"".join(msgs)
    header = feedtap.HEADER.pack(feedtap.MAGIC, feedtap.VERSION, kind, session, first_seq, len(msgs),
                                 feedtap.HEADER.size + len(body))
    return header + body


class Fnv(unittest.TestCase):
    def test_known_vectors(self):
        self.assertEqual(feedtap.fnv1a(b""), 0xCBF29CE484222325)
        self.assertEqual(feedtap.fnv1a(b"a"), 0xAF63DC4C8601EC8C)
        self.assertEqual(feedtap.fnv1a(b"foobar"), 0x85944171F73967E8)


class BookTests(unittest.TestCase):
    def test_price_time_order_and_hash_layout(self):
        b = feedtap.Book(3)
        b.add(11, 1, 100, 5)
        b.add(12, 1, 101, 7)
        b.add(13, 1, 101, 1)
        b.add(14, 2, 105, 2)
        self.assertEqual(list(b.resting()), [(1, 101, 12, 7), (1, 101, 13, 1), (1, 100, 11, 5), (2, 105, 14, 2)])
        h = feedtap.FNV_OFFSET
        for side, price, oid, qty in b.resting():
            h = feedtap.fnv1a(struct.pack("<IBqQI", 3, side, price, oid, qty), h)
        self.assertEqual(b.hash(), h)
        self.assertEqual(b.best_bid(), 101)
        self.assertEqual(b.best_ask(), 105)

    def test_reduce_and_delete(self):
        b = feedtap.Book(0)
        b.add(1, 2, 50, 10)
        b.add(2, 2, 50, 10)
        b.reduce(1, 4)
        self.assertEqual(list(b.resting()), [(2, 50, 1, 6), (2, 50, 2, 10)])
        b.reduce(1, 6)  # fully consumed: leaves the queue
        self.assertEqual(list(b.resting()), [(2, 50, 2, 10)])
        b.delete(2)
        self.assertEqual(list(b.resting()), [])
        self.assertEqual(b.live, 0)
        self.assertEqual(b.asks, {})


class HandlerTests(unittest.TestCase):
    def test_in_order(self):
        h = feedtap.Handler(2)
        h.on_packet(packet(1, [msg("S", 1, ord("O")), msg("A", 2, 1, 0, 1, 100, 5), msg("A", 3, 2, 1, 2, 200, 9)]))
        h.on_packet(packet(4, [msg("E", 4, 1, 3, 1, 100), msg("X", 5, 2, 4), msg("S", 6, ord("C"))]))
        self.assertEqual(h.next_seq, 7)
        self.assertTrue(h.ended)
        self.assertEqual(list(h.books[0].resting()), [(1, 100, 1, 2)])
        self.assertEqual(list(h.books[1].resting()), [(2, 200, 2, 5)])
        self.assertEqual(h.stats["gaps"], 0)
        self.assertEqual(h.stats["system_events"], ["O", "C"])

    def test_gap_then_retransmission_then_duplicate(self):
        h = feedtap.Handler(1)
        p1 = packet(1, [msg("A", 1, 1, 0, 1, 100, 5)])
        p2 = packet(2, [msg("A", 2, 2, 0, 2, 101, 5)])
        p3 = packet(3, [msg("E", 3, 2, 5, 1, 101)])
        h.on_packet(p1)
        h.on_packet(p3)  # p2 lost: must be buffered, not applied
        self.assertEqual(h.next_seq, 2)
        self.assertEqual(h.stats["gaps"], 1)
        self.assertEqual(h.stats["buffered"], 1)
        self.assertEqual(h.books[0].live, 1)
        h.on_packet(packet(2, [msg("A", 2, 2, 0, 2, 101, 5)], kind=feedtap.KIND_RETRANS))
        self.assertEqual(h.next_seq, 4)
        self.assertFalse(h.pending)
        self.assertEqual(h.stats["retrans_packets"], 1)
        self.assertEqual(list(h.books[0].resting()), [(1, 100, 1, 5)])  # order 2 filled by seq 3
        h.on_packet(p2)
        self.assertEqual(h.stats["duplicates"], 1)
        self.assertEqual(h.next_seq, 4)

    def test_overlapping_packet_applies_only_new_messages(self):
        h = feedtap.Handler(1)
        h.on_packet(packet(1, [msg("A", 1, 1, 0, 1, 100, 5), msg("A", 2, 2, 0, 1, 100, 5)]))
        # A retransmission that starts before what we have and extends past it.
        h.on_packet(packet(2, [msg("A", 2, 2, 0, 1, 100, 5), msg("A", 3, 3, 0, 2, 105, 1)], kind=feedtap.KIND_RETRANS))
        self.assertEqual(h.next_seq, 4)
        self.assertEqual(h.books[0].live, 3)
        self.assertEqual(h.stats["duplicates"], 1)

    def test_snapshot_resets_state(self):
        h = feedtap.Handler(1)
        h.on_packet(packet(1, [msg("A", 1, 1, 0, 1, 100, 5)]))
        h.on_packet(packet(50, [msg("A", 50, 9, 0, 2, 120, 1)]))  # far ahead: buffered
        self.assertEqual(h.stats["gaps"], 1)
        snap = packet(50, [msg("A", 0, 7, 0, 1, 99, 3), msg("A", 0, 8, 0, 2, 110, 4), msg("S", 0, ord("Q"))],
                      kind=feedtap.KIND_SNAPSHOT)
        h.on_packet(snap)
        self.assertEqual(h.stats["snapshots"], 1)
        # Snapshot state replaced the old book, then the buffered packet drained.
        self.assertEqual(h.next_seq, 51)
        self.assertFalse(h.pending)
        self.assertEqual(list(h.books[0].resting()), [(1, 99, 7, 3), (2, 110, 8, 4), (2, 120, 9, 1)])

    def test_bad_packets_are_counted_not_fatal(self):
        h = feedtap.Handler(1)
        h.on_packet(b"\x00" * 10)
        h.on_packet(b"XX" + b"\x00" * 30)
        self.assertEqual(h.stats["decode_errors"], 2)
        self.assertEqual(h.next_seq, 1)

    def test_books_hash_matches_manual_fold(self):
        h = feedtap.Handler(2)
        h.on_packet(packet(1, [msg("A", 1, 1, 1, 2, 300, 8)]))
        expected = feedtap.FNV_OFFSET
        for b in h.books:
            expected = feedtap.fnv1a(struct.pack("<Q", b.hash()), expected)
        self.assertEqual(h.books_hash(), expected)


class CaptureFormat(unittest.TestCase):
    def test_read_capture_roundtrip(self):
        import tempfile
        p1 = packet(1, [msg("H", 5)])
        p2 = packet(2, [msg("H", 6)])
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(struct.pack("<I", len(p1)) + p1 + struct.pack("<I", len(p2)) + p2)
            path = f.name
        try:
            self.assertEqual(list(feedtap.read_capture(path)), [p1, p2])
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
