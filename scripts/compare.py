#!/usr/bin/env python3
"""Cross-check the e2e run: exchange, feed handler, replayer and the Python
feed decoder must all agree on the final book. Writes results/e2e.json."""

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RES = os.path.join(ROOT, "results")


def load(name):
    with open(os.path.join(RES, name)) as f:
        return json.load(f)


def main():
    ex = load("exchange_state.json")
    fd = load("feed_state.json")
    cl = load("client.json")
    rp = load("replayer.json")

    tap = subprocess.run(
        [sys.executable, os.path.join(ROOT, "tools", "feedtap.py"), os.path.join(RES, "feed.cap"),
         "--symbols", str(ex["symbols"]), "--json"],
        capture_output=True, text=True, check=True)
    tap = json.loads(tap.stdout)

    checks = {
        "feed_handler_book_matches_exchange": fd["books_hash"] == ex["books_hash"],
        "python_decoder_book_matches_exchange": tap["books_hash"] == ex["books_hash"],
        "replayer_state_matches_exchange": rp["state_hash"] == ex["state_hash"],
        "replayer_books_match_exchange": rp["books_hash"] == ex["books_hash"],
        "feed_handler_consumed_through_end_of_session": fd["next_seq"] > ex["feed_end_of_session_seq"],
        "no_redundant_recovery_traffic": fd["duplicates"] == 0 and fd["recovered_packets"] == fd["retrans_requests"],
        "feed_handler_saw_end_of_session": bool(fd["end_of_session"]) and not fd["gap_outstanding"],
        "packets_were_actually_dropped": ex["feed_packets_dropped"] > 0,
        "every_gap_was_recovered": fd["gaps"] > 0 and fd["recovered_packets"] >= fd["gaps"],
        "client_saw_no_sequence_gaps": cl["seq_gaps"] == 0,
        "client_resumed_and_got_replay": bool(cl["resumed"]) and cl["replayed_on_resume"] > 0,
        "client_saw_end_of_session": bool(cl["end_of_session_seen"]),
        "exchange_closed_on_schedule_with_client_connected": ex["records"] >= ex["end_after_records"] > 0 and ex["sessions_connected_at_close"] == 1,
        "final_books_are_not_empty": sum(b["live_orders"] for b in ex["books"]) > 0,
        "final_books_span_every_symbol": all(b["live_orders"] > 0 for b in ex["books"]),
    }
    summary = {
        "checks": checks,
        "all_ok": all(checks.values()),
        "exchange": {k: ex[k] for k in ("elapsed_s", "records", "orders", "fills", "volume", "cancels", "replaces",
                                       "rejects", "feed_messages", "feed_packets_built", "feed_packets_sent",
                                       "feed_packets_dropped", "feed_bytes", "retrans_served", "snapshots_served",
                                       "inbound_processing_ns", "books_hash", "state_hash", "books")},
        "feed": {k: fd[k] for k in ("udp_packets", "tcp_packets", "messages", "gaps", "retrans_requests",
                                   "recovered_packets", "duplicates", "snapshots", "decode_errors",
                                   "unknown_orders", "packet_handling_ns", "books_hash")},
        "client": {k: cl[k] for k in ("sent_orders", "sent_cancels", "sent_replaces", "sent_total", "window",
                                     "msgs_per_s", "accepted", "executed", "canceled", "replaced", "rejected",
                                     "seq_gaps", "replayed_on_resume", "rtt_ns")},
        "replayer": {k: rp[k] for k in ("records", "records_per_s", "state_hash", "books_hash", "invariants_ok")},
        "feedtap": tap,
    }
    with open(os.path.join(RES, "e2e.json"), "w") as f:
        json.dump(summary, f, indent=2)
    for k, v in checks.items():
        print(("ok   " if v else "FAIL ") + k)
    print("all_ok" if summary["all_ok"] else "SOME CHECKS FAILED")
    return 0 if summary["all_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
