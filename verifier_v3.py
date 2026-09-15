#!/usr/bin/env python3
"""Self-contained offline verification; never starts app.py in LIVE mode."""
import hashlib
from pathlib import Path
import socket
import sys
import unittest


def main():
    root = Path(__file__).resolve().parent
    manifest = root/"SHA256SUMS.txt"
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            expected,name = line.split("  ",1)
            path = root/name
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise SystemExit("Intégrité incorrecte : "+name)
        print("Intégrité du paquet : OK",flush=True)
    # Any accidental real network operation makes the test fail.
    def blocked(*args,**kwargs):
        raise AssertionError("Network access is forbidden during V3 tests")
    socket.socket.connect = blocked
    socket.create_connection = blocked
    suite = unittest.defaultTestLoader.discover(str(root/"tests"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print(f"V3 : {result.testsRun} tests réussis. Aucun ordre réel, aucun réarmement.")


if __name__ == "__main__":main()
