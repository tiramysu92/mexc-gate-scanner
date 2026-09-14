#!/usr/bin/env python3
"""Validate the complete flat package in a temporary, isolated test directory."""
from pathlib import Path
import hashlib
import subprocess
import sys
import tempfile

SUITES=('test_public_flow.py','test_live_recovery.py','test_regularisation.py',
        'test_restart_v2411.py','test_session_v2412.py','test_reprise_v2412.py',
        'test_resync_v2413.py','test_ws_stability_v2413.py','test_restart_v2413.py')


def main():
    root=Path(__file__).resolve().parent
    try:
        for line in (root/'SHA256SUMS_V2413.txt').read_text().splitlines():
            expected,name=line.split('  ',1)
            if Path(name).name!=name or hashlib.sha256((root/name).read_bytes()).hexdigest()!=expected:
                raise ValueError(name)
    except (OSError,ValueError) as exc:
        print('Paquet V2.4.13 incomplet ou modifié :',str(exc),file=sys.stderr);return 1
    with tempfile.TemporaryDirectory(prefix='mexc_v2413_tests_') as test_root:
        for suite in SUITES:
            result=subprocess.run([sys.executable,str(root/suite)],cwd=test_root)
            if result.returncode:return result.returncode
    print('V2.4.13 : toutes les suites réussies. Aucun ordre réel envoyé ; aucun réarmement par les tests.')
    return 0


if __name__=='__main__':raise SystemExit(main())
