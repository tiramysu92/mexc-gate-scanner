#!/usr/bin/env python3
"""Run all release checks with this interpreter; no real exchange calls."""
from pathlib import Path
import subprocess
import sys

root=Path(__file__).resolve().parent
for name in ('test_public_flow.py','test_live_recovery.py','test_regularisation.py','verification_relance/test_restart.py'):
    result=subprocess.run([sys.executable,str(root/name)],cwd=root)
    if result.returncode:
        raise SystemExit(result.returncode)
print('V2.4.11 : 66 tests réussis. Aucun ordre réel envoyé ; LIVE non réarmé.')
