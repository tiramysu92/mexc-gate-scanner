#!/usr/bin/env python3
"""Check the complete package, then run offline suites outside the server CWD."""
import ast
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

SUITES=('test_public_flow.py','test_live_recovery.py','test_regularisation.py',
        'test_restart_v2411.py','test_session_v2412.py','test_reprise_v2412.py',
        'test_resync_v2413.py','test_ws_stability_v2413.py','test_restart_v2413.py',
        'test_quota_v2414.py','test_reprise_v2414.py')


def main():
    root=Path(__file__).resolve().parent
    try:
        names=set()
        for line in (root/'SHA256SUMS_V2414.txt').read_text().splitlines():
            expected,name=line.split('  ',1)
            if Path(name).name!=name or name in names:
                raise ValueError('Entrée de manifeste invalide')
            names.add(name);data=(root/name).read_bytes()
            if hashlib.sha256(data).hexdigest()!=expected:raise ValueError(name)
            if name.endswith('.py'):ast.parse(data,filename=name)
            print(name+': OK',flush=True)
        if not set(SUITES)<=names:raise ValueError('Suite absente du manifeste')
    except (OSError,ValueError,SyntaxError) as exc:
        print('Paquet V2.4.14 incomplet ou modifié :',str(exc),file=sys.stderr);return 1
    env={k:v for k,v in os.environ.items() if not k.startswith(('MEXC_','LIVE_'))}
    env.update(TRADING_MODE='shadow',LIVE_ARMED='0',PYTHONDONTWRITEBYTECODE='1')
    with tempfile.TemporaryDirectory(prefix='mexc_v2414_tests_') as test_root:
        for suite in SUITES:
            result=subprocess.run([sys.executable,str(root/suite)],cwd=test_root,env=env)
            if result.returncode:return result.returncode
    print('V2.4.14 : toutes les suites réussies. Aucun ordre réel envoyé ; aucun réarmement par les tests.')
    return 0


if __name__=='__main__':raise SystemExit(main())
