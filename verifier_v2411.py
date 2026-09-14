#!/usr/bin/env python3
"""Run all release checks with this interpreter; no real exchange calls."""
from pathlib import Path
import subprocess
import sys
import tempfile

SUITES=('test_public_flow.py','test_live_recovery.py','test_regularisation.py',
        'test_restart_v2411.py')
REQUIRED=SUITES+('app.py','regulariser_cto.py','relancer_mexc_v2411.py',
                 'collecte_incident_cto.py','cto_journal_fixture_v2411.json')


def main():
    root=Path(__file__).resolve().parent
    missing=[name for name in REQUIRED if not (root/name).is_file()]
    if missing:
        print('Installation V2.4.11 incomplète. Fichiers manquants :',file=sys.stderr)
        for name in missing:print('  '+name,file=sys.stderr)
        print('Restaurer les fichiers et leurs sous-dossiers avant de relancer les tests.',file=sys.stderr)
        return 1
    # Relative journal paths and the process guard must refer to this isolated
    # test directory, even while the actual scanner is running in the project.
    with tempfile.TemporaryDirectory(prefix='mexc_v2411_tests_') as test_root:
        for name in SUITES:
            result=subprocess.run([sys.executable,str(root/name)],cwd=test_root)
            if result.returncode:return result.returncode
    print('V2.4.11 : 66 tests réussis. Aucun ordre réel envoyé ; LIVE non réarmé.')
    return 0


if __name__=='__main__':raise SystemExit(main())
