#!/usr/bin/env python3
"""Focused stdlib checks for the mixed workload runner."""
import json
import importlib.util
from pathlib import Path
import subprocess
import sys
import socket
import tempfile
import unittest

RUNNER = Path(__file__).with_name("bench-mixed-workload.py")
SPEC = importlib.util.spec_from_file_location("mixed_runner", RUNNER)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
try:
    with socket.socket(socket.AF_INET):
        HAS_SOCKETS = True
except PermissionError:
    HAS_SOCKETS = False

class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps({"seed": 1, "connections": 1, "pipeline": 1,
                                              "rate": 1, "server_workers": 1,
                                              "warmup_ms": 0, "steady_ms": 1, "drain_ms": 1}))
        self.server = self.root / "server.py"
        self.server.write_text("""#!/usr/bin/env python3
import signal, socket, sys, time
address=sys.argv[sys.argv.index('--listen')+1]
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', int(address.rsplit(':', 1)[1]))); s.listen()
print('listening on '+address, flush=True)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
while True: time.sleep(1)
""")
        self.client = self.root / "client.py"
        self.client.write_text("""#!/usr/bin/env python3
import json, pathlib, sys
path=pathlib.Path(sys.argv[sys.argv.index('--output')+1])
path.write_text(json.dumps({'valid': True}))
""")
        self.server.chmod(0o755); self.client.chmod(0o755)

    def tearDown(self):
        self.tmp.cleanup()

    def run_runner(self, *extra):
        return subprocess.run([sys.executable, str(RUNNER), '--manifest', str(self.manifest),
                               '--server', str(self.server), '--client', str(self.client),
                               '--output-dir', str(self.root / 'logs'), *extra],
                              text=True, capture_output=True, check=False)

    @unittest.skipUnless(HAS_SOCKETS, "sandbox does not permit socket lifecycle tests")
    def test_result_path_and_cleanup(self):
        result = self.run_runner()
        self.assertEqual(result.returncode, 0, result.stderr)
        run = next((self.root / 'logs').glob('*')) / 'run-1'
        self.assertEqual(json.loads((run / 'result.json').read_text()), {'valid': True})

    def test_worker_mismatch_rejected(self):
        result = self.run_runner('--workers', '2')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('server_workers', result.stderr)

    @unittest.skipUnless(HAS_SOCKETS, "sandbox does not permit socket lifecycle tests")
    def test_invalid_client_result_is_retained_and_rejected(self):
        self.client.write_text(self.client.read_text().replace("'valid': True", "'valid': False"))
        result = self.run_runner()
        self.assertNotEqual(result.returncode, 0)
        run = next((self.root / 'logs').glob('*')) / 'run-1'
        self.assertFalse(json.loads((run / 'result.json').read_text())['valid'])
        self.assertTrue((run / 'invalid.json').is_file())

    def test_timeout_budget_includes_phases_and_drain(self):
        self.assertEqual(MODULE.manifest_timeout({'warmup_ms': 1000, 'steady_ms': 2000,
                                                  'overload_ms': 3000, 'recovery_ms': 4000,
                                                  'drain_ms': 5000}), 45.0)

if __name__ == '__main__':
    unittest.main()
