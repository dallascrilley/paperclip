"""Small HTTP/OS-lock fixtures for the source probe. No provider or heavy job."""
import fcntl
import importlib.util
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

spec = importlib.util.spec_from_file_location("admission", Path(__file__).parents[1] / "probe-local-resource-admission.py")
admission = importlib.util.module_from_spec(spec)
spec.loader.exec_module(admission)


class Fixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = {}
        cls.requests = 0
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                cls.requests += 1
                data = json.dumps(cls.state).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(data)
            def log_message(self, *args):
                pass
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = "http://127.0.0.1:" + str(cls.server.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.lock = Path(self.temp.name) / "job.lock"
        self.lock.touch(mode=0o600)
        type(self).state = {"status": "ready", "busy": False, "active_model": None,
                            "memory": {"available_gib": 8, "pressure_level": 1}}

    def tearDown(self):
        self.temp.cleanup()

    def check(self, **kwargs):
        return admission.probe(kwargs.get("url", self.url), kwargs.get("lock", str(self.lock)), 4, 0.2)

    def test_pressure_recovers_without_changing_lock_inode(self):
        inode = self.lock.stat().st_ino
        self.state["memory"]["pressure_level"] = 2
        self.assertEqual(self.check()["reason"], "memory_pressure")
        self.state["memory"]["pressure_level"] = 1
        self.assertTrue(self.check()["admitted"])
        self.assertEqual(self.lock.stat().st_ino, inode)

    def test_held_gate_defers_before_http_then_recovers(self):
        with self.lock.open("r+") as holder:
            fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
            before = type(self).requests
            self.assertEqual(self.check()["reason"], "heavy_job_busy")
            self.assertEqual(type(self).requests, before)
        self.assertTrue(self.check()["admitted"])

    def test_headroom_busy_model_and_invalid_snapshot_fail_closed(self):
        self.state["memory"]["available_gib"] = 3
        self.assertEqual(self.check()["reason"], "insufficient_headroom")
        self.state["memory"]["available_gib"] = float("nan")
        self.assertEqual(self.check()["reason"], "invalid_guard_response")
        self.state["memory"] = {"available_gib": 8, "pressure_level": True}
        self.assertEqual(self.check()["reason"], "invalid_guard_response")
        self.state["memory"]["pressure_level"] = 1
        self.state["busy"] = True
        self.assertEqual(self.check()["reason"], "guard_not_ready")
        self.state["busy"] = False
        self.state["active_model"] = "fixture"
        self.assertEqual(self.check()["reason"], "model_active")

    def test_missing_symlink_and_remote_target_fail_closed(self):
        self.assertEqual(self.check(lock=str(self.lock) + "missing")["reason"], "job_gate_unavailable")
        link = self.lock.with_name("link")
        link.symlink_to(self.lock)
        self.assertEqual(self.check(lock=str(link))["reason"], "job_gate_unavailable")
        self.assertEqual(self.check(url="http://example.com")["reason"], "invalid_guard_target")

    def test_outage_and_malformed_response_fail_closed(self):
        self.assertEqual(self.check(url="http://127.0.0.1:1")["reason"], "guard_unavailable")
        type(self).state = {"status": "ready"}
        self.assertEqual(self.check()["reason"], "invalid_guard_response")


if __name__ == "__main__":
    unittest.main()
