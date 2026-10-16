import os
import queue
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest


PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DOCUMENT_LINE = re.compile(
    r"(?m)(?P<text>OFFLINEHello!) (?P<hash>[0-9a-f]{64}) (?P<status>\w+)"
)


class ManagedProcess:
    def __init__(self, command: list[str], cwd: str) -> None:
        self.process = subprocess.Popen(
            [sys.executable, "-u", *command],
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self.stdin = self.process.stdin
        self.output: queue.Queue[str] = queue.Queue()
        self.history = ""
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.output.put(line)

    def wait_for(self, expected: str, timeout: float = 5, start: int = 0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                self.history += self.output.get(timeout=0.1)
            except queue.Empty:
                if self.process.poll() is not None:
                    break
            if expected in self.history[start:]:
                return self.history
        raise AssertionError(
            f"did not observe {expected!r}; process output was:\n{self.history}"
        )

    def command(self, command: str, expected: str | None, timeout: float = 5) -> str:
        start = len(self.history)
        self.stdin.write(command + "\n")
        self.stdin.flush()
        self.wait_for("READY", timeout, start=start)
        if expected is None:
            return self.history
        return self.wait_for(expected, timeout, start=start)

    def stop(self) -> None:
        if self.process.poll() is None:
            self.stdin.write("quit\n")
            self.stdin.flush()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.stdin.close()
        if self.process.stdout is not None:
            self.process.stdout.close()
        self.reader.join(timeout=1)

    def terminate(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.stdin.close()
        if self.process.stdout is not None:
            self.process.stdout.close()
        self.reader.join(timeout=1)


class ProcessEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database = os.path.join(self.temp_dir.name, "relay.sqlite3")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.relay = ManagedProcess(
            ["network_relay.py", "--host", "127.0.0.1",
             "--port", str(self.port), "--database", database],
            PROJECT_DIR,
        )
        self.relay.wait_for("relay listening on")
        self.clients = [
            ManagedProcess(
                ["network_client.py", replica, "--host", "127.0.0.1",
                 "--port", str(self.port), "--machine"],
                PROJECT_DIR,
            )
            for replica in ("alice", "bob", "carol")
        ]
        for client in self.clients:
            client.wait_for("Connected.")
            client.wait_for("READY")

    def tearDown(self) -> None:
        for client in self.clients:
            client.stop()
        self.relay.terminate()
        self.temp_dir.cleanup()

    def test_three_process_offline_reconnect_propagates_exact_document(self):
        alice, bob, carol = self.clients
        alice.command("insert 0 Hello!", None, timeout=5)
        for client in (alice, bob, carol):
            client.command("show", "Hello! ", timeout=5)

        bob.command("disconnect", "local editing remains enabled")
        bob.command("insert 0 OFFLINE", None, timeout=5)
        offline_output = bob.command("show", "OFFLINEHello! ", timeout=5)
        self.assertRegex(offline_output, r"OFFLINEHello! [0-9a-f]{64} offline")

        bob.command("reconnect", "anti-entropy synchronization")
        bob_output = bob.command("show", "OFFLINEHello! ", timeout=5)
        alice_output = alice.command("show", "OFFLINEHello! ", timeout=5)
        carol_output = carol.command("show", "OFFLINEHello! ", timeout=5)

        matches = [
            DOCUMENT_LINE.findall(output)[-1:] for output in
            (alice_output, bob_output, carol_output)
        ]
        matches = [match[0] if match else None for match in matches]
        self.assertTrue(all(matches))
        assert all(matches)
        self.assertEqual({match[0] for match in matches}, {"OFFLINEHello!"})
        self.assertEqual({match[1] for match in matches}, {matches[0][1]})
        self.assertEqual({match[2] for match in matches}, {"connected"})


if __name__ == "__main__":
    unittest.main()
