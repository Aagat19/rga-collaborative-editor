import tempfile
import time
import unittest
import os
import json
import socket

from network_client import NetworkClient, handle_command
from network_relay import OperationStore, RelayServer


class NetworkEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = os.path.join(self.temp_dir.name, "relay.sqlite3")
        self.server = RelayServer(
            ("127.0.0.1", 0), self.database
        )
        import threading
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]
        self.clients = [NetworkClient(name, "127.0.0.1", self.port) for name in ("alice", "bob", "carol")]
        for client in self.clients:
            client.connect()
        time.sleep(0.1)

    def tearDown(self):
        for client in self.clients:
            client.disconnect()
        self.server.shutdown()
        self.server.server_close()
        self.server.store.close()
        self.temp_dir.cleanup()

    def wait_for_convergence(self):
        for _ in range(30):
            if len({client.crdt.document_hash() for client in self.clients}) == 1:
                return
            time.sleep(0.05)
        self.fail([client.text() for client in self.clients])

    def test_three_real_socket_clients_and_offline_anti_entropy(self):
        alice, bob, carol = self.clients
        alice.insert(0, "A")
        time.sleep(0.1)
        bob.disconnect()
        self.assertFalse(bob.is_connected)
        bob.insert(0, "B")
        self.assertIn("B", bob.text())
        carol.insert(0, "C")
        bob.connect()
        self.assertTrue(bob.is_connected)
        bob.sync()
        for client in self.clients:
            client.sync()
        self.wait_for_convergence()
        self.assertEqual(len({client.crdt.document_hash() for client in self.clients}), 1)
        self.assertGreater(self.server.store.bytes("default"), 0)
        self.assertGreater(alice.received_bytes + carol.received_bytes, 0)

    def test_exact_offline_insert_content_and_hashes_on_all_clients(self):
        alice, bob, carol = self.clients
        alice.insert_text(0, "Hello!")
        time.sleep(0.2)
        self.assertEqual(alice.text(), "Hello!")
        self.assertEqual(bob.text(), "Hello!")
        self.assertEqual(carol.text(), "Hello!")

        bob.disconnect()
        bob.insert_text(0, "OFFLINE")
        self.assertEqual(bob.text(), "OFFLINEHello!")
        bob.connect()
        bob.sync()
        for client in self.clients:
            client.sync()
        self.wait_for_convergence()

        self.assertEqual([client.text() for client in self.clients],
                         ["OFFLINEHello!"] * 3)
        self.assertEqual(
            len({client.crdt.document_hash() for client in self.clients}), 1
        )
        self.assertEqual(len(bob.outbox), 0)
        for client in self.clients:
            operation_ids = list(client.crdt.operations)
            self.assertEqual(len(operation_ids), len(set(operation_ids)))
            self.assertEqual(len(operation_ids), 13)
            self.assertEqual(
                [client.crdt.nodes[node_id][1]
                 for node_id in client.crdt.visible_ids()[:7]],
                list("OFFLINE"),
            )

    def test_disconnect_preserves_process_state_and_reconnects(self):
        alice, bob, _ = self.clients
        for offset, character in enumerate("online"):
            alice.insert(offset, character)
        time.sleep(0.1)
        bob.disconnect()
        text_before_offline = bob.text()
        for offset, character in enumerate("offline"):
            bob.insert(offset, character)
        self.assertNotEqual(bob.text(), text_before_offline)
        self.assertTrue(all(character in bob.text() for character in "offline"))
        self.assertEqual(len(bob.outbox), len("offline"))
        bob.connect()
        bob.sync()
        for client in self.clients:
            client.sync()
        self.wait_for_convergence()
        self.assertTrue(bob.is_connected)
        self.assertEqual(len(bob.outbox), 0)

    def test_cli_commands_keep_process_alive_while_offline(self):
        _, bob, _ = self.clients
        self.assertFalse(handle_command(bob, "disconnect"))
        self.assertFalse(bob.is_connected)
        self.assertFalse(handle_command(bob, "insert 0 OFFLINE"))
        self.assertIn("OFFLINE", bob.text())
        self.assertFalse(handle_command(bob, "reconnect"))
        self.assertTrue(bob.is_connected)
        self.assertTrue(handle_command(bob, "quit"))
        self.assertTrue(bob.is_connected)

    def test_reconnect_sends_local_operations_missing_from_relay(self):
        alice, bob, _ = self.clients
        bob.disconnect()
        operation = bob.crdt.make_insert(0, "R")
        bob.crdt.apply(operation)
        bob.connect()
        bob.sync()
        time.sleep(0.2)
        self.assertEqual(self.server.store.summary("default").get("bob"), 1)
        alice.sync()
        self.wait_for_convergence()
        self.assertEqual([client.text() for client in self.clients], ["R"] * 3)

    def test_relay_ignores_malformed_json_line(self):
        raw = socket.create_connection(("127.0.0.1", self.port), timeout=2)
        raw.sendall(b"not-json\n")
        raw.sendall((json.dumps({
            "type": "hello",
            "replica": "malformed-test",
            "room": "default",
            "summary": {},
        }) + "\n").encode())
        response = raw.recv(4096).decode()
        raw.close()
        self.assertIn('"type":"ready"', response)

    def test_store_reads_legacy_operation_without_lamport(self):
        path = os.path.join(self.temp_dir.name, "legacy.sqlite3")
        store = OperationStore(path)
        store.db.execute(
            "INSERT INTO operations VALUES (?, ?, ?, ?)",
            ("default", "legacy", 1, json.dumps({
                "op_id": ["legacy", 1],
                "kind": "insert",
                "anchor": "HEAD",
                "value": "x",
                "target": None,
                "deps": {},
            })),
        )
        store.db.commit()
        operations = store.missing("default", {})
        store.close()
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0].lamport, 0)

    def test_relay_restart_recovers_persisted_operations(self):
        alice, bob, _ = self.clients
        alice.insert_text(0, "persisted")
        for _ in range(30):
            if self.server.store.summary("default").get("alice", 0) >= 8:
                break
            time.sleep(0.05)
        self.assertGreaterEqual(self.server.store.summary("default").get("alice", 0), 8)
        for client in self.clients:
            client.disconnect()
        self.server.shutdown()
        self.server.server_close()
        self.server.store.close()

        self.server = RelayServer(("127.0.0.1", self.port), self.database)
        self.thread = __import__("threading").Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        recovered = NetworkClient("new-client", "127.0.0.1", self.port)
        recovered.connect()
        recovered.sync()
        for _ in range(30):
            if recovered.text() == "persisted":
                break
            time.sleep(0.05)
        self.assertEqual(recovered.text(), "persisted")
        recovered.disconnect()


if __name__ == "__main__":
    unittest.main()
