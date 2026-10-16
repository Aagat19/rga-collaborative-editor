import random
import unittest

from crdt_editor import Client, Relay, RGA, shuffled_delivery, synchronization_report


class CRDTTests(unittest.TestCase):
    def make_clients(self):
        relay = Relay()
        clients = [Client(name, relay) for name in ("a", "b", "c")]
        for client in clients:
            client.connect()
        return relay, clients

    def test_offline_anti_entropy_and_convergence(self):
        relay, clients = self.make_clients()
        a, b, c = clients
        a.insert(0, "A")
        b.disconnect()
        b.insert(0, "B")
        c.insert(0, "C")
        b.connect()
        shuffled_delivery(clients, relay.rooms["default"].values(), seed=3)
        for client in clients:
            relay.sync(client)
        self.assertTrue(synchronization_report(relay, clients)["converged"])
        self.assertEqual(len({client.text() for client in clients}), 1)

    def test_duplicate_and_delayed_delivery_is_idempotent(self):
        relay, clients = self.make_clients()
        clients[0].insert(0, "x")
        clients[0].insert(1, "y")
        operations = list(relay.rooms["default"].values())
        shuffled_delivery(clients, operations, seed=99)
        shuffled_delivery(clients, operations, seed=100)
        for client in clients:
            relay.sync(client)
        self.assertTrue(synchronization_report(relay, clients)["converged"])
        self.assertEqual(clients[0].text(), clients[1].text())

    def test_concurrent_insert_and_delete_same_position(self):
        relay, clients = self.make_clients()
        a, b, _ = clients
        a.insert(0, "x")
        relay.sync(b)
        a.disconnect()
        b.disconnect()
        a.insert(0, "A")
        b.delete(0)
        operations = list(a.crdt.operations.values()) + list(b.crdt.operations.values())
        shuffled_delivery(clients, operations, seed=11)
        self.assertEqual(a.crdt.document_hash(), b.crdt.document_hash())
        self.assertIn("A", a.text())
        self.assertNotIn("x", a.text())

    def test_tombstone_requires_stable_delete_summary(self):
        relay, clients = self.make_clients()
        a, b, _ = clients
        a.insert(0, "x")
        relay.sync(b)
        a.delete(0)
        delete_op = next(op for op in a.crdt.operations.values() if op.kind == "delete")
        summaries = {
            a.crdt.replica_id: a.crdt.summary,
            b.crdt.replica_id: {},
            clients[2].crdt.replica_id: {},
        }
        self.assertEqual(
            a.crdt.safe_compact(summaries, summaries), 0
        )
        b.receive(delete_op)
        clients[2].receive(delete_op)
        summaries = {
            replica.crdt.replica_id: replica.crdt.summary for replica in clients
        }
        self.assertEqual(a.crdt.safe_compact(summaries, summaries), 1)

    def test_compaction_rejects_omitted_relevant_replica(self):
        relay, clients = self.make_clients()
        a, b, c = clients
        a.insert(0, "x")
        relay.sync(b)
        a.delete(0)
        summaries = {
            a.crdt.replica_id: a.crdt.summary,
            b.crdt.replica_id: b.crdt.summary,
        }
        with self.assertRaises(ValueError):
            a.crdt.safe_compact(summaries, {"a", "b", "c"})

    def test_multi_character_insert_preserves_order_at_all_positions(self):
        client = Client("single", Relay())
        client.connect()
        client.insert_text(0, "BEGIN")
        client.insert_text(5, "END")
        client.insert_text(5, "-MIDDLE-")
        self.assertEqual(client.text(), "BEGIN-MIDDLE-END")

    def test_exact_offline_text_at_beginning_middle_and_end(self):
        for initial, index, expected in (
            ("", 0, "OFFLINE"),
            ("Hello!", 3, "HelOFFLINElo!"),
            ("Hello!", 6, "Hello!OFFLINE"),
        ):
            client = Client(f"position-{index}", Relay())
            client.connect()
            client.insert_text(0, initial)
            client.insert_text(index, "OFFLINE")
            self.assertEqual(client.text(), expected)

    def test_ordered_chain_survives_shuffled_duplicate_delivery(self):
        relay, clients = self.make_clients()
        source = clients[0]
        source.insert_text(0, "Hello!")
        source.insert_text(3, "OFFLINE")
        operations = list(relay.rooms["default"].values())
        shuffled_delivery(clients, operations, seed=123)
        shuffled_delivery(clients, operations, seed=456)
        for client in clients:
            relay.sync(client)
        self.assertEqual([client.text() for client in clients],
                         ["HelOFFLINElo!"] * 3)
        self.assertEqual(
            len({client.crdt.document_hash() for client in clients}), 1
        )

    def test_insert_waits_for_missing_anchor(self):
        source = RGA("source")
        operations = source.make_inserts(0, "ab")
        source.apply(operations[0])
        source.apply(operations[1])

        receiver = RGA("receiver")
        receiver.apply(operations[1])
        self.assertEqual(receiver.text(), "")
        receiver.apply(operations[0])

        self.assertEqual(receiver.text(), "ab")
        self.assertEqual(receiver.summary, {"source": 2})

    def test_delete_waits_for_missing_target(self):
        source = RGA("source")
        insert = source.make_insert(0, "x")
        source.apply(insert)
        delete = source.make_delete(0)
        source.apply(delete)

        receiver = RGA("receiver")
        receiver.apply(delete)
        self.assertEqual(receiver.text(), "")
        self.assertEqual(receiver.tombstone_count(), 0)
        receiver.apply(insert)

        self.assertEqual(receiver.text(), "")
        self.assertEqual(receiver.tombstone_count(), 1)
        summaries = {receiver.replica_id: receiver.summary}
        self.assertEqual(receiver.safe_compact(summaries, summaries), 1)

    def test_causally_later_beginning_insert_precedes_existing_sibling(self):
        relay = Relay()
        existing = Client("zulu", relay)
        editor = Client("alice", relay)
        existing.connect()
        editor.connect()
        existing.insert_text(0, "Hello!")
        editor.insert_text(0, "OFFLINE")
        self.assertEqual(editor.text(), "OFFLINEHello!")
        offline_ids = editor.crdt.visible_ids()[:len("OFFLINE")]
        self.assertEqual(
            [editor.crdt.nodes[node_id][1] for node_id in offline_ids],
            list("OFFLINE"),
        )

    def test_lamport_order_converges_under_many_shuffled_orders(self):
        operations = []
        for replica_id in "abc":
            replica = RGA(replica_id)
            for value in "xyz":
                operation = replica.make_insert(0, value)
                replica.apply(operation)
                operations.append(operation)
        for seed in range(100):
            shuffled = list(operations)
            random.Random(seed).shuffle(shuffled)
            replicas = [RGA(replica_id) for replica_id in "abc"]
            for operation in shuffled:
                for replica in replicas:
                    replica.apply(operation)
            self.assertEqual(len({replica.document_hash() for replica in replicas}), 1)

    def test_random_three_replica_histories_converge_with_deletes(self):
        for seed in range(25):
            histories = []
            for replica_id in "abc":
                source = RGA(replica_id)
                history = []
                generator = random.Random(seed * 10 + ord(replica_id))
                for _ in range(20):
                    if source.visible_ids() and generator.random() < 0.25:
                        operation = source.make_delete(
                            generator.randrange(len(source.visible_ids()))
                        )
                    else:
                        operation = source.make_insert(
                            generator.randrange(len(source.visible_ids()) + 1),
                            generator.choice("xyz"),
                        )
                    source.apply(operation)
                    history.append(operation)
                histories.extend(history)
            random.Random(seed).shuffle(histories)
            replicas = [RGA(replica_id) for replica_id in "abc"]
            for operation in histories:
                for replica in replicas:
                    replica.apply(operation)
            self.assertEqual(len({replica.document_hash() for replica in replicas}), 1)

    def test_replayed_replica_advances_counter(self):
        original = RGA("restart")
        operation = original.make_insert(0, "x")
        original.apply(operation)
        restarted = RGA("restart")
        restarted.apply(operation)
        next_operation = restarted.make_insert(1, "y")
        self.assertNotEqual(next_operation.id, operation.id)
        restarted.apply(next_operation)
        self.assertEqual(restarted.text(), "xy")

    def test_iterative_walk_handles_long_documents(self):
        replica = RGA("long")
        operations = replica.make_inserts(0, "x" * 1500)
        for operation in operations:
            replica.apply(operation)
        self.assertEqual(len(replica.text()), 1500)

    def test_compaction_requires_stable_minimum_and_no_pending_reference(self):
        replicas = [RGA(name) for name in "abc"]
        insert = replicas[0].make_insert(0, "x")
        for replica in replicas:
            replica.apply(insert)
        delete = replicas[0].make_delete(0)
        for replica in replicas:
            replica.apply(delete)
        before = replicas[0].metadata_bytes()
        summaries = {
            replica.replica_id: replica.summary for replica in replicas
        }
        self.assertEqual(
            replicas[0].safe_compact(summaries, summaries), 1
        )
        self.assertLess(replicas[0].metadata_bytes(), before)

    def test_compaction_preserves_child_anchor(self):
        replica = RGA("anchor")
        replica.apply(replica.make_insert(0, "x"))
        replica.apply(replica.make_insert(1, "y"))
        delete = replica.make_delete(0)
        replica.apply(delete)
        summaries = {replica.replica_id: replica.summary}
        self.assertEqual(replica.safe_compact(summaries, summaries), 0)
        self.assertEqual(replica.text(), "y")


if __name__ == "__main__":
    unittest.main()
