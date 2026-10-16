from crdt_editor import Client, RGA, Relay, shuffled_delivery, synchronization_report
from editor import Editor


def main() -> None:
    relay = Relay()
    clients = [Client(replica, relay) for replica in ("alice", "bob", "carol")]
    for client in clients:
        client.connect()

    Editor(clients[0]).insert("x")
    for client in clients:
        relay.sync(client)
    before_delete = synchronization_report(relay, clients)

    clients[1].disconnect()
    clients[2].disconnect()
    clients[1].insert(0, "A")
    clients[2].delete(0)
    operations = list(clients[1].crdt.operations.values())
    operations.extend(clients[2].crdt.operations.values())
    shuffled_delivery(clients, operations, seed=25)
    for client in clients:
        relay.sync(client)
    report = synchronization_report(relay, clients)
    print("concurrent insertion/deletion result:", repr(clients[0].text()))
    print("converged:", report["converged"])
    print("hashes:", report["document_hashes"])
    print("metadata before delete:", before_delete["metadata_bytes"])
    print("metadata after delete:", report["metadata_bytes"])

    summaries = {client.crdt.replica_id: client.crdt.summary for client in clients}
    replicas = {client.crdt.replica_id for client in clients}
    for client in clients:
        client.crdt.safe_compact(summaries, replicas)
    compacted = synchronization_report(relay, clients)
    print("metadata after stable compaction:", compacted["metadata_bytes"])
    print("sync bytes:", {
        key: report[key] for key in ("relay_log_bytes", "relay_forwarded_bytes")
    })
    print(
        "Tombstones may be removed only after every replica acknowledges the "
        "delete, no node or pending operation references it, and all anchors "
        "are visible. A permanently offline replica blocks removal."
    )


if __name__ == "__main__":
    main()
