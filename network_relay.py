"""Persistent TCP relay for three (or more) collaborative CRDT clients.

Protocol: newline-delimited JSON messages. A client sends hello, then op and
summary messages. The relay stores every operation in SQLite and sends missing
operations based on the client's version vector.
"""

from __future__ import annotations

import argparse
import json
import socketserver
import sqlite3
import threading
from typing import Dict

from crdt_editor import Operation


class OperationStore:
    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS operations (room TEXT NOT NULL, "
            "replica TEXT NOT NULL, counter INTEGER NOT NULL, payload TEXT NOT NULL, "
            "PRIMARY KEY(room, replica, counter))"
        )
        self.db.commit()
        self.lock = threading.Lock()

    def put(self, room: str, operation: Operation) -> bool:
        with self.lock:
            existing = self.db.execute(
                "SELECT 1 FROM operations WHERE room = ? AND replica = ? AND counter = ?",
                (room, operation.op_id.replica, operation.op_id.counter),
            ).fetchone()
            if existing is not None:
                return False
            self.db.execute(
                "INSERT INTO operations VALUES (?, ?, ?, ?)",
                (room, operation.op_id.replica, operation.op_id.counter,
                 json.dumps(operation.to_dict(), sort_keys=True)),
            )
            self.db.commit()
            return True

    def missing(self, room: str, summary: Dict[str, int]) -> list[Operation]:
        with self.lock:
            rows = self.db.execute(
                "SELECT payload FROM operations WHERE room = ? ORDER BY replica, counter",
                (room,),
            ).fetchall()
        missing: list[Operation] = []
        for payload, in rows:
            data = json.loads(payload)
            replica, counter = data["op_id"]
            if counter > summary.get(replica, 0):
                missing.append(Operation.from_dict(data))
        return missing

    def summary(self, room: str) -> Dict[str, int]:
        with self.lock:
            rows = self.db.execute(
                "SELECT replica, counter FROM operations WHERE room = ? "
                "ORDER BY replica, counter",
                (room,),
            ).fetchall()
        counters: Dict[str, set[int]] = {}
        for replica, counter in rows:
            counters.setdefault(replica, set()).add(counter)
        result: Dict[str, int] = {}
        for replica, known in counters.items():
            counter = 1
            while counter in known:
                counter += 1
            result[replica] = counter - 1
        return result

    def bytes(self, room: str) -> int:
        with self.lock:
            row = self.db.execute(
                "SELECT COALESCE(SUM(length(payload)), 0) FROM operations WHERE room = ?",
                (room,),
            ).fetchone()
        return int(row[0])

    def close(self) -> None:
        self.db.close()


class RelayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], database: str) -> None:
        self.store = OperationStore(database)
        self.peers: dict[str, set["RelayHandler"]] = {}
        self.peers_lock = threading.Lock()
        super().__init__(address, RelayHandler)

    def register(self, handler: "RelayHandler", room: str) -> None:
        with self.peers_lock:
            peers = self.peers.setdefault(room, set())
            if handler.replica:
                peers.difference_update(
                    {peer for peer in list(peers) if peer.replica == handler.replica}
                )
            peers.add(handler)

    def unregister(self, handler: "RelayHandler", room: str | None) -> None:
        if room is None:
            return
        with self.peers_lock:
            self.peers.get(room, set()).discard(handler)

    def broadcast(self, sender: "RelayHandler", operation: Operation) -> None:
        with self.peers_lock:
            peers = list(self.peers.get(sender.room or "", set()))
        for peer in peers:
            if peer is not sender:
                try:
                    peer.send({"type": "op", "operation": operation.to_dict()})
                except OSError:
                    self.unregister(peer, peer.room)


class RelayHandler(socketserver.StreamRequestHandler):
    server: RelayServer

    def setup(self) -> None:
        super().setup()
        self.room: str | None = None
        self.replica: str | None = None
        self.send_lock = threading.Lock()

    def send(self, message: dict) -> None:
        encoded = (json.dumps(message, separators=(",", ":")) + "\n").encode()
        with self.send_lock:
            self.wfile.write(encoded)
            self.wfile.flush()

    def handle(self) -> None:
        try:
            for line in self.rfile:
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if message["type"] == "hello":
                    self.room = str(message.get("room", "default"))
                    self.replica = str(message.get("replica", ""))
                    self.server.register(self, self.room)
                    self.send({
                        "type": "ready",
                        "stored_bytes": self.server.store.bytes(self.room),
                        "summary": self.server.store.summary(self.room),
                    })
                    self.send_missing(message.get("summary", {}))
                elif message["type"] == "op":
                    operation = Operation.from_dict(message["operation"])
                    if self.room is None:
                        raise ValueError("hello must be sent before operations")
                    if self.server.store.put(self.room, operation):
                        self.server.broadcast(self, operation)
                elif message["type"] == "summary":
                    self.send_missing(message.get("summary", {}))
                elif message["type"] == "bye":
                    return
        finally:
            self.server.unregister(self, self.room)

    def send_missing(self, summary: dict) -> None:
        if self.room is None:
            return
        for operation in self.server.store.missing(self.room, summary):
            try:
                self.send({"type": "op", "operation": operation.to_dict()})
            except OSError:
                self.server.unregister(self, self.room)
                return


def main() -> None:
    parser = argparse.ArgumentParser(description="Persistent CRDT relay")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--database", default="relay.sqlite3")
    args = parser.parse_args()
    server = RelayServer((args.host, args.port), args.database)
    print(f"relay listening on {args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        server.store.close()


if __name__ == "__main__":
    main()
