"""Socket client and minimal interactive editor for the persistent relay."""

from __future__ import annotations

import argparse
import json
import socket
import threading
from typing import Dict, Optional

from crdt_editor import Operation, RGA


class NetworkClient:
    def __init__(self, replica_id: str, host: str, port: int, room: str = "default") -> None:
        self.crdt = RGA(replica_id)
        self.host, self.port, self.room = host, port, room
        self.socket: Optional[socket.socket] = None
        self.file = None
        self.reader_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.lock = threading.RLock()
        self.send_lock = threading.Lock()
        self._relay_summary: Dict[str, int] = {}
        self.sent_bytes = 0
        self.received_bytes = 0
        self.received_messages: list[dict] = []
        self.outbox: list[Operation] = []

    def connect(self) -> None:
        with self.lock:
            if self.is_connected:
                return
            sock = socket.create_connection((self.host, self.port), timeout=5)
            sock.settimeout(None)
            reader = sock.makefile("rwb")
            self.socket = sock
            self.file = reader
            self.stop_event.clear()
            self.ready_event.clear()
            self._relay_summary = {}
            self.reader_thread = threading.Thread(
                target=self._read_loop, args=(reader,), daemon=True
            )
            self.reader_thread.start()
            hello = {
                "type": "hello",
                "replica": self.crdt.replica_id,
                "room": self.room,
                "summary": self.crdt.summary,
            }
        self.send(hello)
        if not self.ready_event.wait(5):
            self.disconnect()
            raise TimeoutError("relay did not acknowledge hello")
        with self.lock:
            relay_summary = dict(self._relay_summary)
            operations = [
                operation for operation in self.crdt.operations.values()
                if operation.op_id.counter
                > relay_summary.get(operation.op_id.replica, 0)
            ]
            operation_ids = {operation.id for operation in operations}
            operations.extend(
                operation for operation in self.outbox
                if operation.id not in operation_ids
            )
            self.outbox.clear()
        for operation in sorted(operations, key=lambda item: (
            item.op_id.replica, item.op_id.counter
        )):
            try:
                self.send({"type": "op", "operation": operation.to_dict()})
            except OSError:
                with self.lock:
                    self.outbox.append(operation)
                self._mark_offline(reader)
                break

    def _read_loop(self, reader) -> None:
        try:
            for raw in reader:
                message = json.loads(raw)
                with self.lock:
                    self.received_bytes += len(raw)
                    self.received_messages.append(message)
                    if message["type"] == "ready":
                        self._relay_summary = {
                            str(key): int(value)
                            for key, value in message.get("summary", {}).items()
                        }
                        self.ready_event.set()
                    elif message["type"] == "op":
                        self.crdt.apply(Operation.from_dict(message["operation"]))
                if self.stop_event.is_set():
                    break
        except (OSError, ValueError, json.JSONDecodeError):
            self._mark_offline(reader)
        finally:
            self._mark_offline(reader)

    def _mark_offline(self, reader) -> None:
        with self.lock:
            if self.file is reader:
                self.stop_event.set()
                self.file = None
                self.socket = None

    @property
    def is_connected(self) -> bool:
        with self.lock:
            return self.file is not None and not self.stop_event.is_set()

    def send(self, message: dict) -> None:
        with self.lock:
            if self.file is None:
                raise ConnectionError("client is offline")
            raw = (json.dumps(message, separators=(",", ":")) + "\n").encode()
            writer = self.file
        with self.send_lock:
            writer.write(raw)
            writer.flush()
        with self.lock:
            self.sent_bytes += len(raw)

    def publish(self, operation: Operation) -> None:
        with self.lock:
            self.crdt.apply(operation)
            if self.file is None:
                self.outbox.append(operation)
                return
        try:
            self.send({"type": "op", "operation": operation.to_dict()})
        except (OSError, ConnectionError):
            with self.lock:
                self.outbox.append(operation)
                reader = self.file
            self._mark_offline(reader)

    def sync(self) -> None:
        if not self.is_connected:
            raise ConnectionError("client is offline; use reconnect first")
        with self.lock:
            summary = self.crdt.summary
        self.send({"type": "summary", "summary": summary})

    def disconnect(self) -> None:
        with self.lock:
            reader = self.file
            sock = self.socket
            reader_thread = self.reader_thread
            if reader is None:
                return
            self.stop_event.set()
            self.ready_event.clear()
            self.file = None
            self.socket = None
        try:
            with self.send_lock:
                reader.write(b'{"type":"bye"}\n')
                reader.flush()
        except OSError:
            pass
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        try:
            reader.close()
        except OSError:
            pass
        if reader_thread is not None and reader_thread is not threading.current_thread():
            reader_thread.join(timeout=1)
        with self.lock:
            if self.reader_thread is reader_thread:
                self.reader_thread = None

    def insert(self, index: int, value: str) -> None:
        with self.lock:
            operation = self.crdt.make_insert(index, value)
        self.publish(operation)

    def insert_text(self, index: int, text: str) -> None:
        with self.lock:
            operations = self.crdt.make_inserts(index, text)
        for operation in operations:
            self.publish(operation)

    def delete(self, index: int) -> None:
        with self.lock:
            operation = self.crdt.make_delete(index)
        self.publish(operation)

    def text(self) -> str:
        with self.lock:
            return self.crdt.text()

    def document_hash(self) -> str:
        with self.lock:
            return self.crdt.document_hash()


def handle_command(client: NetworkClient, line: str) -> bool:
    """Execute one CLI command; return True only when the process should exit."""
    parts = line.split(maxsplit=2)
    if not parts:
        return False
    if parts[0] == "insert" and len(parts) == 3:
        client.insert_text(int(parts[1]), parts[2])
    elif parts[0] == "delete" and len(parts) == 2:
        client.delete(int(parts[1]))
    elif parts[0] == "show":
        status = "connected" if client.is_connected else "offline"
        print(client.text(), client.document_hash(), status)
    elif parts[0] == "disconnect":
        client.disconnect()
        print("Disconnected from relay; local editing remains enabled.")
    elif parts[0] == "reconnect":
        client.connect()
        client.sync()
        print("Reconnected and requested anti-entropy synchronization.")
    elif parts[0] == "quit":
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal network CRDT editor")
    parser.add_argument("replica")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--machine", action="store_true")
    args = parser.parse_args()
    client = NetworkClient(args.replica, args.host, args.port)
    client.connect()
    print(
        "Connected. Commands: insert INDEX TEXT | delete INDEX | show | "
        "disconnect | reconnect | quit"
        , flush=True
    )
    try:
        while True:
            if args.machine:
                print("READY", flush=True)
            if handle_command(client, input("> ")):
                break
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
