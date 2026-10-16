"""A small, dependency-free collaborative text editor based on an RGA sequence CRDT."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import random
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

HEAD = "HEAD"
NodeId = Tuple[str, int]


def node_key(node_id: NodeId) -> Tuple[str, int]:
    return node_id


@dataclass(frozen=True, order=True)
class Version:
    replica: str
    counter: int

    def as_tuple(self) -> NodeId:
        return self.replica, self.counter


@dataclass(frozen=True)
class Operation:
    """An immutable operation. `op_id` is also the inserted node ID for inserts."""

    op_id: Version
    kind: str
    anchor: Optional[NodeId] = None
    value: Optional[str] = None
    target: Optional[NodeId] = None
    deps: Dict[str, int] = field(default_factory=dict)
    lamport: int = 0

    def __post_init__(self) -> None:
        if self.kind not in {"insert", "delete"}:
            raise ValueError(f"unsupported operation kind: {self.kind}")
        if self.kind == "insert" and (self.anchor is None or self.value is None):
            raise ValueError("insert operations require anchor and value")
        if self.kind == "delete" and self.target is None:
            raise ValueError("delete operations require target")
        if self.value is not None and len(self.value) != 1:
            raise ValueError("an insert operation must contain one character")

    @property
    def id(self) -> NodeId:
        return self.op_id.as_tuple()

    def to_dict(self) -> dict:
        return {
            "op_id": [self.op_id.replica, self.op_id.counter],
            "kind": self.kind,
            "anchor": (
                list(self.anchor) if isinstance(self.anchor, tuple) else self.anchor
            ) if self.anchor else None,
            "value": self.value,
            "target": list(self.target) if self.target else None,
            "deps": dict(sorted(self.deps.items())),
            "lamport": self.lamport,
        }

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()

    @classmethod
    def from_dict(cls, data: dict) -> "Operation":
        return cls(
            op_id=Version(data["op_id"][0], data["op_id"][1]),
            kind=data["kind"],
            anchor=(
                HEAD if data["anchor"] == HEAD else tuple(data["anchor"])
            ) if data.get("anchor") else None,
            value=data.get("value"),
            target=tuple(data["target"]) if data.get("target") else None,
            deps={str(key): int(value) for key, value in data.get("deps", {}).items()},
            lamport=int(data.get("lamport", 0)),
        )


class RGA:
    """Replicated Growable Array with deterministic sibling ordering and tombstones."""

    def __init__(self, replica_id: str) -> None:
        self.replica_id = replica_id
        self._counter = 0
        self._lamport = 0
        self.nodes: Dict[NodeId, Tuple[NodeId | str, str, bool]] = {}
        self._children_by_parent: Dict[NodeId | str, List[NodeId]] = {}
        self.operations: Dict[NodeId, Operation] = {}
        self._pending: Dict[NodeId, Operation] = {}
        self._seen: Dict[str, Set[int]] = {}
        self._delete_by_target: Dict[NodeId, Operation] = {}

    @property
    def summary(self) -> Dict[str, int]:
        """Return the contiguous version-vector prefix known for every replica."""
        result: Dict[str, int] = {}
        for replica, counters in self._seen.items():
            next_counter = 1
            while next_counter in counters:
                next_counter += 1
            result[replica] = next_counter - 1
        return result

    def _next_version(self) -> Version:
        self._counter += 1
        return Version(self.replica_id, self._counter)

    def _new_operation(self, kind: str, **kwargs: object) -> Operation:
        self._lamport += 1
        return Operation(
            self._next_version(), kind, deps=self.summary,
            lamport=self._lamport, **kwargs
        )

    def make_insert(self, index: int, value: str) -> Operation:
        if len(value) != 1:
            raise ValueError("make_insert accepts exactly one character")
        visible = self.visible_ids()
        if index < 0 or index > len(visible):
            raise IndexError("insert index out of range")
        anchor: NodeId | str = HEAD if index == 0 else visible[index - 1]
        return self._new_operation("insert", anchor=anchor, value=value)

    def make_inserts(self, index: int, text: str) -> List[Operation]:
        """Create a contiguous run whose anchors preserve the input order."""
        if index < 0 or index > len(self.visible_ids()):
            raise IndexError("insert index out of range")
        operations: List[Operation] = []
        anchor: NodeId | str = HEAD if index == 0 else self.visible_ids()[index - 1]
        for character in text:
            operation = self._new_operation("insert", anchor=anchor, value=character)
            operations.append(operation)
            anchor = operation.id
        return operations

    def make_delete(self, index: int) -> Operation:
        visible = self.visible_ids()
        if index < 0 or index >= len(visible):
            raise IndexError("delete index out of range")
        return self._new_operation("delete", target=visible[index])

    def apply(self, operation: Operation) -> bool:
        """Apply once; return False for an already-known operation."""
        op_id = operation.id
        if op_id in self.operations or op_id in self._pending:
            return False
        if operation.kind == "insert" and operation.anchor != HEAD:
            if operation.anchor not in self.nodes:
                self._pending[op_id] = operation
                return True
        if operation.kind == "delete" and operation.target not in self.nodes:
            self._pending[op_id] = operation
            return True
        self._apply_ready(operation)
        self._drain_pending()
        return True

    def _apply_ready(self, operation: Operation) -> None:
        op_id = operation.id
        self.operations[op_id] = operation
        self._lamport = max(self._lamport, operation.lamport)
        if operation.op_id.replica == self.replica_id:
            self._counter = max(self._counter, operation.op_id.counter)
        self._seen.setdefault(operation.op_id.replica, set()).add(operation.op_id.counter)
        if operation.kind == "insert":
            assert operation.anchor is not None and operation.value is not None
            self.nodes[op_id] = (operation.anchor, operation.value, False)
            children = self._children_by_parent.setdefault(operation.anchor, [])
            children.append(op_id)
        else:
            assert operation.target is not None
            anchor, value, _ = self.nodes[operation.target]
            self.nodes[operation.target] = (anchor, value, True)
            self._delete_by_target[operation.target] = operation

    def _drain_pending(self) -> None:
        changed = True
        while changed:
            changed = False
            for op_id, operation in list(self._pending.items()):
                ready = (
                    operation.anchor == HEAD
                    or operation.anchor in self.nodes
                ) if operation.kind == "insert" else operation.target in self.nodes
                if ready:
                    del self._pending[op_id]
                    self._apply_ready(operation)
                    changed = True

    def _children(self, anchor: NodeId | str) -> List[NodeId]:
        return sorted(
            self._children_by_parent.get(anchor, []),
            key=lambda node_id: (
                self.operations[node_id].lamport,
                node_id[0],
                node_id[1],
            ),
            reverse=True,
        )

    def _walk(self, anchor: NodeId | str, output: List[NodeId]) -> None:
        stack = list(reversed(self._children(anchor)))
        while stack:
            node_id = stack.pop()
            output.append(node_id)
            stack.extend(reversed(self._children(node_id)))

    def all_ids(self) -> List[NodeId]:
        result: List[NodeId] = []
        self._walk(HEAD, result)
        return result

    def visible_ids(self) -> List[NodeId]:
        return [node_id for node_id in self.all_ids() if not self.nodes[node_id][2]]

    def text(self) -> str:
        return "".join(self.nodes[node_id][1] for node_id in self.visible_ids())

    def document_hash(self) -> str:
        return hashlib.sha256(self.text().encode("utf-8")).hexdigest()

    def tombstone_count(self) -> int:
        return sum(1 for _, _, deleted in self.nodes.values() if deleted)

    def metadata_bytes(self) -> int:
        return sum(len(json.dumps({
            "id": list(node_id), "node": [list(parent) if isinstance(parent, tuple) else parent, value, deleted]
        }, separators=(",", ":")).encode()) for node_id, (parent, value, deleted) in self.nodes.items())

    @staticmethod
    def minimum_summary(summaries: Iterable[Dict[str, int]]) -> Dict[str, int]:
        vectors = list(summaries)
        if not vectors:
            return {}
        replicas = set().union(*(vector.keys() for vector in vectors))
        return {
            replica: min(vector.get(replica, 0) for vector in vectors)
            for replica in replicas
        }

    def safe_compact(
        self,
        replica_summaries: Mapping[str, Dict[str, int]],
        relevant_replicas: Iterable[str],
    ) -> int:
        """Remove tombstones covered by the minimum of all supplied summaries.

        Callers must label and provide one summary per relevant replica. A
        permanently offline replica must remain in this collection, otherwise
        its missing acknowledgement cannot be represented and compaction is
        rejected.
        """
        required = set(relevant_replicas)
        missing = required.difference(replica_summaries)
        if not required or missing:
            raise ValueError(
                "safe_compact requires a summary for every relevant replica"
            )
        stable_summary = self.minimum_summary(replica_summaries.values())
        removable: Set[NodeId] = set()
        for node_id, (_, _, deleted) in self.nodes.items():
            if not deleted:
                continue
            delete_op = self._delete_by_target.get(node_id)
            if delete_op and stable_summary.get(delete_op.op_id.replica, 0) >= delete_op.op_id.counter:
                anchored = bool(self._children_by_parent.get(node_id))
                pending_reference = any(
                    operation.kind == "insert" and operation.anchor == node_id
                    or operation.kind == "delete" and operation.target == node_id
                    for operation in self._pending.values()
                )
                if not anchored and not pending_reference:
                    removable.add(node_id)
        for node_id in removable:
            parent, _, _ = self.nodes[node_id]
            del self.nodes[node_id]
            self._children_by_parent.get(parent, []).remove(node_id)
            self._delete_by_target.pop(node_id, None)
        return len(removable)


class Relay:
    """In-memory persistent relay. It stores an append-only operation log per room."""

    def __init__(self) -> None:
        self.rooms: Dict[str, Dict[NodeId, Operation]] = {}
        self.connected: Dict[str, Set["Client"]] = {}
        self.bytes_forwarded = 0

    def connect(self, client: "Client", room: str = "default") -> None:
        self.connected.setdefault(room, set()).add(client)
        self.sync(client, room)

    def disconnect(self, client: "Client", room: str = "default") -> None:
        self.connected.get(room, set()).discard(client)

    def publish(self, client: "Client", operation: Operation, room: str = "default") -> None:
        log = self.rooms.setdefault(room, {})
        if operation.id in log:
            return
        log[operation.id] = operation
        for peer in self.connected.get(room, set()):
            if peer is not client:
                self.bytes_forwarded += len(operation.to_bytes())
                peer.receive(operation)

    def sync(self, client: "Client", room: str = "default") -> int:
        missing = self.missing(room, client.crdt.summary)
        for operation in missing:
            self.bytes_forwarded += len(operation.to_bytes())
            client.receive(operation)
        return len(missing)

    def missing(self, room: str, summary: Dict[str, int]) -> List[Operation]:
        return [op for op in self.rooms.get(room, {}).values()
                if op.op_id.counter > summary.get(op.op_id.replica, 0)]

    def log_metadata_bytes(self, room: str = "default") -> int:
        return sum(len(operation.to_bytes()) for operation in self.rooms.get(room, {}).values())


class Client:
    def __init__(self, replica_id: str, relay: Relay, room: str = "default") -> None:
        self.crdt = RGA(replica_id)
        self.relay = relay
        self.room = room
        self.online = False
        self._outbox: List[Operation] = []

    def connect(self) -> None:
        self.online = True
        self.relay.connect(self, self.room)
        self.flush()

    def disconnect(self) -> None:
        self.online = False
        self.relay.disconnect(self, self.room)

    def receive(self, operation: Operation) -> None:
        self.crdt.apply(operation)

    def submit(self, operation: Operation) -> None:
        self.crdt.apply(operation)
        self._outbox.append(operation)
        self.flush()

    def flush(self) -> None:
        if not self.online:
            return
        while self._outbox:
            self.relay.publish(self, self._outbox.pop(0), self.room)
        self.relay.sync(self, self.room)

    def insert(self, index: int, value: str) -> None:
        self.submit(self.crdt.make_insert(index, value))

    def insert_text(self, index: int, text: str) -> None:
        operations = self.crdt.make_inserts(index, text)
        for operation in operations:
            self.crdt.apply(operation)
            self._outbox.append(operation)
        self.flush()

    def delete(self, index: int) -> None:
        self.submit(self.crdt.make_delete(index))

    def text(self) -> str:
        return self.crdt.text()


def shuffled_delivery(clients: Iterable[Client], operations: Iterable[Operation], seed: int = 7) -> None:
    """Broadcast duplicated operations in a deterministic shuffled order."""
    queue = list(operations)
    queue.extend(queue[:])
    random.Random(seed).shuffle(queue)
    clients_list = list(clients)
    for operation in queue:
        for client in clients_list:
            client.receive(operation)


def synchronization_report(relay: Relay, clients: Iterable[Client], room: str = "default") -> dict:
    client_list = list(clients)
    return {
        "document_hashes": [client.crdt.document_hash() for client in client_list],
        "converged": len({client.crdt.document_hash() for client in client_list}) == 1,
        "relay_log_bytes": relay.log_metadata_bytes(room),
        "relay_forwarded_bytes": relay.bytes_forwarded,
        "tombstones": [client.crdt.tombstone_count() for client in client_list],
        "metadata_bytes": [client.crdt.metadata_bytes() for client in client_list],
    }
