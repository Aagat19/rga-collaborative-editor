# Real-Time Collaborative Text Editor

This project implements a dependency-free sequence CRDT using an RGA (Replicated
Growable Array). Each inserted character has a globally unique `(replica,
counter)` identifier. Operations carry a Lamport timestamp and a version-vector
dependency summary. Inserts reference an anchor character, deletes set a
tombstone, and siblings are ordered deterministically by descending
`(lamport, replica, counter)`. Lamport order handles causal siblings; the full
tuple resolves concurrent siblings without depending on arrival order.

## Run

```text
python -m unittest -v
python -m unittest -v test_process_e2e.py
python demo.py
```

`test_process_e2e.py` owns the complete process-level verification: it starts
a fresh temporary SQLite relay plus independent Alice, Bob, and Carol client
processes, waits for explicit acknowledgements with bounded timeouts, performs
the offline/reconnect workflow, and asserts exact text and identical hashes.
It does not use manual input or arbitrary sleeps.

For the actual end-to-end distributed run, start the persistent relay in one
terminal:

```text
python network_relay.py --port 8765 --database relay.sqlite3
```

Then start three independent clients in three terminals:

```text
python network_client.py alice
python network_client.py bob
python network_client.py carol
```

Each client is a real TCP process. The relay persists operations in SQLite,
forwards new operations to currently connected peers, and answers reconnect
requests using the client's per-replica version vector. `disconnect` closes
only the network connection: the client process, local document, and offline
operation queue remain alive. `reconnect` opens a new connection, publishes
queued operations, and requests missing operations using the version vector.
`quit` is the only command that exits the client process.

### Exact offline-edit test

In the `bob` terminal, run:

```text
show
disconnect
insert 0 OFFLINE
show
reconnect
show
```

The second `show` must display `OFFLINE` with status `offline`. The
`reconnect` command must return to `connected`, synchronize with the relay,
and converge with the `alice` and `carol` terminals. If the document contained
`Hello!` before disconnecting, the exact result after reconnection is
`OFFLINEHello!` on all three clients, with identical document hashes. Use
`quit` only when you want to end the client process.

`Client` can edit while disconnected. On reconnect, `Relay.sync` compares the
client's per-replica contiguous sequence summary (a version vector) with its
append-only operation log and sends missing operations. Relay forwarding and
anti-entropy are idempotent, so operations can be delayed, shuffled, and
duplicated. `synchronization_report` measures document hashes, serialized relay
bytes, forwarded operation-payload bytes, tombstones, and per-client metadata
bytes. Per-client metadata is a serialized estimate of CRDT node metadata, not
Python process memory. Forwarded bytes are in-memory relay accounting and are
not a complete TCP wire-traffic measurement; the persistent TCP relay and the
in-memory demo relay expose different instrumentation surfaces.

`test_network_e2e.py` starts the relay on an ephemeral TCP port and verifies
three real socket clients, SQLite persistence, forwarding, offline queued
editing, process-preserving disconnect/reconnect, anti-entropy, equal document
hashes, and reconnect uploads for operations missing from the relay.

The editor façade in `editor.py` provides `insert`, `backspace`, cursor movement,
and rendering. Multi-character `insert INDEX TEXT` commands are submitted as a
causally chained run, so input order is preserved at the beginning, middle, or
end of a document. Causal ordering also ensures a later insertion at a known
position is ordered relative to an existing sibling independently of replica
name; only truly concurrent siblings use the deterministic replica-ID tie-break.
The test suite demonstrates three-client offline editing,
convergence under adversarial delivery, and concurrent insertion/deletion at one
position.

Tombstones are not removed merely because a local client has seen them. A
tombstone is safe to compact only after the caller supplies a labeled summary
for every relevant replica and the minimum of those summaries proves every
replica has acknowledged the delete, no node anchors on it, no pending
operation references it, and replicas only anchor on visible characters. The
API rejects an omitted relevant replica; a permanently offline replica must
remain in the supplied set and its unacknowledged summary blocks removal. The
relay operation log is append-only and never shrinks; compaction or
snapshotting can only be performed at a stable vector and does not rewrite
that relay history.

`demo.py` demonstrates concurrent insertion and deletion at one position,
prints metadata before deletion, after deletion, and after stable compaction,
and reports synchronization bytes. The demo uses the in-memory relay and does
not delete any persistent database. For a fresh TCP relay database, pass a new
path with `--database` or remove the old database deliberately after stopping
all relay/client processes.
