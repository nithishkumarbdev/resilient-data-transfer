# Resilient File Transfer — Phase 1

Java + Protobuf + TCP file sender/receiver, with application-level message
framing, chunking, and checksum verification. This is Phase 1 of a larger
project — no WAL, crash recovery, UDP, HTTP layer, or cloud integration yet.
Those come later.

## What this proves

> A file on the sender side is split into chunks, each one serialized with
> Protobuf, sent over a raw TCP socket using our own message framing, and
> reassembled on the receiver side into a file that is byte-for-byte
> identical to the original — verified by comparing whole-file SHA-256
> checksums.

## Architecture

```text
TransferClient                              TransferServer
     |                                            |
     | read file, split into 64KB chunks          |
     | compute SHA-256 (whole file + per chunk)    |
     |                                             |
     v                                             |
 FileMetadata  ---- TransferMessage (oneof) ---->  |
     |               [4-byte length][protobuf]     v
     |                                        parse, pre-allocate
     |                                        output file
     v                                             |
 FileChunk x N ---- TransferMessage (oneof) ---->  |
     |               [4-byte length][protobuf]     v
     |                                        verify chunk checksum,
     |                                        write at offset
     v                                             |
 (socket closed)                                   v
                                        compare whole-file SHA-256
                                        against metadata.file_checksum
                                        -> INTEGRITY CHECK PASSED/FAILED
```

## Project layout

```text
resilient-file-transfer/
├── pom.xml
├── proto/
│   └── transfer.proto          # FileMetadata, FileChunk, TransferMessage
├── src/main/java/com/nithish/filetransfer/
│   ├── ChecksumUtils.java      # SHA-256 for bytes and for files
│   ├── FramingUtils.java       # length-prefix framing over a socket
│   ├── FileChunker.java        # splits a file into fixed-size chunks
│   ├── TransferClient.java     # sender: reads file -> sends over TCP
│   └── TransferServer.java     # receiver: accepts, reassembles, verifies
└── src/test/java/com/nithish/filetransfer/
    └── TransferIntegrationTest.java
```

`transfer.proto` compiles into generated Java classes under
`target/generated-sources/protobuf/` at build time — those aren't checked
in, Maven regenerates them from the `.proto` file every build.

## Building and running locally

You'll need a JDK 17+ and Maven, with internet access (Maven needs to
download `protoc` and the dependencies the first time).

```bash
cd resilient-file-transfer
mvn compile
```

**Run the tests** (this is the fastest way to see the whole thing work,
including the crash/corruption cases):

```bash
mvn test
```

**Run it manually as two processes**, to actually watch a transfer happen:

Terminal 1 (receiver):
```bash
mvn compile exec:java -Dexec.mainClass="com.nithish.filetransfer.TransferServer" -Dexec.args="9000 /tmp/received 8080 9001"
```

Terminal 2 (sender):
```bash
mvn compile exec:java -Dexec.mainClass="com.nithish.filetransfer.TransferClient" -Dexec.args="localhost 9000 /path/to/some/file.txt"
```

(If `exec:java` isn't available, add the `exec-maven-plugin` to `pom.xml`,
or just run the compiled classes directly with `java -cp target/classes:...`.)

**Expected output**, receiver side:
```text
Receiver listening on port 9000, writing to /tmp/received
Connection from /127.0.0.1:54321
Receiving file.txt (1234 bytes, 1 chunks, transfer_id=...)
Transfer OK: integrity check passed (file.txt)
```

Then confirm manually:
```bash
sha256sum /path/to/some/file.txt /tmp/received/file.txt
```
Both hashes should match.

## What the tests cover

| Test | What it checks |
|---|---|
| `smallTextFile` | basic end-to-end correctness |
| `binaryFile` | non-text bytes survive Protobuf + TCP unchanged |
| `largeMultiChunkFile` | multiple chunks, including a partial final chunk, reassemble correctly |
| `emptyFile` | zero-chunk edge case still completes and verifies |
| `corruptedChunkIsDetected` | a tampered chunk checksum is caught, not silently accepted |
| `endToEndFileChecksumMatches` | the whole-file checksum, not just per-chunk checksums, is the real correctness signal |

## Interview-important concepts in this phase

- **TCP is a byte stream, not a message stream** → why we need length-prefix
  framing (`FramingUtils`).
- **Protobuf `oneof`** → how `TransferMessage` carries two different
  message shapes (metadata vs. chunk) over one stream.
- **Chunking** → why files are split rather than sent whole, and why chunk
  size is a real design decision (64 KB here), not an arbitrary constant.
- **Two different checksums, two different jobs** → per-chunk checksum
  catches wire corruption; whole-file checksum catches everything else
  (wrong offset, dropped chunk, reassembly bug).
- **Pre-allocating the output file with `setLength()` and writing by
  offset, not by append** → this is what will let Phase 3's WAL-based
  resume write chunks that arrive out of order or get retried, without
  corrupting the file.

## Not in Phase 1 (on purpose)

WAL, crash recovery/resume, retry/ACK logic, UDP, the HTTP management
layer, TLS, and CloudWatch. Those are Phases 2–5.

---

## Phase 2 — WAL + Crash Recovery

Extends Phase 1 (unchanged) with durability and resume: a server that can be
killed mid-transfer, restarted, and continue exactly where it left off, with
no corruption and no duplication.

### What's new

- **`proto/transfer.proto`** — added `ResumeRequest`/`ResumeResponse` to the
  network `TransferMessage` oneof, and a separate `WalEntry` oneof
  (`WalTransferStart`, `WalChunkCommitted`) for internal WAL payloads. WAL
  entries are never sent over the network; they're just a convenient,
  already-available serialization for the generic `WalWriter.append(byte[])`.
- **`wal/WalWriter`, `wal/WalReader`, `wal/WalRecord`** — the generic,
  transfer-agnostic durable log from the earlier step (unchanged, except
  `WalWriter` now correctly continues its sequence counter across a restart
  instead of resetting to 0).
- **`TransferState`** — per-transfer in-memory state: which chunks are
  committed, and the resume point (**highest contiguous committed chunk**,
  not the highest chunk number seen — see below).
- **`RecoveryManager`** — replays the WAL at startup and rebuilds the same
  `TransferState` a live server would have accumulated. This *is* the whole
  recovery algorithm; there's no separate repair pass.
- **`TransferServer`** — rewritten to hold transfer state in a
  `Map<transfer_id, TransferState>` that survives across connections and is
  rebuilt from the WAL at startup, to branch on `FileMetadata` vs.
  `ResumeRequest` as the first message on a connection, and to treat a
  chunk as committed only after it's durably written.
- **`TransferClient.sendFileWithResume(...)`** — reconnects on failure, asks
  the server where it actually got to via `ResumeRequest`, and continues
  from the next required chunk. The original `sendFile(...)` is untouched
  and still used by the plain Phase 1 tests.

### Durability boundary — what "committed" actually means

```text
receive FileChunk over TCP
        |
        v
validate transfer_id + checksum
        |
        v
write chunk bytes to destination file at (sequence_number * chunk_size), fsync
        |
        v
append WalChunkCommitted to WAL, fsync   <-- this line returning is the
        |                                    durability boundary
        v
mark chunk committed in TransferState
```

A chunk only counts as committed once its `WalChunkCommitted` record is
durably on disk. Nothing before that line is trusted after a crash.

### Why there's no separate "PREPARED" / "RECEIVED" WAL state

The obvious two-phase design would log a chunk as "received" before writing
it to the destination file, then "committed" after. This project
deliberately uses **one** durable state instead, for a concrete reason: the
recovery strategy always treats "no durable `WalChunkCommitted` record" as
"ask the client to resend this chunk," and rewriting a chunk at its exact
offset is idempotent. So a chunk that crashed mid-file-write behaves
*identically*, from recovery's point of view, to a chunk that was never
sent — both get re-requested and safely reapplied. A two-phase design would
require storing the chunk's raw bytes inside the WAL (to redo the file write
during recovery without the network), which this design avoids entirely.

### Resume handshake

```text
Client reconnects
     |
     v
ResumeRequest { transfer_id }
     |
     v
Server looks up transfer_id in its (WAL-rebuilt) state map
     |
     v
ResumeResponse { last_committed_sequence }
     -1  -> server has no record of this transfer; client restarts from FileMetadata
     N   -> client resumes sending from chunk N+1
```

### Crash recovery, end to end

```text
Server startup
     |
     v
WalReader.replay(wal file)     -- stops cleanly at any truncated/corrupted tail
     |
     v
RecoveryManager rebuilds Map<transfer_id, TransferState>
     |
     v
Server ready -- a ResumeRequest for a known transfer_id now gets a real answer
```

### Real test performed (not simulated)

Ran an actual `kill -9` against a separate server process mid-transfer, in
this environment, and captured the logs:

- Started `TransferServer` as its own OS process.
- Started a resumable client sending a 2,000,000-byte file (31 chunks), with
  a small artificial per-chunk delay so the crash window was reproducible
  rather than a timing race.
- After 6 chunks committed, sent `SIGKILL` (`kill -9`) directly to the
  server process — not a graceful shutdown.
- Restarted the server against the same output directory. It logged:
  `Recovered 1 transfer(s) from WAL: ... 6/31 chunks contiguous-committed`.
- The client's in-flight write failed with `Broken pipe`, retried with
  backoff, got `Connection refused` a few times while the server was down,
  then reconnected once it was back up and received
  `last_committed_sequence=5`.
- It resumed sending from chunk 6 through 30. Final result:
  `Transfer OK: integrity check passed (bigfile.bin)`.
- `sha256sum` of the original and reconstructed file matched exactly.

Also verified directly:
- A fresh server with no existing WAL starts cleanly (no recovered transfers).
- Sending the same chunk three times in a row (simulated retransmission)
  results in one commit and two `Duplicate chunk ignored` log lines — final
  file is the correct size and checksum, not corrupted or tripled.

### Fsync / durability policy (current, simple version)

- **WAL**: `WalWriter.FlushPolicy.EVERY_RECORD` — every WAL append calls
  `FileDescriptor.sync()` before returning. Safest, and simple to reason
  about; the cost is one fsync per chunk, which is fine at this scale.
- **Destination file**: also fsynced after every chunk write, for the same
  reason. This is more conservative than strictly necessary (the file write
  isn't the durability boundary, the WAL append is) but keeps the two
  writes close together in practice and is simple to explain.
- Not implemented: a configurable "fsync every N records / every T ms"
  policy. The reference log-streaming project used one; this project keeps
  a single fixed policy for now and documents the tradeoff instead of
  building the configurability, per the "don't overengineer this phase"
  guidance.

### Known limitations

- Single WAL file, no segment rotation — acceptable at this scale; would
  need rollover if a transfer ran long enough to make one file unwieldy.
- The server handles one connection at a time (the accept loop is
  sequential, not per-connection-threaded) — fine for the resume/crash
  scenario this phase targets, not a concurrent-multi-client server yet.
- Destination file writes and WAL appends are two separate fsyncs, not one
  atomic operation — a crash between them is still handled correctly (see
  the "no PREPARED state" reasoning above), but it's worth being able to
  name that they aren't atomic together if asked.
- No authentication, no encryption, no distributed coordination — single
  node, local filesystem, as expected at this phase.

---

## Phase 3 — File Integrity Monitoring

Adds a File Integrity Monitoring (FIM) component that watches a directory
for unexpected changes, independent of the transfer/WAL system from
Phases 1–2.

### Transfer checksum vs. file integrity monitoring — not the same thing

- **Transfer checksum** (Phases 1–2): "did this chunk/file arrive
  correctly over the network?" Answered once, at transfer completion.
- **File integrity monitoring** (this phase): "has this file changed
  compared to its last known trusted state?" An ongoing question, asked
  every time the filesystem reports a change, for as long as the monitor
  runs.

They share one thing on purpose: both use `ChecksumUtils.sha256Hex(...)`.
There's no second SHA-256 implementation anywhere in this project.

### Architecture

```text
Filesystem (java.nio.file.WatchService)
        |
        v
Change detected (CREATE / MODIFY / DELETE)
        |
        v
Debounce (250ms, coalesces a burst of events for one path into one check)
        |
        v
"Known transfer activity" filter -- suppressed if TransferServer marked this path active
        |
        v
SHA-256 the file, compare against BaselineManager
        |
        v
IntegrityEvent (FILE_CREATED / FILE_MODIFIED / FILE_DELETED)
        |
        v
IntegrityEventStore (append-only log, outputDir/integrity-events.log)
```

### New components

- **`integrity/BaselineManager`** — persisted map of path → {sha256, size,
  last_verified}, stored at `outputDir/integrity-baseline.txt`. Two
  distinct entry points, on purpose: `createBaseline(root, ...)` is the
  explicit "trust whatever's on disk right now" operation; `load(...)` is
  the normal restart path and never implicitly trusts new content.
- **`integrity/IntegrityEventStore`** — append-only event log at
  `outputDir/integrity-events.log`. The audit trail Phase 4's API will
  eventually expose.
- **`integrity/IntegrityEvent`**, **`IntegrityEventType`** — three event
  types (`FILE_CREATED`/`FILE_MODIFIED`/`FILE_DELETED`), not four --
  see the javadoc on `IntegrityEventType` for why a separate
  `INTEGRITY_VIOLATION` type would be redundant here.
- **`integrity/FileIntegrityMonitor`** — the watcher itself: recursive
  `WatchService` registration, debounce, startup reconciliation, and the
  known-transfer-activity suppression described below.
- **`TransferServer`** — gained an optional `FileIntegrityMonitor` field
  (nullable; a plain `new TransferServer(port, dir)` still works exactly
  as before) and calls `markTransferActive`/`markTransferComplete` at the
  right points in the transfer lifecycle.

### How legitimate transfers avoid triggering false violations

This is the part of Phase 3 that actually required design thought, not
just plumbing. `TransferServer` writes to a destination file many times
per transfer (once per chunk). Without an explicit signal, every one of
those writes is indistinguishable from tampering.

```text
handleNewTransfer() creates/pre-allocates the output file
        |
        v
fim.markTransferActive(path)   -- FIM now suppresses checks on this path entirely
        |
        v
... chunks arrive, file is written to repeatedly ...
        |
        v
transfer completes, whole-file SHA-256 verified
        |
        v
fim.markTransferComplete(path, verifiedHash, size)
        |
        +--> removes the suppression
        +--> updates the baseline to the VERIFIED hash directly
             (not re-derived from a filesystem event)
```

If a transfer ends without a verified checksum (failure/incomplete),
`unmarkTransferActive()` removes the suppression without touching the
baseline -- an unverified file never becomes trusted.

A file's baseline is otherwise **never** auto-updated just because a
modification was detected. An unexplained hash change stays flagged
until something explicit says otherwise (a verified transfer, or a
fresh `createBaseline()`). Silently trusting every change would defeat
the point of the feature.

### Reconciliation on startup

`WatchService` events aren't a durable log -- if the monitor wasn't
running, it saw nothing. On `start()`, before watching begins,
`reconcile()` walks the current filesystem against the baseline and
reports anything that drifted while nobody was watching (created,
modified, or deleted). This was proven in practice during the Phase 2
regression re-test: a file left partially-written by a killed server
process was correctly picked up by reconciliation as a new baseline
entry, then correctly suppressed again once the resumed transfer's
writes continued against it.

### Configuration

The monitored root is whatever directory the server was told to write
transfers into -- baseline and event log live alongside it
(`outputDir/integrity-baseline.txt`, `outputDir/integrity-events.log`).
No separate config file; the existing `<port> <outputDir>` CLI arguments
are enough. `transfer.wal` and FIM's own two files are excluded from
monitoring by filename, since they're the system's own operational
state, not transferred content.

### Real tests executed (JUnit 5, run via junit-platform-console-standalone)

All 10 of the required scenarios, plus a full regression run of every
earlier test:

```text
23 tests found, 23 successful, 0 failed
(WalTest: 4, FileIntegrityMonitorTest: 10, ResumeAndRecoveryTest: 3, TransferIntegrationTest: 6)
```

The FIM-specific tests, and what the captured log output actually showed:

- **Baseline creation** — hashes match `ChecksumUtils.sha256Hex` directly.
- **Unchanged file** — zero events generated.
- **Modification** — one `FILE_MODIFIED` event, correct path.
- **Deletion** — one `FILE_DELETED` event, baseline entry removed.
- **New file** — one `FILE_CREATED` event, file added to baseline.
- **Same filename, changed content** — hash comparison catches it; baseline
  correctly still holds the *old* hash until something explicit updates it.
- **Legitimate transfer** (the important one) — ran a real transfer through
  `TransferServer` with FIM attached. Log output showed
  `Known transfer activity: .../legit.bin (FIM checks suppressed until complete)`
  at the start, zero `FILE_MODIFIED` events during any of the 8 chunk
  writes, and `Baseline updated after verified transfer: .../legit.bin`
  at the end -- exactly the intended behavior, not just an assertion that
  happened to pass.
- **Baseline persistence** — a second, independent `BaselineManager`
  instance loading the same file sees the same entries.
- **Event persistence** — a second, independent `IntegrityEventStore`
  instance loading the same file sees the same events.
- **Multiple files, one modified** — exactly one event, for the right file.

Also re-ran the standalone real `kill -9` test from Phase 2, this time
through the actual `TransferServer.main()` entrypoint (which now starts
FIM automatically) to confirm the two systems coexist correctly under a
real crash -- resume completed, final SHA-256 matched, and the log
showed FIM correctly reconciling the partially-written file from the
killed process as a new baseline entry before suppressing it again once
the resumed writes continued.

### Known limitations

- Baseline and event log are plain rewrite-whole-file-on-update / plain
  append text formats -- fine at this scale, would need something more
  structured (or at least indexed) if the monitored directory grew large.
- `WatchService` behavior (especially around editors' save patterns) is
  somewhat OS/filesystem dependent; the 250ms debounce is a simple fixed
  window, not adaptive.
- No cryptographic signing of the baseline or event log -- someone with
  filesystem access to the server could edit `integrity-baseline.txt`
  directly and the monitor would trust it. Acceptable for this phase;
  would matter for a real security-monitoring product.
- Reconciliation is a full directory walk on every startup -- fine at
  this scale, would need to be bounded or incremental for a very large
  monitored tree.

---

## Phase 4 (Part 1–3) — HTTP Management API

Adds a read-only HTTP management/visibility layer. **This is not a second
transfer protocol** -- binary file transfer still only happens over
Protobuf/TCP, exactly as in Phases 1–3. HTTP exists purely so a human or
a script can ask "what's going on" without speaking the wire protocol.

UDP transport and TLS (the rest of Phase 4) are deliberately **not**
included in this pass -- see "Not yet done" below.

### Why a separate layer, not endpoints bolted onto TransferServer

```text
HTTP request
     |
     v
ManagementApiServer (JDK com.sun.net.httpserver.HttpServer -- no framework)
     |
     v
reads directly from the SAME live TransferServer / FileIntegrityMonitor
state the TCP path uses -- no second copy of "API state" to keep in sync
```

`ManagementApiServer` never touches a socket's raw bytes and
`TransferServer`/`FileIntegrityMonitor` have no HTTP-awareness at all --
the boundary is a handful of read-only getters (`getAllTransferStates()`,
`getTransfersStarted()`, `fim.getBaseline()`, `fim.getEventStore()`, ...).

### Endpoints implemented

| Endpoint | Notes |
|---|---|
| `GET /health` | flat `{"status":"ok","uptime_seconds":N}`, matching the spec's own example shape |
| `GET /server` | state, active connections/transfers, and the real observability counters (Part 10) |
| `GET /transfers` | every known transfer, status derived from real committed-chunk counts |
| `GET /transfers/{id}` | 404 for an unknown id; the id segment is validated (see Security) |
| `GET /integrity/events` | FIM's persisted event log; optional `?type=FILE_MODIFIED` filter, 400 for an unrecognized type |
| `GET /integrity/files` | FIM's current baseline (path, sha256, size) |
| `GET /recovery` | recovered-transfer count and current WAL file size |

All other endpoints from the spec (`POST /transfers/{id}/resume`,
`POST /integrity/rescan`, etc.) are **not implemented** -- they're listed
in the spec as optional, and the read-only set above already satisfies
every required acceptance criterion.

### Response format

`{"status":"ok","data": ...}` on success; `{"status":"error","message":"..."}`
with a real status code (400/404) on failure. `/health` is the one
deliberate exception, matching the flat shape the spec itself gave as
its literal example.

### Transfer status -- only states backed by real data

`PENDING` / `TRANSFERRING` / `COMPLETED`, derived directly from
`highestContiguousCommitted()` vs `totalChunks`. No `FAILED` or
`RECOVERING` state: this project doesn't durably track a per-transfer
failure flag, so reporting one would be inventing a state not actually
backed by data. A checksum failure is still real and visible -- it
shows up in `transfers_failed` on `/server`, which IS a genuine counter.

### Observability counters (Part 10)

`transfers_started`, `transfers_completed`, `transfers_failed`,
`bytes_transferred`, `active_connections`, `recovery_count`,
`integrity_events` -- all incremented at the exact code points they
describe (see `TransferServer`'s `AtomicLong`/`AtomicInteger` fields),
never derived or hardcoded. Folded into `/server` rather than a separate
`/metrics` endpoint, since the spec didn't require one and this keeps
the endpoint count smaller.

### Security (Part 8)

- The `transfer_id` path segment is decoded and rejected (400) if it's
  blank, contains `..`, `/`, or `\`, or is implausibly long -- verified
  directly with a URL-encoded traversal attempt
  (`/transfers/..%2F..%2Fetc%2Fpasswd` -> 400).
- The `?type=` filter on `/integrity/events` is validated against the
  real `IntegrityEventType` enum -- an unrecognized value is a 400, not
  a silently-empty result.
- No endpoint accepts a raw filesystem path or exposes file deletion,
  shell execution, or arbitrary file reads -- there's nothing here for a
  path-traversal attack to actually reach.

### Real tests executed

**31/31 JUnit 5 tests**, run three consecutive times to confirm the
result is stable, via `junit-platform-console-standalone`:

```text
WalTest: 4, FileIntegrityMonitorTest: 10, ResumeAndRecoveryTest: 3,
TransferIntegrationTest: 6, ManagementApiServerTest: 8
```

`ManagementApiServerTest` makes real `java.net.http.HttpClient` requests
against a live `ManagementApiServer`+`TransferServer` -- no mocked HTTP
layer -- and covers: health/server/transfers/detail/404/integrity events
(including the type filter and its 400 case)/integrity files/recovery,
plus the path-traversal rejection.

Two real bugs were found and fixed while writing these tests (both in
the tests, not the implementation): a fixed-port assumption that raced
across rapid start/stop cycles (switched to ephemeral ports read back
via new `getPort()` accessors on both `TransferServer` and
`ManagementApiServer`), and two places where an HTTP check ran
immediately after the client's socket closed without waiting for the
server's accept thread to actually finish processing the final frame --
fixed by polling `TransferState.isComplete()` instead of assuming it's
instant.

Also re-ran the real standalone `kill -9` test through the actual
`main()` entrypoint (now `<tcpPort> <outputDir> <httpPort>`) with the
HTTP API live throughout: `curl`'d `/health` and `/server` before the
crash, killed the server process, restarted it, and `curl`'d
`/recovery` and `/transfers` on the **new** process -- both correctly
reflected the WAL-recovered, then resumed and completed, transfer.
Final SHA-256 matched.

### Not yet done (this pass)

- **UDP transport adapter** (Part 4) and the transport abstraction to
  support it (Part 5) -- not implemented yet.
- **TLS/secure TCP** (Part 6) -- not implemented yet.
- Given the scope difference from Phases 1–3, these are being done as
  separate follow-up passes rather than rushed into this one, per the
  spec's own "do not implement everything at once" guidance.

### Known limitations

- `HttpServer`'s default executor (single-threaded) is used -- fine for
  a management API queried occasionally, not load-tested for concurrent
  API traffic.
- No authentication on the API -- anyone who can reach the port can read
  transfer/integrity state. Acceptable for a local management interface
  at this phase; would need addressing before any real deployment.
- `/transfers` and `/integrity/events`/`/integrity/files` return every
  record with no pagination -- fine at this scale, would need it for a
  long-running server with many transfers or a large monitored tree.

---

## Phase 4 (Part 4) — UDP Transport

Adds a second transport, alongside TCP, feeding the exact same
WAL/recovery/FIM/API system. **The claim here is precise: UDP itself is
not reliable; this project implements reliability on top of it at the
application layer.** TCP's transport already guarantees ordered,
lossless delivery; UDP guarantees nothing, so sequence numbers, ACKs,
timeouts, retries, and duplicate handling all had to be built by hand.

### The refactor this required: `TransferProcessor`

Before adding UDP, the "is this chunk safe to keep" logic (duplicate
detection, checksum validation, file write, WAL commit, FIM
integration, completion check) lived entirely inside `TransferServer`.
Adding UDP without duplicating all of that would have meant either a
second copy of the durability logic (explicitly forbidden by this
phase's spec) or UDP silently bypassing WAL/FIM. So that logic was
extracted into a new transport-agnostic `TransferProcessor`:

```text
                    TransferProcessor
                 (WAL, transfers map, FIM,
                  chunk validation & commit)
                    /              \
                   /                \
          TransferServer      UdpTransferServer
             (TCP framing,      (datagram framing,
              length-prefix)     ACK/retry/timeout)
```

One `WalWriter`, one `Map<transfer_id, TransferState>`, one
`FileIntegrityMonitor` instance -- a transfer started over UDP and one
started over TCP are both just entries in the same map, replayed from
the same WAL on restart. `TransferServer`'s public constructors and
getters are unchanged (backward compatible with every earlier test);
internally it now just delegates to a `TransferProcessor` it owns, and
exposes it via `getProcessor()` so `UdpTransferServer` can share it.

### Why UDP needed its own message design (mostly reused, one real addition)

`FileMetadata` and `FileChunk` are reused as-is over UDP -- no second
schema. Two additions were genuinely necessary:

- **`chunk_size` on `FileMetadata`** (and on the internal
  `WalTransferStart` WAL entry). TCP and UDP deliberately use
  *different* chunk sizes (below), and a chunk's file offset is
  `sequence_number * chunk_size` -- that formula silently breaks if the
  chunk size isn't the one this specific transfer actually declared.
  This used to be a hardcoded constant; it's now per-transfer state.
- **`UdpAck`**, added as one more branch of the existing
  `TransferMessage` oneof -- not a new top-level protocol. TCP never
  needed an application-level ack (the transport already guarantees
  delivery); UDP does, for literally everything.

A UDP datagram carries exactly one `TransferMessage` with no framing
needed at all -- unlike TCP, UDP already preserves message boundaries
per datagram, so `FramingUtils`' length-prefix scheme is a TCP-only
concern.

### Chunk size: 1024 bytes, not TCP's 64 KB

A 64 KB UDP datagram would almost certainly fragment at the IP layer,
and if any one fragment is lost, the whole datagram is gone with no
partial-delivery signal -- exactly the kind of hidden reliance on
fragmentation this phase's spec explicitly warns against.
`UdpTransferClient.UDP_CHUNK_SIZE = 1024` keeps a whole chunk, plus its
protobuf/field overhead, safely under a typical 1500-byte Ethernet MTU.

### Reliability: stop-and-wait, deliberately simple

```text
send DATA(seq=N)
      |
      v
wait up to timeoutMillis for ACK(seq=N)
      |
   +--+--+
   |     |
  yes    no (timeout)
   |     |
   v     v
 send   retry (bounded by maxRetries)
 N+1     |
         v
   retries exhausted -> IOException, transfer fails
```

One outstanding chunk at a time, not a sliding window -- the spec
explicitly asked for correctness and explainability over throughput
here. An ACK carrying `sequence_number = -1` specifically acknowledges
`FileMetadata` (i.e. "transfer registered, send chunk 0").

### Duplicate handling -- the same mechanism that makes a lost ACK self-heal

A duplicate arrives whenever a client's ACK was lost and it retried a
chunk the server already committed. `TransferProcessor.processChunk`
already had exactly the check needed (`state.isCommitted(seq)`, shared
with TCP's resume logic) -- UDP just re-sends the ACK for an already-committed
chunk instead of treating it as an error. This was verified directly: a
chunk sent twice results in one WAL commit, two "duplicate chunk
ignored" log lines, and a file that is not corrupted or doubled in size.

### Out-of-order handling -- inherited "for free" from Phase 2

Chunks are written by seek+offset, not by append, and completion is
tracked via `highestContiguousCommitted()` -- the exact same properties
that make TCP resume safe already make UDP's chunk processing
order-independent. Chunks 0, 2, 1 (sent deliberately out of sequence,
bypassing the client's normal stop-and-wait discipline) were verified
to assemble into the correct file. No separate out-of-order buffering
logic was needed or written.

### WAL integration -- no second durability system

`UdpTransferServer.handlePacket` calls the identical
`TransferProcessor.processChunk`/`finalizeIfComplete` methods TCP uses.
The durability boundary is unchanged: a chunk is only ACKed after its
bytes are fsynced to the file AND its `WalChunkCommitted` record is
fsynced to the WAL. Verified directly: a UDP transfer interrupted after
2 of 3 chunks, followed by a full `TransferProcessor`/`UdpTransferServer`
restart against the same output directory, correctly recovered
`transport=UDP`, `chunk_size=10` (the test's small value), and
`highestContiguousCommitted() == 1` from the WAL alone -- then accepted
the remaining chunk and completed correctly.

### HTTP API integration -- real field, not faked

`TransferState` now carries a real `transport` field ("TCP" or "UDP"),
set once at registration from the value the transport layer actually
passed in, persisted through the WAL, and surfaced as
`"transport":"TCP"` / `"transport":"UDP"` in `/transfers` and
`/transfers/{id}` -- verified with a real transfer sent over UDP,
queried through the same `ManagementApiServer` TCP transfers use.

### Real tests executed

**39/39 JUnit 5 tests, run three consecutive full-suite times, zero
failures** (`UdpTransferTest` adds 8: normal transfer, multi-chunk
assembly, checksum-mismatch rejection with correct resend, duplicate/
lost-ack self-heal, timeout+bounded-retry-exhaustion with an exact
retry-count assertion, out-of-order assembly, WAL recovery after a
restart, and the API transport-field check).

A real, previously-flaky pre-existing bug was found and fixed along
the way (unrelated to UDP itself): `FileIntegrityMonitor` recorded an
event *before* updating the baseline in four places, creating a real
TOCTOU race for any reader polling the event log and then immediately
checking the baseline. Reordered to update the baseline first in all
four sites; the previously ~25%-flaky `deletedFile_isDetected` test
passed 8/8 in isolation afterward, and 3/3 full-suite runs stayed clean.

Also ran, for real, outside JUnit: a UDP transfer through the actual
`main()` entrypoint, a `kill -9` of the TCP server mid-transfer with
restart and resume, and a `curl /transfers` on the **restarted** server
showing both the UDP-completed and the crash-recovered TCP-completed
transfer with correct `transport` fields and matching SHA-256 checksums.

### Known limitations

- Stop-and-wait only -- one outstanding datagram at a time. A sliding
  window would improve throughput but was explicitly out of scope for
  this pass.
- No congestion control -- a real concern at scale, not attempted here.
- `UdpTransferServer` processes packets on a single thread, matching
  the existing TCP server's one-connection-at-a-time model; concurrent
  multi-client UDP traffic isn't load-tested.
- No performance benchmarking was done in this pass -- the spec is
  explicit that correctness comes before throughput measurement, and
  this pass stopped at correctness.

### Still not done

TLS/secure TCP (the remaining piece of Phase 4) -- not implemented in
this pass either.

---

## Phase 4 (Part 5) — TLS-Secured TCP

Adds an optional TLS-secured variant of the existing TCP transport. Plain
TCP is unchanged and still available — **TLS is additive, not a
replacement.** The claim here is precise and bounded: standard JDK TLS
(`javax.net.ssl`) protecting the existing TCP/Protobuf transfer protocol,
nothing more. No custom cryptography, no trust-all shortcuts, no
production PKI claims.

### Architecture: TLS is just a different kind of socket

```text
Transfer Manager (TransferProcessor)
         |
      Transport
         |
    +----+----+
    |         |
   TCP       UDP
    |
 +--+--+
 |     |
plain TLS
Socket SSLSocket
```

`SSLServerSocket.accept()` returns an `SSLSocket`, and `SSLSocket extends
Socket`. That single fact is why nothing above the socket layer needed to
change: `TransferServer.handleConnection(Socket socket)` is used
completely unchanged for both plain and TLS connections — it has no idea
which one it's holding. Chunking, Protobuf serialization, WAL commits,
resume state, SHA-256 verification, and FIM integration are all
byte-for-byte the same code path either way.

### What's new

- **`tls/TlsContextFactory`** — builds `SSLContext` from a real PKCS12
  keystore (server) or truststore (client) using standard
  `KeyManagerFactory`/`TrustManagerFactory` APIs. No custom
  `TrustManager`, no `HostnameVerifier` that always returns true —
  every rejection test in `TlsTransferTest` would pass for the wrong
  reason if either existed.
- **`TransferServer.startTls(tlsPort, sslContext)`** — an additive
  second listener, bound to its own port (TLS and plain TCP are never
  multiplexed on one port). Explicitly calls `startHandshake()` right
  after `accept()`, in its own try/catch, so one client's bad handshake
  is logged and dropped without affecting the accept loop or any other
  connection.
- **`TransferClient.sendFileWithResumeTls(...)`** — the *exact same*
  resume/retry method as plain TCP, refactored to take a `Socket`-creation
  strategy instead of hardcoding `new Socket(...)`. The TLS variant
  additionally sets `SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")`
  before the handshake — **not on by default** for `SSLSocket`, and the
  actual check that validates the certificate's identity against the
  hostname connected to, not just that it chains to a trusted root.
- **`generate-dev-certs.sh`** — reproducible `keytool` commands for a
  local self-signed dev certificate. Never commit the `.p12` files it
  produces; run it locally, or let `TestCertGenerator` (test-only) shell
  out to real `keytool` per test run.

### Configuration: environment variables only

```text
TLS_ENABLED=true
TLS_PORT=9443
TLS_KEYSTORE_PATH=/path/to/server-keystore.p12
TLS_KEYSTORE_PASSWORD=changeit
```

Never CLI arguments, never hardcoded — a keystore password in a CLI arg
ends up in shell history and `ps` output; in Java source it ends up in
version control. `main()` only enables TLS when `TLS_ENABLED=true`; a
plain-TCP-only server run is unaffected by any of this.

### Certificate setup (reproducible, no secrets committed)

```bash
./generate-dev-certs.sh ./certs
# writes ./certs/server-keystore.p12 and ./certs/client-truststore.p12,
# both dev-only, both .gitignore'd -- never commit them

TLS_ENABLED=true TLS_PORT=9443 \
  TLS_KEYSTORE_PATH=./certs/server-keystore.p12 TLS_KEYSTORE_PASSWORD=changeit \
  java -cp target/classes:... com.nithish.filetransfer.TransferServer 9000 /tmp/received 8080 9001
```

The client needs the matching truststore (`TlsContextFactory.clientContext(...)`)
built from the exact same certificate.

### TLS protocol actually negotiated (measured, not assumed)

Verified directly against a running server:

```text
Negotiated protocol: TLSv1.3
Negotiated cipher suite: TLS_AES_256_GCM_SHA384
```

`TlsContextFactory.PROTOCOL = "TLSv1.3"` requests this explicitly rather
than hand-picking cipher suites — the JDK's own secure defaults choose
the cipher.

### Certificate validation — no shortcuts, verified by making it fail correctly

Two *different* rejection paths were tested, on purpose, because they
protect against different things:

- **Untrusted certificate**: a client with an empty truststore (trusts
  nothing) fails the handshake with a real `SSLException` in the cause
  chain. The server logs the failed handshake and stays healthy — a
  *different* client with the correct truststore was verified to
  succeed immediately afterward, on the same server, same JVM run.
- **Hostname/identity mismatch**: a server presenting a certificate for
  `wronghost.example` (not `localhost`), with a client that trusts that
  *exact* certificate (so the trust chain itself is fine) but connects
  to `"localhost"` — endpoint identification correctly rejects this on
  the identity mismatch specifically, not the trust chain. This is the
  check `SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")`
  turns on; without it, this connection would have silently succeeded.

### A real bug this phase's testing found — in Phase 2's resume protocol, not TLS

A real end-to-end `kill -9` test against a TLS-enabled server (killing it
right as the client's metadata write returned locally but before the
server had necessarily processed it) surfaced a genuine, previously
untested bug: when a `ResumeRequest` comes in for a transfer_id the
server has never seen, `handleConnection` was closing the connection
immediately after sending back `last_committed_sequence = -1` — but
`TransferClient`'s resume logic is specifically designed to respond to
that `-1` by sending fresh `FileMetadata` on the *same* connection. Every
earlier resume/recovery test happened to always hit the "known transfer,
resume from N" branch; this exact "-1, unknown, continue on the same
connection" branch had never been exercised before. Fixed by having
`handleConnection` keep reading from the connection in that case instead
of returning immediately, with a new deterministic regression test
(`ResumeAndRecoveryTest.resumeWithUnknownTransferId_continuesOnSameConnection`)
added specifically for this path. Re-ran the exact real `kill -9`
scenario that found it afterward — resumed and completed correctly, SHA-256
matched.

### Real tests executed

**50/50 JUnit 5 tests, 10 consecutive full-suite runs with zero
failures** after the fixes above (`TlsTransferTest` adds 10: normal
transfer, plain-TCP-alongside-TLS, multi-chunk, resume-after-restart,
untrusted-certificate rejection + server-stays-healthy-afterward,
hostname-mismatch rejection, wrong-password config error, missing-file
config error, HTTP API coexistence, UDP regression).

Also ran, for real, outside JUnit: the actual `generate-dev-certs.sh`
script end-to-end; a real `kill -9` of a TLS-enabled server via the
actual `main()` entrypoint with environment-variable configuration,
resume, and a final SHA-256 match; and a direct protocol/cipher-suite
check against a live handshake.

### Known limitations

- Server-authenticated TLS only — no mutual TLS (client certificates).
  Explicitly out of scope for this pass per the spec's own "don't add
  mTLS just to increase the feature count" guidance.
- No user authentication/authorization above the transport layer, no
  production PKI (self-signed dev certs only), no certificate rotation,
  no distributed identity management.
- No plain-TCP-vs-TLS throughput comparison was measured in this pass —
  the spec explicitly deprioritizes this, and no benchmark numbers are
  reported here since none were actually run.
- Test-suite timing (`waitUntil` polling windows, retry budgets) was
  tuned empirically against this sandbox's behavior under a full
  50-test concurrent run, not against a fixed SLA — a real CI runner
  with different resource characteristics may need different margins.

---

## Phase 5 — AWS Deployment, IAM, CloudWatch Observability

**Read this section's structure before the content**: every claim below is
labeled either ✅ **VERIFIED LOCALLY** (actually run, with real output
shown) or ⬜ **NOT EXECUTED — requires a real AWS account** (written,
locally validated for syntax/structure where possible, never applied or
tested against real AWS). This project's development environment has
**no AWS credentials, no AWS CLI, and its network policy explicitly
blocks `amazonaws.com`** — confirmed directly, not assumed. Nothing in
this section claims otherwise.

### Architecture: AWS wraps the existing application, doesn't replace it

```text
                     Remote Client
                         |
             +-----------+-----------+
             |           |           |
            HTTP        TLS          UDP
             |           |           |
             v           v           v
                 +----------------+
                 |      EC2       |
                 | Java Server    |   <- same Phases 1-4 code, unmodified
                 +-------+--------+      except the metrics/logging hooks below
                         |
              +----------+----------+
              |                     |
              v                     v
             WAL                   FIM
              |                     |
              v                     v
         File Storage         Integrity Events
              |
              +----------------------+
                                     |
                                     v
                              CloudWatch Agent (host) + MetricsPublisher (app)
                                     |
                       +-------------+-------------+
                       |             |             |
                       v             v             v
                     Logs         Metrics        Alarms
```

### ✅ VERIFIED LOCALLY — the application-side observability code

**`MetricsPublisher` interface** (`metrics/MetricsPublisher.java`) — every
metric call in the application goes through this. Three implementations:
- `LoggingMetricsPublisher` — the local-mode default, logs every metric via
  `AppLog`, makes zero network calls.
- `CloudWatchMetricsPublisher` — real AWS SDK v2 code (see the "NOT
  COMPILED OR TESTED" banner in its own javadoc — this one file genuinely
  could not be verified in this environment; see below).
- `MetricsPublisherFactory` — picks based on `CLOUDWATCH_ENABLED`, loading
  `CloudWatchMetricsPublisher` via **reflection** specifically so the rest
  of the application has zero compile-time dependency on the AWS SDK.

**Real counters, not invented ones** — `TransferProcessor`, `TransferServer`,
and `FileIntegrityMonitor` call the publisher at the exact point each
counter already changes: `TransfersStarted`/`TransfersCompleted`/
`TransfersFailed`/`BytesTransferred` (already existed from Phase 4's HTTP
API), plus three genuinely new ones this phase added: `RecoveredTransfers`
and `WALRecoveryDurationMs` (measured from the real WAL replay in
`TransferProcessor.initialize()`), `TransferDurationMs` (measured from each
transfer's real start to its real completion), `ActiveConnections` (gauged
on every connect/disconnect), and `IntegrityEvents` (incremented from FIM's
real `recordEvent`, with the real event type as a dimension).

**`AppLog`** (`logging/AppLog.java`) — structured, timestamped,
`key=value` lines to stdout always, and to a file when `APP_LOG_PATH` is
set (what the CloudWatch Agent config below tails). Wired into: server
start, API start, transfer started/completed/failed, WAL recovery, TLS
handshake failures, FIM file created/deleted/integrity-violation events.

**Test results**: **59/59 real JUnit 5 tests, 3 consecutive full-suite
runs, zero failures** (9 new: `MetricsIntegrationTest` — 4 tests proving
real events drive real metrics (a real transfer, a real checksum failure,
a real restart, a real FIM violation — not synthetic calls);
`MetricsPublisherFactoryTest` — 2 tests against this environment's actual
unset `CLOUDWATCH_ENABLED`; `AppLogTest` — 3 tests for format/never-throws).

**The resilience property Part 32 requires — demonstrated for real, not
just asserted**: ran the actual server with `CLOUDWATCH_ENABLED=true` in
this environment, where the AWS SDK genuinely isn't on the classpath.
Captured output:
```text
WARN metrics_publisher_fallback reason=java.lang.ClassNotFoundException: com.nithish.filetransfer.metrics.CloudWatchMetricsPublisher
INFO transfer_started transfer_id=... filename=f5.bin total_size=400000 total_chunks=7 transport=TCP
INFO transfer_completed transfer_id=... filename=f5.bin duration_ms=187 transport=TCP
```
The transfer completed successfully and the SHA-256 matched, despite
"CloudWatch" being completely unavailable — exactly the required
behavior, not a simulation of it. Also confirmed the `APP_LOG_PATH` file
received the identical structured log lines in real time.

**Local mode with CloudWatch disabled** — the default state of every one
of the 59 tests above, and of every prior phase's test. The application
has never, in this entire project, required AWS to be reachable to work.

### ⬜ NOT EXECUTED — requires a real AWS account

Everything below was written carefully and validated where a local tool
exists for that (shown per item), but **no AWS resource was created, no
metric was actually delivered to CloudWatch, no dashboard or alarm was
observed firing, and no EC2 instance ran this application.**

| Artifact | What it is | Local validation performed |
|---|---|---|
| `terraform/*.tf` | EC2 instance, security group, IAM role + instance profile + policy, CloudWatch log group, dashboard, 4 alarms, SNS topic | Parsed with `terraform-config-inspect` — all 12 resources, 6 outputs, 12 variables recognized, zero diagnostics/errors. **`terraform plan`/`apply` never run** — no AWS credentials to run them with. |
| `iam/trust-policy.json`, `iam/policy.json` | EC2 assume-role trust policy; least-privilege policy (`cloudwatch:PutMetricData` scoped by namespace condition, `logs:*` scoped to one log group ARN) | Valid JSON (`jq`). **Never attached to a real role or tested against the real CloudWatch/Logs APIs.** |
| `deploy/protobuf-transfer.service` | systemd unit — runs as a dedicated non-root user, `Restart=on-failure`, `ProtectSystem=strict` | Hand-reviewed against systemd unit syntax. **Never installed or started on a real host.** |
| `deploy/cloudwatch-agent-config.json` | CloudWatch Agent config — CPU/mem/disk/diskio/net host metrics, one log file collected | Valid JSON (`jq`). **Never loaded by a real agent.** |
| `deploy/deploy.sh` | End-to-end deployment script (Java install → service user → directories → copy app → optional agent → systemd → health check) | `bash -n` syntax check passed. **Never run against a real instance.** |
| `deploy/app.env.template` | Environment config template for the systemd `EnvironmentFile` | N/A (plain template). |
| `CloudWatchMetricsPublisher.java` | Real `PutMetricData` calls via AWS SDK v2 | **Not compiled** — no SDK jar available in this sandbox at all (checked directly: not in apt, Maven Central unreachable). Written from documented API knowledge; run `mvn compile` yourself first thing after pulling this. |

**Before trusting any of the above in a real deployment**: run
`terraform plan` (review every resource it intends to create), run
`mvn compile` (confirm `CloudWatchMetricsPublisher` actually compiles
against the real SDK version in `pom.xml`), and work through Parts
23-30 of the original Phase 5 spec (real logging/metrics/alarm tests,
remote TLS/UDP transfers, a real crash/recovery test, and a reboot
test) against the instance Terraform creates — none of that is
something this environment could do for you.

### Decisions worth knowing about

- **Parameter Store was not integrated.** The existing environment-variable
  configuration (already implemented since Phase 4, tested throughout)
  was judged sufficient for a single-instance deployment. Documented here
  as a deliberate scope decision, not an oversight — Parameter Store would
  be a reasonable addition if this grew to multiple instances needing
  centrally-managed config.
- **S3/EFS were not introduced.** Local EBS storage (`gp3`, encrypted)
  backs the WAL/file data, per the spec's explicit "don't add S3 just
  because this is AWS" guidance. This is instance-attached storage, not
  distributed storage — documented as a real limitation, not glossed over.
- **Reflection, not a feature flag, gates `CloudWatchMetricsPublisher`.**
  This was a direct consequence of this environment lacking the AWS SDK,
  but it turned into the actual correct design for Part 32's resilience
  requirement — see `MetricsPublisherFactory`'s javadoc.

### Known limitations

- No mutual TLS, no Parameter Store, no multi-instance/auto-scaling —
  all explicitly out of scope per the spec.
- Alarm thresholds (80% CPU, 85% disk, any transfer failure, any
  integrity violation) are demo values, not derived from load testing.
  Stated plainly in `terraform/alarms.tf`'s own comments, not just here.
- `CloudWatchMetricsPublisher.java` is unverified code — real, written
  carefully against the documented SDK v2 API, but genuinely untested
  by anyone, including this session.
