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
mvn compile exec:java -Dexec.mainClass="com.nithish.filetransfer.TransferServer" -Dexec.args="9000 /tmp/received"
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
