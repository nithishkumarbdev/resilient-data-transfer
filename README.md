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
