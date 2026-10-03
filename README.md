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
