package com.nithish.filetransfer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * INTERVIEW-IMPORTANT: TCP is a byte stream, not a message stream.
 *
 * The OS gives no guarantee that one write() on the sender lines up with
 * one read() on the receiver. A single write can be delivered across
 * several reads, and several small writes can be coalesced into one
 * read. If you assume "one write = one read", it works fine on
 * localhost with small payloads and then silently breaks over a real
 * network or with larger data — a classic bug.
 *
 * The fix is to impose our own message boundaries on top of the byte
 * stream: every message is preceded by a 4-byte big-endian length
 * prefix, and the receiver always reads exactly that many bytes before
 * trying to parse a message. This is "length-prefix framing", and it's
 * why we can't just hand the socket's raw InputStream to
 * TransferMessage.parseFrom() and expect it to know where one message
 * ends and the next begins.
 */
public final class FramingUtils {

    private FramingUtils() {}

    public static void writeFrame(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /**
     * Returns null if the stream ends cleanly between frames (the sender
     * closed the connection after its last message — normal end of
     * transfer). Throws EOFException if the stream ends in the middle of
     * a frame, since that means a message was truncated and something
     * went wrong (e.g. the sender crashed mid-write).
     */
    public static byte[] readFrame(DataInputStream in) throws IOException {
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            return null;
        }
        if (length < 0) {
            throw new IOException("Negative frame length: " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload); // throws EOFException if the stream ends mid-frame
        return payload;
    }
}
