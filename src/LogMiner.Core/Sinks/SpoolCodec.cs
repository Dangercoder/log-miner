using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;

namespace LogMiner.Sinks;

/// <summary>CRC-32C (Castagnoli) using the hardware instruction where available.</summary>
public static class Crc32C
{
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        var longs = MemoryMarshal.Cast<byte, ulong>(data);
        foreach (var l in longs) crc = BitOperations.Crc32C(crc, l);
        foreach (var b in data[(longs.Length * 8)..]) crc = BitOperations.Crc32C(crc, b);
        return ~crc;
    }
}

/// <summary>A message as stored in the spool, with its spool sequence number.</summary>
/// <param name="Seq">Sequence number, contiguous.</param>
/// <param name="AppendedUnixMs">When it was handed over.</param>
/// <param name="Message">The message.</param>
/// <param name="Batch">Sequence number of the first record of the same group commit: a later batch is only written once this one is on disk (0: not known).</param>
public sealed record SpoolRecord(long Seq, long AppendedUnixMs, SinkMessage Message, long Batch = 0);

/// <summary>
/// Spool record framing (little endian):
/// <code>
/// [magic u32 'LMS2'][bodyLength u32][crc32c(body) u32][body]          ('LMS1': without batch)
/// body = seq i64, appendedUnixMs i64, batch i64, flags u8, database str?, (stream str, commitLsn 10B, sequence i32)?, destination str, key str,
///        messageId str, contentType str?, headerCount u16, (name str, value str)*, payloadLength i32, payload
/// str  = i32 byte length (-1 = null) + UTF-8
/// </code>
/// A record is valid only if magic, length bounds and CRC all check out; the first invalid record ends the valid data.
/// </summary>
public static class SpoolCodec
{
    public const uint Magic = 0x32534D4C; // "LMS2"
    private const uint MagicV1 = 0x31534D4C; // "LMS1"
    public const int HeaderSize = 12;
    public const int MaxBodySize = 64 * 1024 * 1024;
    private const byte HasPosition = 1;

    /// <summary>Why the message cannot be spooled (it could not be read back), or null.</summary>
    public static string? Problem(SinkMessage m)
    {
        var size = 3 * (m.Destination.Length + m.Key.Length + m.MessageId.Length + (long)(m.Stream?.Length ?? 0) + (m.Database?.Length ?? 0) + (m.ContentType?.Length ?? 0)) + m.Payload.Length + 256L;
        foreach (var (k, v) in m.Headers) size += 8 + 3L * (k.Length + v.Length);
        return size > MaxBodySize ? $"message {m.MessageId} is larger than a spool record can be ({m.Payload.Length} bytes)"
             : m.Headers.Count > ushort.MaxValue ? $"message {m.MessageId} has too many headers" : null;
    }

    public static void Write(SpoolRecord r, IBufferWriter<byte> output)
    {
        if (Problem(r.Message) is { } why) throw new ArgumentException(why, nameof(r));
        var body = new ArrayBufferWriter<byte>(r.Message.Payload.Length + 256);
        var m = r.Message;
        WriteI64(body, r.Seq);
        WriteI64(body, r.AppendedUnixMs);
        WriteI64(body, r.Batch);
        var flags = m.Stream is not null && m.Position is not null ? HasPosition : (byte)0;
        body.GetSpan(1)[0] = flags; body.Advance(1);
        WriteStr(body, m.Database);
        if (flags == HasPosition)
        {
            WriteStr(body, m.Stream);
            var span = body.GetSpan(Lsn.BinarySize);
            m.Position!.Value.CommitLsn.WriteBinary(span);
            body.Advance(Lsn.BinarySize);
            WriteI32(body, m.Position.Value.Sequence);
        }
        WriteStr(body, m.Destination);
        WriteStr(body, m.Key);
        WriteStr(body, m.MessageId);
        WriteStr(body, m.ContentType);
        var hs = body.GetSpan(2); BinaryPrimitives.WriteUInt16LittleEndian(hs, (ushort)m.Headers.Count); body.Advance(2);
        foreach (var (k, v) in m.Headers) { WriteStr(body, k); WriteStr(body, v); }
        WriteI32(body, m.Payload.Length);
        body.Write(m.Payload.Span);

        var header = output.GetSpan(HeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(header[4..], (uint)body.WrittenCount);
        BinaryPrimitives.WriteUInt32LittleEndian(header[8..], Crc32C.Compute(body.WrittenSpan));
        output.Advance(HeaderSize);
        output.Write(body.WrittenSpan);
    }

    /// <summary>Decodes one record from the start of <paramref name="data"/>. Returns false (record = null) when the data
    /// is incomplete or invalid; <paramref name="consumed"/> is the record's total size when valid.</summary>
    public static bool TryRead(ReadOnlySpan<byte> data, out SpoolRecord? record, out int consumed)
    {
        record = null;
        consumed = 0;
        if (data.Length < HeaderSize) return false;
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(data);
        if (magic != Magic && magic != MagicV1) return false;
        var length = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        if (length < 17 || length > MaxBodySize || HeaderSize + length > data.Length) return false;
        var body = data.Slice(HeaderSize, (int)length);
        if (Crc32C.Compute(body) != BinaryPrimitives.ReadUInt32LittleEndian(data[8..])) return false;
        try
        {
            var p = 0;
            var seq = ReadI64(body, ref p);
            var appended = ReadI64(body, ref p);
            var batch = magic == Magic ? ReadI64(body, ref p) : 0;
            var flags = body[p++];
            string? stream = null;
            SourcePosition? pos = null;
            var db = ReadStr(body, ref p);
            if ((flags & HasPosition) != 0)
            {
                stream = ReadStr(body, ref p);
                var lsn = Lsn.FromBinary(body.Slice(p, Lsn.BinarySize)); p += Lsn.BinarySize;
                pos = new SourcePosition(lsn, ReadI32(body, ref p));
            }
            var destination = ReadStr(body, ref p) ?? "";
            var key = ReadStr(body, ref p) ?? "";
            var id = ReadStr(body, ref p) ?? "";
            var contentType = ReadStr(body, ref p);
            int headerCount = BinaryPrimitives.ReadUInt16LittleEndian(body[p..]); p += 2;
            var headers = new KeyValuePair<string, string>[headerCount];
            for (var i = 0; i < headerCount; i++) headers[i] = new(ReadStr(body, ref p) ?? "", ReadStr(body, ref p) ?? "");
            var payloadLength = ReadI32(body, ref p);
            if (payloadLength < 0 || p + payloadLength != body.Length) return false;
            var payload = body.Slice(p, payloadLength).ToArray();
            record = new SpoolRecord(seq, appended, new SinkMessage
            {
                Database = db, Stream = stream, Position = pos, Destination = destination, Key = key, MessageId = id, ContentType = contentType,
                Headers = headers, Payload = payload,
            }, batch);
            consumed = HeaderSize + (int)length;
            return true;
        }
        catch (ArgumentOutOfRangeException) { return false; }
        catch (IndexOutOfRangeException) { return false; }
    }

    private static void WriteI64(ArrayBufferWriter<byte> w, long v) { BinaryPrimitives.WriteInt64LittleEndian(w.GetSpan(8), v); w.Advance(8); }
    private static void WriteI32(ArrayBufferWriter<byte> w, int v) { BinaryPrimitives.WriteInt32LittleEndian(w.GetSpan(4), v); w.Advance(4); }

    private static void WriteStr(ArrayBufferWriter<byte> w, string? s)
    {
        if (s is null) { WriteI32(w, -1); return; }
        var n = Encoding.UTF8.GetByteCount(s);
        WriteI32(w, n);
        Encoding.UTF8.GetBytes(s, w.GetSpan(n));
        w.Advance(n);
    }

    private static long ReadI64(ReadOnlySpan<byte> b, ref int p) { var v = BinaryPrimitives.ReadInt64LittleEndian(b[p..]); p += 8; return v; }
    private static int ReadI32(ReadOnlySpan<byte> b, ref int p) { var v = BinaryPrimitives.ReadInt32LittleEndian(b[p..]); p += 4; return v; }

    private static string? ReadStr(ReadOnlySpan<byte> b, ref int p)
    {
        var n = ReadI32(b, ref p);
        if (n < 0) return null;
        var s = Encoding.UTF8.GetString(b.Slice(p, n));
        p += n;
        return s;
    }
}
