using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using LogMiner.Rows;
using LogMiner.Schema;

namespace LogMiner.Events;

public enum ChangeOp : byte
{
    Create, Update, Delete,
    /// <summary>A row of a historical load.</summary>
    Read,
    /// <summary>Markers around the rows of a historical load. Every row of the table is published between them, or was changed by a transaction whose
    /// events are: a consumer that knows of other rows at the end has rows the table does not have.</summary>
    LoadBegin, LoadEnd,
}

/// <summary>One row change (or snapshot row) ready to publish.</summary>
public sealed class ChangeEvent
{
    public required TableLayout Table { get; init; }
    public required ChangeOp Op { get; init; }
    public RowImage? Before { get; init; }
    public RowImage? After { get; init; }
    /// <summary>LSN of the row's log record (snapshot rows: the high watermark of their chunk).</summary>
    public required Lsn Lsn { get; init; }
    public required Lsn CommitLsn { get; init; }
    public Lsn BeginLsn { get; init; }
    public DateTime? CommitTime { get; init; }
    public ulong TransactionId { get; init; }
    /// <summary>Position of this change within its transaction (or snapshot chunk): ascending from 0, not contiguous when the transaction also
    /// changed replicated rows that are not captured.</summary>
    public required int Sequence { get; init; }
    /// <summary>Events of the transaction.</summary>
    public int TransactionSize { get; init; }
    /// <summary>Columns whose value was fetched from the table after the fact (off-row LOB / truncated / partial update).</summary>
    public IReadOnlyList<string>? LookedUp { get; init; }
    public string? SnapshotId { get; init; }
    /// <summary>Rows of the load published (at its end).</summary>
    public long LoadRows { get; init; }
    /// <summary>Database incarnation (recovery fork + creation time): LSNs are only comparable within one incarnation.</summary>
    public string Incarnation { get; init; } = "";

    /// <summary>The same change with other images.</summary>
    public ChangeEvent With(RowImage? before, RowImage? after) => new()
    {
        Table = Table, Op = Op, Before = before, After = after, Lsn = Lsn, CommitLsn = CommitLsn, BeginLsn = BeginLsn, CommitTime = CommitTime, TransactionId = TransactionId,
        Sequence = Sequence, TransactionSize = TransactionSize, LookedUp = LookedUp, SnapshotId = SnapshotId, SnapshotRead = SnapshotRead, Incarnation = Incarnation, LoadRows = LoadRows,
    };

    /// <summary>The reading of a chunk this row came from: every reading is different, also of a load that starts over after a restart (its
    /// rows may be others, at the same positions).</summary>
    public string? SnapshotRead { get; init; }

    /// <summary>
    /// Deterministic id (destination de-duplication): identical when the same change is replayed, and never that of another change - of another
    /// server, database, or incarnation of it. Names are escaped: ASCII only, and no separator in them.
    /// </summary>
    public string MessageId(string server)
    {
        var source = $"{Uri.EscapeDataString(server)}:{Uri.EscapeDataString(Table.Database)}:{IncarnationId(Incarnation)}";
        return SnapshotId is null
            ? string.Create(CultureInfo.InvariantCulture, $"{source}:{CommitLsn}:{Sequence}")
            : string.Create(CultureInfo.InvariantCulture, $"{source}:snap:{SnapshotRead}:{Op switch { ChangeOp.LoadBegin => "begin", ChangeOp.LoadEnd => "end", _ => Sequence.ToString(CultureInfo.InvariantCulture) }}");
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> IncarnationIds = new(StringComparer.Ordinal);

    /// <summary>Short name of an incarnation, over all of it.</summary>
    public static string IncarnationId(string incarnation) =>
        IncarnationIds.GetOrAdd(incarnation, static i => Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(i)).AsSpan(0, 6)));
}

public static class ChangeEventJson
{
    private static readonly JsonWriterOptions Options = new() { Indented = false, SkipValidation = true };

    /// <summary>Writes the event envelope as UTF-8 JSON into <paramref name="buffer"/>.</summary>
    public static void Write(ChangeEvent e, string server, IBufferWriter<byte> buffer)
    {
        using var w = new Utf8JsonWriter(buffer, Options);
        w.WriteStartObject();
        w.WriteString("op"u8, e.Op switch { ChangeOp.Create => "c", ChangeOp.Update => "u", ChangeOp.Delete => "d", ChangeOp.Read => "r", _ => "l" });
        w.WritePropertyName("key"u8);
        WriteKey(w, e);
        w.WritePropertyName("before"u8);
        WriteImage(w, e.Before);
        w.WritePropertyName("after"u8);
        WriteImage(w, e.After);
        if (e.Op is ChangeOp.LoadBegin or ChangeOp.LoadEnd)
        {
            w.WriteStartObject("load"u8);
            w.WriteString("id"u8, e.SnapshotId);
            w.WriteString("phase"u8, e.Op == ChangeOp.LoadBegin ? "begin" : "end");
            if (e.Op == ChangeOp.LoadEnd) w.WriteNumber("rows"u8, e.LoadRows);
            w.WriteEndObject();
        }
        w.WriteStartObject("source"u8);
        w.WriteString("server"u8, server);
        w.WriteString("db"u8, e.Table.Database);
        w.WriteString("incarnation"u8, ChangeEvent.IncarnationId(e.Incarnation)); // positions are comparable within one only
        w.WriteString("schema"u8, e.Table.Schema);
        w.WriteString("table"u8, e.Table.Table);
        w.WriteString("lsn"u8, e.Lsn.ToString());
        // rows of a load have no place in the commit order: a consumer's high-water mark is neither compared with them nor moved by them
        if (e.SnapshotId is null) w.WriteString("commit_lsn"u8, e.CommitLsn.ToString());
        if (!e.BeginLsn.IsZero) w.WriteString("begin_lsn"u8, e.BeginLsn.ToString());
        if (e.TransactionId != 0) w.WriteString("xid"u8, Log.LogRecord.FormatTransactionId(e.TransactionId));
        if (e.SnapshotId is null) w.WriteNumber("seq"u8, e.Sequence);
        if (e.TransactionSize > 0) w.WriteNumber("txn_size"u8, e.TransactionSize);
        if (e.CommitTime is { } ct) w.WriteString("commit_time"u8, ct.ToString(ct.Kind == DateTimeKind.Utc ? "yyyy-MM-ddTHH:mm:ss.fffZ" : "yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture));
        if (e.SnapshotId is not null) w.WriteString("snapshot"u8, e.SnapshotId);
        if (e.LookedUp is { Count: > 0 })
        {
            w.WriteStartArray("looked_up"u8);
            foreach (var c in e.LookedUp) w.WriteStringValue(c);
            w.WriteEndArray();
        }
        w.WriteEndObject();
        w.WriteNumber("ts_ms"u8, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        w.WriteEndObject();
    }

    public static byte[] ToUtf8(ChangeEvent e, string server)
    {
        var buffer = new ArrayBufferWriter<byte>(512);
        Write(e, server, buffer);
        return buffer.WrittenSpan.ToArray();
    }

    private static void WriteKey(Utf8JsonWriter w, ChangeEvent e)
    {
        var image = e.After ?? e.Before;
        if (image is null || e.Table.Key.Count == 0) { w.WriteNullValue(); return; }
        w.WriteStartObject();
        foreach (var k in e.Table.Key)
        {
            w.WritePropertyName(k.Name);
            WriteValue(w, image.Values[e.Table.VisibleIndex[k.ColumnId]]);
        }
        w.WriteEndObject();
    }

    private static void WriteImage(Utf8JsonWriter w, RowImage? image)
    {
        if (image is null) { w.WriteNullValue(); return; }
        w.WriteStartObject();
        var cols = image.Layout.Visible;
        for (var i = 0; i < cols.Count; i++)
        {
            w.WritePropertyName(cols[i].Name);
            WriteValue(w, image.Values[i]);
        }
        w.WriteEndObject();
    }

    private static bool WellFormed(string s)
    {
        for (var i = 0; i < s.Length; i++)
        {
            if (!char.IsSurrogate(s[i])) continue;
            if (!char.IsHighSurrogate(s[i]) || i + 1 == s.Length || !char.IsLowSurrogate(s[i + 1])) return false;
            i++;
        }
        return true;
    }

    /// <summary>nvarchar may hold half a surrogate pair (LEFT(N'😀', 1)). As an escape it stays what it is; as UTF-8 it would become U+FFFD, and
    /// different keys the same.</summary>
    private static string Escaped(string s)
    {
        var sb = new StringBuilder(s.Length + 16).Append('"');
        foreach (var c in s)
            if (c is '"' or '\\' || c < ' ' || c > '~') sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
            else sb.Append(c);
        return sb.Append('"').ToString();
    }

    /// <summary>JSON mapping: integers and floats as numbers, decimal/money as exact strings, temporal as ISO-8601, binary as base64.</summary>
    public static void WriteValue(Utf8JsonWriter w, object? v)
    {
        switch (v)
        {
            case null: w.WriteNullValue(); break;
            case Unavailable: w.WriteStartObject(); w.WriteBoolean("unavailable"u8, true); w.WriteEndObject(); break;
            case bool b: w.WriteBooleanValue(b); break;
            case byte b: w.WriteNumberValue(b); break;
            case short s: w.WriteNumberValue(s); break;
            case int i: w.WriteNumberValue(i); break;
            case long l: w.WriteNumberValue(l); break;
            case float f when !float.IsFinite(f): w.WriteStringValue(f.ToString(CultureInfo.InvariantCulture)); break; // JSON has no number for it
            case double d when !double.IsFinite(d): w.WriteStringValue(d.ToString(CultureInfo.InvariantCulture)); break;
            case float f: w.WriteNumberValue(f); break;
            case double d: w.WriteNumberValue(d); break;
            case decimal m: w.WriteStringValue(m.ToString(CultureInfo.InvariantCulture)); break;
            case string s when !WellFormed(s): w.WriteRawValue(Escaped(s), skipInputValidation: true); break;
            case string s: w.WriteStringValue(s); break;
            case RawText t: w.WriteStartObject(); w.WriteBase64String("undecodable"u8, t.Bytes); w.WriteEndObject(); break;
            case byte[] bytes: w.WriteBase64StringValue(bytes); break;
            case Guid g: w.WriteStringValue(g.ToString("D")); break;
            case DateOnly d: w.WriteStringValue(d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)); break;
            case TimeSpan t: w.WriteStringValue(t.ToString("hh\\:mm\\:ss\\.fffffff", CultureInfo.InvariantCulture)); break;
            case DateTime dt: w.WriteStringValue(dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture)); break;
            case DateTimeOffset dto: w.WriteStringValue(dto.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture)); break;
            default: w.WriteStringValue(Convert.ToString(v, CultureInfo.InvariantCulture)); break;
        }
    }
}
