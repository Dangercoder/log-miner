using LogMiner.Rows;
using LogMiner.Schema;

namespace LogMiner.Capture;

/// <summary>A chunk of current table rows read between two log positions (see <see cref="ISnapshotSource"/>).</summary>
public sealed class SnapshotChunk
{
    public required TableLayout Table { get; init; }
    /// <summary>Log end before the chunk was read.</summary>
    public required Lsn Low { get; init; }
    /// <summary>Log end after the chunk was read.</summary>
    public required Lsn High { get; init; }
    public required IReadOnlyList<RowImage> Rows { get; init; }
    /// <summary>Key values of the last row (resume point), null when the table is exhausted.</summary>
    public object?[]? LastKey { get; init; }
}

/// <summary>Reads table rows in key order, bracketed by log positions.</summary>
public interface ISnapshotSource
{
    /// <summary>Reads up to <paramref name="chunkSize"/> rows with key greater than <paramref name="afterKey"/> (null = from the start),
    /// with locking read-committed semantics (each row is the latest committed version at the time it is read).</summary>
    Task<SnapshotChunk> ReadChunkAsync(TableLayout table, object?[]? afterKey, int chunkSize, CancellationToken ct);
}

/// <summary>
/// Historical load of one table, interleaved with the live stream ("watermark" chunking, as in Netflix DBLog):
/// <list type="number">
/// <item>Read a chunk between log positions L (before) and H (after).</item>
/// <item>While the stream processes transactions committed in [L, H), drop chunk rows whose key they touched - the stream
/// carries a newer image of those rows.</item>
/// <item>When the stream reaches H (the first transaction committed at or after H, or the end of the log), emit the remaining
/// rows as snapshot reads, in stream order.</item>
/// </list>
/// Why it is exact: a row read at time t reflects every commit that completed before t. A key with no commit in [L, H) has the
/// same value at H as when it was read; a key with a commit in [L, H) is superseded by that commit's event, which the stream
/// emits in commit order. Requires locking read committed (a version read under RCSI can lag a commit whose log record is
/// already written) and fully durable commits (delayed durability makes a commit visible before its log record is flushed).
/// No long transaction, no table lock; resumable by key; on any fault the current chunk is simply re-read.
/// </summary>
public sealed class TableSnapshot(TableLayout table, string snapshotId, int chunkSize)
{
    private readonly HashSet<string> _touched = new(StringComparer.Ordinal);
    private bool _unknownTouched;
    private SnapshotChunk? _pending;

    public TableLayout Table { get; private set; } = table;
    public string SnapshotId { get; } = snapshotId;
    public int ChunkSize { get; } = chunkSize > 0 ? chunkSize : throw new ArgumentOutOfRangeException(nameof(chunkSize), chunkSize, "a chunk has at least one row");
    public object?[]? NextAfterKey { get; private set; }
    public bool Done { get; private set; }
    public long RowsEmitted { get; private set; }
    public int Chunks { get; private set; }
    /// <summary>True when the load gave up (see <see cref="Error"/>); <see cref="Done"/> is then also true.</summary>
    public bool Failed { get; private set; }
    /// <summary>The last chunk read failure.</summary>
    public string? Error { get; private set; }
    public int ConsecutiveFailures { get; private set; }
    /// <summary>A failed chunk read is retried after this delay, doubling per consecutive failure (up to 30 s).</summary>
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>Consecutive chunk read failures after which the load fails (instead of retrying forever).</summary>
    public int MaxConsecutiveFailures { get; init; } = 5;
    /// <summary>The same for what the server reports (a lock, a deadlock, a database being maintained): about half an hour.</summary>
    public int MaxConsecutiveServerFailures { get; init; } = 60;
    internal DateTimeOffset RetryAt { get; private set; }
    private int _chunkSeq;

    internal SnapshotChunk? Pending => _pending;
    /// <summary>Unique id of the pending chunk (part of the snapshot rows' message ids).</summary>
    internal int ChunkSeq => _chunkSeq;
    private readonly string _attempt = Guid.NewGuid().ToString("N")[..16];
    /// <summary>Unique to the pending chunk's reading, over restarts and other loads of the same table.</summary>
    internal string ReadId => $"{_attempt}.{_chunkSeq}";
    /// <summary>Of this attempt's markers.</summary>
    internal string AttemptId => _attempt;
    /// <summary>The marker before its first row was acknowledged.</summary>
    internal bool Begun { get; set; }

    /// <summary>Begins tracking a freshly read chunk; null discards a pending chunk (it is re-read after a fault).</summary>
    internal void Start(SnapshotChunk? chunk)
    {
        _pending = chunk;
        _touched.Clear();
        _unknownTouched = false;
        if (chunk is not null) { Table = chunk.Table; _chunkSeq++; ConsecutiveFailures = 0; }
    }

    /// <summary>A chunk read failed (lock timeout, table dropped, ...): retry later, or give up after too many in a row.</summary>
    internal void ChunkFailed(Exception ex, DateTimeOffset now)
    {
        ConsecutiveFailures++;
        Error = $"{ex.GetType().Name}: {ex.Message}";
        if (ConsecutiveFailures >= (ex is System.Data.Common.DbException ? MaxConsecutiveServerFailures : MaxConsecutiveFailures)) { Failed = true; Done = true; return; }
        var delay = RetryDelay.TotalMilliseconds * Math.Pow(2, ConsecutiveFailures - 1);
        RetryAt = now + TimeSpan.FromMilliseconds(Math.Min(delay, 30_000));
    }

    /// <summary>The load cannot continue (e.g. its database was removed).</summary>
    public void Abandon(string reason)
    {
        if (Done) return;
        Error = reason;
        Failed = true;
        Done = true;
    }

    /// <summary>Called for every change the stream emits before H: records keys committed inside [L, H).</summary>
    internal void Observe(Lsn commitLsn, TableLayout table, RowImage? before, RowImage? after)
    {
        if (_pending is null || table.ObjectId != Table.ObjectId || commitLsn < _pending.Low || commitLsn >= _pending.High) return;
        foreach (var image in new[] { before, after })
            if (image is null) { }
            else if (KeyOf(image) is { } key) _touched.Add(key);
            else _unknownTouched = true; // some row of the chunk may have changed, and it cannot be told which
    }

    /// <summary>The pending chunk cannot be used: read it again.</summary>
    internal bool MustReadAgain => _unknownTouched;

    /// <summary>Rows that survive (no commit to their key in [L, H)). Progress is NOT advanced here: only
    /// <see cref="Acknowledge"/> (after the sink acknowledged the rows) moves the resume key, so a failed send re-reads the chunk.</summary>
    internal (IReadOnlyList<RowImage> Rows, object?[]? LastKey, bool IsLast) Complete()
    {
        var chunk = _pending ?? throw new InvalidOperationException("no pending chunk");
        var rows = chunk.Rows.Where(r => KeyOf(r) is not { } key || !_touched.Contains(key)).ToList();
        _pending = null;
        return (rows, chunk.LastKey, chunk.LastKey is null || chunk.Rows.Count < ChunkSize);
    }

    internal void Acknowledge(object?[]? lastKey, bool isLast, int rowCount)
    {
        Chunks++;
        RowsEmitted += rowCount;
        NextAfterKey = lastKey;
        if (isLast) Done = true;
    }

    /// <summary>Equal exactly when every key value is: no two different keys look the same (fractions of a second, separators within strings,
    /// null and empty). Null when a key value is not known.</summary>
    internal static string? KeyOf(RowImage row)
    {
        var sb = new System.Text.StringBuilder();
        var ic = System.Globalization.CultureInfo.InvariantCulture;
        foreach (var k in row.Layout.Key)
        {
            var part = row.Values[row.Layout.VisibleIndex[k.ColumnId]] switch
            {
                null => "n",
                Unavailable => null,
                string s => "s" + s,
                RawText r => "r" + Convert.ToHexString(r.Bytes),
                byte[] b => "b" + Convert.ToHexString(b),
                DateTime d => "t" + d.Ticks.ToString(ic),
                DateTimeOffset d => "o" + d.Ticks.ToString(ic) + "/" + d.Offset.Ticks.ToString(ic),
                DateOnly d => "d" + d.DayNumber.ToString(ic),
                TimeSpan s => "i" + s.Ticks.ToString(ic),
                double d => "f" + BitConverter.DoubleToInt64Bits(d).ToString(ic),
                float f => "e" + BitConverter.SingleToInt32Bits(f).ToString(ic),
                IFormattable f => "v" + f.ToString(null, ic),
                var other => "x" + other,
            };
            if (part is null) return null;
            sb.Append(part.Length.ToString(ic)).Append(':').Append(part);
        }
        return sb.ToString();
    }
}
