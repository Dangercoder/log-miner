using LogMiner.Log;

namespace LogMiner.Transactions;

public enum ChangeKind : byte
{
    Insert, Update, Delete, PartialUpdate,
    /// <summary>A replicated record of a kind that is not understood. Never ignored: if its rowset is captured, the stream stops there.</summary>
    Unknown,
}

/// <summary>A row change buffered until its transaction commits. Images are raw row bytes; decoding happens at emit time.</summary>
public sealed class PendingChange
{
    public required Lsn Lsn { get; init; }
    public required ChangeKind Kind { get; init; }
    public required long AllocUnitId { get; init; }
    public required long PartitionId { get; init; }
    public byte[]? Before { get; init; }
    public byte[]? After { get; init; }
    /// <summary>Partial updates (no full images): byte offset of the modified region in the row.</summary>
    public short Offset { get; init; }
    /// <summary>Partial updates: key row (clustered index) identifying the row.</summary>
    public byte[]? KeyRow { get; init; }
    /// <summary>What an <see cref="ChangeKind.Unknown"/> record is.</summary>
    public string? Description { get; init; }
    /// <summary>The log record was longer than the images we received (fn_dblog's 8000-byte cap).</summary>
    public bool PossiblyTruncated { get; init; }

    internal int ByteSize => 64 + (Before?.Length ?? 0) + (After?.Length ?? 0) + (KeyRow?.Length ?? 0);
}

public sealed class CommittedTransaction
{
    public required ulong TransactionId { get; init; }
    public required Lsn BeginLsn { get; init; }
    public required Lsn CommitLsn { get; init; }
    public DateTime? CommitTime { get; init; }
    public required IReadOnlyList<PendingChange> Changes { get; init; }
    /// <summary>The transaction changed the schema of a tracked table (LOP_REPL_COMMAND seen): reload metadata before decoding later transactions.</summary>
    public bool HasSchemaChange { get; init; }
    /// <summary>Replicated records of the transaction that were received, undone ones too.</summary>
    public long RecordsSeen { get; init; }
    /// <summary>How many there are, according to its commit record (-1: not known).</summary>
    public long RecordsLogged { get; init; } = -1;
    /// <summary>Every replicated record the transaction logged was received. Otherwise its beginning was not read (or records are missing):
    /// neither may it be published as it is, nor may it be passed over.</summary>
    public bool Complete => RecordsLogged < 0 || RecordsSeen == RecordsLogged;
}

/// <summary>
/// Turns a database's log records (in LSN order) into committed transactions (in commit order).
/// <list type="bullet">
/// <item>Changes are buffered per transaction id until LOP_COMMIT_XACT; LOP_ABORT_XACT drops the buffer.</item>
/// <item>Every replicated commit comes out, also when nothing is left of it (all changes undone, or not a single record received): it holds
/// log until it is released, and it may be a loss.</item>
/// <item>A compensation record (rollback to a savepoint, a failed statement) says which record it undoes: everything the
/// transaction logged after <see cref="LogRecord.UndoneAfter"/> is cancelled. (Counting compensation records per allocation unit is not
/// enough: DDL and LOB work also undo row changes that were never captured, and would cancel unrelated earlier changes.)</item>
/// <item><see cref="RestartLsn"/> is the oldest begin LSN of any open transaction: resuming from the last commit
/// would lose the early records of transactions still open at that point.</item>
/// </list>
/// </summary>
public sealed class TransactionAssembler(Func<long, bool> isTrackedAllocUnit)
{
    private sealed class Buffer
    {
        public Lsn FirstLsn;
        public readonly List<PendingChange> Changes = [];
        public bool SchemaChange;
        public long Bytes, Replicated;
    }

    private readonly Dictionary<ulong, Buffer> _open = [];
    private long _bufferedBytes;
    private Lsn _lastLsn;

    public int OpenTransactions => _open.Count;
    public long BufferedBytes => _bufferedBytes;
    public Lsn LastLsn => _lastLsn;

    /// <summary>Oldest first-seen LSN among open transactions that touched tracked tables, or null when none are open.</summary>
    public Lsn? RestartLsn
    {
        get
        {
            Lsn? min = null;
            foreach (var b in _open.Values)
                if (min is null || b.FirstLsn < min.Value) min = b.FirstLsn;
            return min;
        }
    }

    /// <summary>Feeds one record; appends a <see cref="CommittedTransaction"/> to <paramref name="committed"/> when one completes.</summary>
    public void Process(LogRecord r, List<CommittedTransaction> committed)
    {
        if (r.Lsn <= _lastLsn && !_lastLsn.IsZero)
            throw new InvalidOperationException($"log records out of order: {r.Lsn} after {_lastLsn}");
        _lastLsn = r.Lsn;

        switch (r.Operation)
        {
            case LogOperation.CommitXact:
                if (_open.Remove(r.TransactionId, out var done)) _bufferedBytes -= done.Bytes;
                if (r.IsReplicated || done is { Changes.Count: > 0 } or { SchemaChange: true })
                    committed.Add(new CommittedTransaction
                    {
                        TransactionId = r.TransactionId,
                        BeginLsn = r.TransactionBegin.IsZero ? done?.FirstLsn ?? r.Lsn : r.TransactionBegin,
                        CommitLsn = r.Lsn,
                        CommitTime = r.EndTime,
                        Changes = done?.Changes ?? [],
                        HasSchemaChange = done?.SchemaChange ?? false,
                        RecordsSeen = done?.Replicated ?? 0,
                        RecordsLogged = r.ReplicatedRecords,
                    });
                return;
            case LogOperation.AbortXact:
                if (_open.Remove(r.TransactionId, out var aborted)) _bufferedBytes -= aborted.Bytes;
                return;
            case LogOperation.ReplCommand:
                Count(r).SchemaChange = true;
                return;
        }

        if (r.IsCompensation)
        {
            if (!_open.TryGetValue(r.TransactionId, out var b)) return;
            if (r.UndoneAfter.IsZero) throw new InvalidOperationException($"compensation record {r.Lsn} does not say what it undoes");
            while (b.Changes.Count > 0 && b.Changes[^1].Lsn > r.UndoneAfter)
            {
                b.Bytes -= b.Changes[^1].ByteSize;
                _bufferedBytes -= b.Changes[^1].ByteSize;
                b.Changes.RemoveAt(b.Changes.Count - 1);
            }
            return;
        }

        var buffer = r.IsReplicated ? Count(r) : null;
        if (!isTrackedAllocUnit(r.AllocUnitId)) return;

        var change = ToChange(r);
        if (change is null) return;
        buffer ??= GetOrOpen(r);
        buffer.Changes.Add(change);
        buffer.Bytes += change.ByteSize;
        _bufferedBytes += change.ByteSize;
    }

    private Buffer Count(LogRecord r)
    {
        var b = GetOrOpen(r);
        if (r.IsReplicated) b.Replicated++;
        return b;
    }

    private Buffer GetOrOpen(LogRecord r)
    {
        if (!_open.TryGetValue(r.TransactionId, out var b))
        {
            b = new Buffer { FirstLsn = r.Lsn };
            _open[r.TransactionId] = b;
        }
        return b;
    }

    private static PendingChange? ToChange(LogRecord r)
    {
        if (r.Context is not (LogContext.Heap or LogContext.Clustered or LogContext.MarkAsGhost)) return Unknown(r);
        var truncated = r.LogRecordLength > 0 && (r.RowLog0?.Length == 8000 || r.RowLog1?.Length == 8000);
        switch (r.Operation)
        {
            case LogOperation.InsertRows when r.RowLog0 is { Length: > 0 }:
                return new PendingChange { Lsn = r.Lsn, Kind = ChangeKind.Insert, AllocUnitId = r.AllocUnitId, PartitionId = r.PartitionId, After = r.RowLog0, PossiblyTruncated = truncated };
            case LogOperation.DeleteRows when r.RowLog0 is { Length: > 0 }:
                return new PendingChange { Lsn = r.Lsn, Kind = ChangeKind.Delete, AllocUnitId = r.AllocUnitId, PartitionId = r.PartitionId, Before = r.RowLog0, PossiblyTruncated = truncated };
            case LogOperation.ModifyRow when r.IsReplicated && r.OffsetInRow == 0 && r.RowLog0 is { Length: > 0 } && r.RowLog1 is { Length: > 0 }:
                return new PendingChange { Lsn = r.Lsn, Kind = ChangeKind.Update, AllocUnitId = r.AllocUnitId, PartitionId = r.PartitionId, Before = r.RowLog0, After = r.RowLog1, PossiblyTruncated = truncated };
            case LogOperation.ModifyRow or LogOperation.ModifyColumns:
                return new PendingChange { Lsn = r.Lsn, Kind = ChangeKind.PartialUpdate, AllocUnitId = r.AllocUnitId, PartitionId = r.PartitionId, Before = r.RowLog0, After = r.RowLog1, Offset = r.OffsetInRow, KeyRow = r.RowLog2 };
            default:
                return Unknown(r);
        }
    }

    /// <summary>Not a row change: fine for what is known to repeat a row record (LOB pages and values, brackets), or is not replicated at all.</summary>
    private static PendingChange? Unknown(LogRecord r) =>
        !r.IsReplicated || r.Operation is LogOperation.ReplicationInfo or LogOperation.BeginXact || r.Context is LogContext.TextMix or LogContext.TextTree ? null
        : new PendingChange { Lsn = r.Lsn, Kind = ChangeKind.Unknown, AllocUnitId = r.AllocUnitId, PartitionId = r.PartitionId, Description = $"replicated record {r.Lsn} of an unknown kind ({r.Operation}, {r.Context}, {r.RowLog0?.Length ?? 0} bytes)" };
}
