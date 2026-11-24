using System.Globalization;

namespace LogMiner.Log;

public enum LogOperation : byte
{
    Other = 0,
    BeginXact,
    CommitXact,
    AbortXact,
    InsertRows,
    DeleteRows,
    ModifyRow,
    ModifyColumns,
    /// <summary>LOP_REPL_COMMAND: replication/CDC DDL marker written for schema changes on tracked tables.</summary>
    ReplCommand,
    HobtDdl,
    /// <summary>Replicated records that say nothing a row record of the same transaction does not say again: LOB values and update/prepare
    /// brackets (experiments/28). They count towards <see cref="LogRecord.ReplicatedRecords"/>.</summary>
    ReplicationInfo,
}

public enum LogContext : byte
{
    Other = 0,
    Null,
    Heap,
    Clustered,
    MarkAsGhost,
    IndexLeaf,
    TextMix,
    TextTree,
}

/// <summary>Bits of fn_dblog's [Flag Bits] that the miner relies on (verified empirically, see experiments/).</summary>
[Flags]
public enum LogRecordBits : ushort
{
    None = 0,
    /// <summary>Compensation log record (undo of an earlier record during rollback / rollback to savepoint).</summary>
    Compensation = 0x0001,
    /// <summary>Record belongs to a table marked for replication/CDC (full row images are logged).</summary>
    Replicated = 0x0004,
}

/// <summary>
/// One transaction log record with the fields the miner needs. Filled from fn_full_dblog columns by the engine
/// source; a direct (file) source would fill it from raw log blocks.
/// </summary>
public sealed class LogRecord
{
    public Lsn Lsn { get; init; }
    public LogOperation Operation { get; init; }
    public LogContext Context { get; init; }
    public ulong TransactionId { get; init; }
    public LogRecordBits Flags { get; init; }
    public long AllocUnitId { get; init; }
    public long PartitionId { get; init; }
    public short OffsetInRow { get; init; }
    public short ModifySize { get; init; }
    public int LogRecordLength { get; init; }
    /// <summary>Row image (insert/delete) or before image (modify).</summary>
    public byte[]? RowLog0 { get; init; }
    /// <summary>After image (modify).</summary>
    public byte[]? RowLog1 { get; init; }
    /// <summary>Key row of a partial (non-replicated) clustered-index modify.</summary>
    public byte[]? RowLog2 { get; init; }
    /// <summary>Compensation records: the transaction's record before the one being undone. Undo runs strictly backwards, so everything
    /// the transaction logged above it is undone (experiments/14).</summary>
    public Lsn UndoneAfter { get; init; }
    /// <summary>Commit/abort records: LSN of the transaction's LOP_BEGIN_XACT.</summary>
    public Lsn TransactionBegin { get; init; }
    /// <summary>Commit/abort records: server-local end time.</summary>
    public DateTime? EndTime { get; init; }
    /// <summary>Commit records: how many replicated records the transaction logged (undone ones too, the commit itself not); exact in every
    /// recovery model, with row versioning, accelerated database recovery, bulk loads and LOBs (experiments/28). -1: not known.</summary>
    public long ReplicatedRecords { get; init; } = -1;

    public bool IsCompensation => (Flags & LogRecordBits.Compensation) != 0;
    public bool IsReplicated => (Flags & LogRecordBits.Replicated) != 0;

    public static LogOperation ParseOperation(string op) => op switch
    {
        "LOP_BEGIN_XACT" => LogOperation.BeginXact,
        "LOP_COMMIT_XACT" => LogOperation.CommitXact,
        "LOP_ABORT_XACT" => LogOperation.AbortXact,
        "LOP_INSERT_ROWS" => LogOperation.InsertRows,
        "LOP_DELETE_ROWS" => LogOperation.DeleteRows,
        "LOP_MODIFY_ROW" => LogOperation.ModifyRow,
        "LOP_MODIFY_COLUMNS" => LogOperation.ModifyColumns,
        "LOP_REPL_COMMAND" => LogOperation.ReplCommand,
        "LOP_HOBT_DDL" => LogOperation.HobtDdl,
        "LOP_TEXT_INFO_BEGIN" or "LOP_TEXT_INFO_END" or "LOP_TEXT_VALUE" or "LOP_BEGIN_UPDATE" or "LOP_END_UPDATE" or "LOP_PREP_XACT" => LogOperation.ReplicationInfo,
        _ => LogOperation.Other,
    };

    public static LogContext ParseContext(string ctx) => ctx switch
    {
        "LCX_NULL" => LogContext.Null,
        "LCX_HEAP" => LogContext.Heap,
        "LCX_CLUSTERED" => LogContext.Clustered,
        "LCX_MARK_AS_GHOST" => LogContext.MarkAsGhost,
        "LCX_INDEX_LEAF" => LogContext.IndexLeaf,
        "LCX_TEXT_MIX" => LogContext.TextMix,
        "LCX_TEXT_TREE" => LogContext.TextTree,
        _ => LogContext.Other,
    };

    /// <summary>Parses fn_dblog's "0000:000004c3" transaction id into a 48-bit integer.</summary>
    public static ulong ParseTransactionId(ReadOnlySpan<char> s)
    {
        var colon = s.IndexOf(':');
        if (colon < 0) return ulong.Parse(s, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture);
        var hi = ulong.Parse(s[..colon], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture);
        var lo = ulong.Parse(s[(colon + 1)..], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture);
        return (hi << 32) | lo;
    }

    public static string FormatTransactionId(ulong xid) => string.Create(CultureInfo.InvariantCulture, $"{xid >> 32:x4}:{xid & 0xFFFFFFFF:x8}");

    /// <summary>Parses fn_dblog's "2026/11/23 19:07:09:180" timestamps.</summary>
    public static DateTime? ParseLogTime(string? s) =>
        s is null ? null
        : DateTime.TryParseExact(s, "yyyy/MM/dd HH:mm:ss:fff", CultureInfo.InvariantCulture, DateTimeStyles.None, out var t) ? t : null;
}
