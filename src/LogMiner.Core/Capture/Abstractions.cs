using LogMiner.Log;
using LogMiner.Pipeline;
using LogMiner.Schema;

namespace LogMiner.Capture;

/// <summary>
/// Which database, and which history of it. The log is read by id, the catalog and the release go by name, and both move: names are swapped,
/// a dropped or detached database's id is taken by the next one, a restore puts another history in the same place. Databases made from one
/// backup even have the same LSNs and rowset ids, so a mix-up would not show in the log. Every access therefore proves all three.
/// </summary>
public readonly record struct DatabaseRef(int Id, string Name, string Incarnation)
{
    public override string ToString() => $"{Name} ({Id})";
}

/// <summary>The id or the name no longer belongs to that database and history (renamed, dropped, offline, restored, ...).</summary>
public sealed class DatabaseChangedException : Exception
{
    public DatabaseChangedException() { }
    public DatabaseChangedException(string message) : base(message) { }
    public DatabaseChangedException(string message, Exception inner) : base(message, inner) { }
    public DatabaseChangedException(DatabaseRef database, Exception? inner = null) : base($"{database} is not that database any more (renamed, dropped, taken offline or restored)", inner) { }
}

/// <summary>Where log records and table layouts come from (the engine via fn_full_dblog in production, a simulator in tests).</summary>
public interface ILogSource
{
    /// <summary>Records from <paramref name="start"/> (inclusive; null = oldest available), in LSN order, at most <paramref name="maxRows"/>.
    /// A start that is no longer in the log raises <see cref="LogTruncatedException"/>; it is never moved silently. When it was another
    /// database's log, or becomes it meanwhile, <see cref="DatabaseChangedException"/> is raised after the last record at the latest:
    /// nothing that was read may be used before the enumeration has ended (or <see cref="VerifyAsync"/> has answered).
    /// <paramref name="settled"/>: see <see cref="Pipeline.Checkpoint.Settled"/>; only then may the beginning of its log block be missing.</summary>
    IAsyncEnumerable<LogRecord> ReadAsync(DatabaseRef database, Lsn? start, int maxRows, CancellationToken ct, bool settled = false);

    /// <summary>The last read ended before captured changes that exist and are not on disk yet (they are not read before they are).</summary>
    bool Unhardened => false;

    /// <summary>Raises <see cref="DatabaseChangedException"/> unless it still is that database. Usable while a read is open.</summary>
    Task VerifyAsync(DatabaseRef database, CancellationToken ct);

    /// <summary>Layouts of the captured tables: current, and as of every LSN from <paramref name="from"/> (null = start of the log) on.</summary>
    /// <param name="database">The database.</param>
    /// <param name="from">Oldest record that will be decoded with the result.</param>
    /// <param name="unresolvedRowsets">Rowsets with replicated records that the previous load did not know (e.g. of a table whose CDC was disabled since).</param>
    /// <param name="ct">Cancellation.</param>
    Task<DatabaseSchema> LoadSchemaAsync(DatabaseRef database, Lsn? from, IReadOnlySet<long> unresolvedRowsets, CancellationToken ct);

    /// <summary>The log was truncated beyond <paramref name="position"/>: were all captured changes after it retained?</summary>
    Task<(LogContinuity Continuity, string Detail)> CheckContinuityAsync(DatabaseRef database, Checkpoint position, CancellationToken ct);
}

/// <summary>The log was truncated beyond the requested position. Whether changes were lost is decided by <see cref="ILogSource.CheckContinuityAsync"/>.</summary>
public sealed class LogTruncatedException : Exception
{
    public LogTruncatedException() { }
    public LogTruncatedException(string message) : base(message) { }
    public LogTruncatedException(string message, Exception inner) : base(message, inner) { }
    public LogTruncatedException(Lsn requested, Lsn oldest) : base($"the log starts at {oldest}, after the requested {requested}") => (Requested, Oldest) = (requested, oldest);
    public Lsn Requested { get; }
    public Lsn Oldest { get; }
}

public enum LogContinuity
{
    /// <summary>The held-back transaction is still unreleased: whatever was truncated held no captured change after the position.</summary>
    Intact,
    /// <summary>Someone else released the log beyond the position: changes may have been lost.</summary>
    Broken,
    /// <summary>Nothing was held back at the position, so it cannot be told.</summary>
    Unknown,
    /// <summary>A table is captured from some point on; what it held before was never published.</summary>
    NewTable,
    /// <summary>Rows came into or went out of a table by SWITCH: none of them is in the log.</summary>
    Switched,
    /// <summary>A table has another name (or schema): its rows were published under the old one.</summary>
    Renamed,
}

/// <summary>Captured changes may be missing: everything between two positions of a database's (or one table's) history.</summary>
public sealed record CaptureGap(string Database, string Incarnation, Lsn AfterCommit, Lsn ResumedAt, LogContinuity Continuity, string Reason, DateTimeOffset DetectedAt, string? Table = null);

/// <summary>What to do when captured changes may be missing. In every case the gap is recorded durably first and reported loudly.</summary>
public enum GapPolicy
{
    /// <summary>Go on with what is there, and load every captured table of the database (or the one table) again: consumers converge on the
    /// current rows; deletes inside the gap are not conveyed.</summary>
    Resync,
    /// <summary>Go on with what is there.</summary>
    Continue,
}

/// <summary>Durable record of gaps (and the request to load the database again).</summary>
public interface IGapLog
{
    Task RecordAsync(CaptureGap gap, bool resync, CancellationToken ct);
}

/// <summary>Allows the source to discard log up to a position (sp_repldone in production).</summary>
public interface ILogReleaser
{
    /// <summary>Raises <see cref="DatabaseChangedException"/>, releasing nothing, when it is not that database any more: releasing a
    /// position in another database's log would let SQL Server drop changes nobody has read.</summary>
    Task ReleaseAsync(DatabaseRef database, Lsn beginLsn, Lsn commitLsn, CancellationToken ct);
}

public enum StartPosition
{
    /// <summary>Without a checkpoint: resume from SQL Server's own replication state when the database has undistributed
    /// transactions (it was captured before - nothing is skipped), otherwise start at the end of the log. Resolved by the engine.</summary>
    Auto,
    /// <summary>Without a checkpoint, publish every replicated transaction still in the active log.</summary>
    Earliest,
    /// <summary>Without a checkpoint, skip what is already in the log; publish transactions committed after the first catch-up.</summary>
    Latest,
}

public sealed class CaptureSettings
{
    public StartPosition StartPosition { get; init; } = StartPosition.Latest;
    public int MaxRowsPerRead { get; init; } = 20_000;
    /// <summary>Events per sink call. Every committed transaction ready after a log read goes out in as few calls as possible,
    /// each awaited before the next (one unacknowledged call per database: nothing can overtake a failed batch).</summary>
    public int MaxBatchEvents { get; init; } = 4096;
    public GapPolicy OnGap { get; init; } = GapPolicy.Resync;
    /// <summary>Upper bound for buffered (uncommitted) row images per database before the stream faults loudly.</summary>
    public long MaxBufferedBytes { get; init; } = 1L * 1024 * 1024 * 1024;
}
