using LogMiner.Events;
using LogMiner.Log;
using LogMiner.Pipeline;
using LogMiner.Rows;
using LogMiner.Schema;
using LogMiner.Sinks;
using LogMiner.Transactions;

namespace LogMiner.Capture;

public enum CaptureState { Idle, Running, Faulted }

/// <summary>
/// Capture state of one database. Pumped by at most one worker at a time.
/// <para>Correctness rests on three rules, all exercised by the pipeline model test:</para>
/// <list type="number">
/// <item>Transactions are published in commit order and acknowledged in that order (an acknowledged prefix).</item>
/// <item>A checkpoint's restart LSN is at or before the first record of every transaction that is open or not yet
/// acknowledged, so re-reading from it rebuilds them completely; its commit LSN only covers acknowledged transactions.</item>
/// <item>Any failure (decode, sink, source) resets the stream to its last <i>durable</i> checkpoint - never forward.</item>
/// <item>A transaction is only published when every replicated record SQL Server counted at its commit was received. One that is not is read
/// again from its beginning; if that is gone, it is recorded as a gap; anything else stops the stream. No replicated commit is passed over.</item>
/// </list>
/// <para>The newest acknowledged transaction is not released: it is the bookmark that proves, after a truncation, that nobody else
/// released log beyond the checkpoint (<see cref="Checkpoint.Held"/>).</para>
/// </summary>
public sealed class DatabaseCapture
{
    /// <summary>A committed transaction (or a snapshot chunk) whose events are built and not yet acknowledged.</summary>
    private sealed record Ready(Lsn Begin, Lsn Commit, List<ChangeEvent> Events, bool IsSnapshot = false, Action? OnAcknowledged = null);
    private sealed class UnknownRowsetException(long rowsetId, Lsn lsn) : Exception($"rowset {rowsetId} at {lsn}")
    {
        public long RowsetId { get; } = rowsetId;
    }
    /// <summary>Records of the transaction are missing although the log should still have them: read it from its beginning.</summary>
    private sealed class IncompleteTransactionException(CommittedTransaction txn) : Exception($"{txn.RecordsSeen} of {txn.RecordsLogged} records of the transaction at {txn.CommitLsn}")
    {
        public CommittedTransaction Transaction { get; } = txn;
    }

    private readonly Lock _gate = new();
    private readonly List<Ready> _ready = [];
    /// <summary>Committed, being turned into events (which may take a catalog read): neither open any more nor ready yet.</summary>
    private readonly List<CommittedTransaction> _preparing = [];
    private Lsn _rereadFor;
    /// <summary>What was acknowledged when a transaction turned out not to be publishable: the retries start there, not before it.</summary>
    private Checkpoint? _progress;
    private readonly Queue<TableSnapshot> _snapshots = new();
    private TableSnapshot? _snapshot;
    private readonly StartPosition _start;
    private TransactionAssembler _assembler = NewAssembler();
    private DatabaseSchema? _schema;
    private bool _schemaStale = true;
    private Lsn? _scanFrom;
    private Lsn _lastLsn;
    private Lsn _skipThrough;
    private bool _silent;
    private Lsn _ackedCommit, _ackedBegin;
    private bool _held;
    private readonly Lsn _holdFrom;
    private Lsn _fullRelease;
    /// <summary>Where reading went on after a truncation: a transaction that began before it may have lost records and is never published in part.</summary>
    private Lsn _resumedAt;
    /// <summary>The truncation at <see cref="_resumedAt"/> was recorded as a gap (transactions cut by it are part of that gap).</summary>
    private bool _resumedInGap;
    private bool _caughtUp;
    private IGapLog? _gapLog;
    private GapPolicy _onGap;
    private (Lsn Begin, Lsn Commit)? _releasable;
    private Checkpoint? _durable;
    /// <summary>
    /// Where a <see cref="StartPosition.Latest"/> start went live. Nothing is published until it is durable: the start point of a
    /// "Latest" stream depends on when the catch-up ended, so a crash before it is saved would pick a different one - a gap after
    /// events already delivered, or older history delivered after newer events.
    /// </summary>
    private Checkpoint? _baseline;

    private int _queued;
    private volatile bool _dirty = true;

    /// <summary>Scheduler state: new log activity was reported since the last pump started.</summary>
    public bool Dirty { get => _dirty; set => _dirty = value; }
    /// <summary>Scheduler state: marks the capture as queued; false when it already was.</summary>
    public bool TryMarkQueued()
    {
        if (Interlocked.CompareExchange(ref _queued, 1, 0) != 0) return false;
        QueuedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        return true;
    }
    /// <summary>When the capture was last queued (Stopwatch timestamp), for queue-wait metrics.</summary>
    public long QueuedAt { get; private set; }
    /// <summary>When the pump that is running started (Stopwatch timestamp).</summary>
    public long PumpStarted { get; set; }
    public void ClearQueued() => Volatile.Write(ref _queued, 0);

    private Lsn _bell; // the newest log block known to have been written
    private bool _written; // something was written, which block is not known
    private bool _unhardened;
    private int _bellRetries;
    internal const int MaxBellRetries = 40;
    /// <summary>Pushed blocks that could not be read for as long as it was tried (they are found by the audit, or by the sweep).</summary>
    public long BellsGivenUp { get; private set; }
    /// <summary>A log block was written (which one, if it is known): the next pump is expected to read it.</summary>
    public void Ring(Lsn block)
    {
        lock (_gate)
        {
            if (block > _bell) _bell = block;
            else if (block.IsZero) _written = true;
        }
    }

    /// <summary>
    /// After a pump: null when it reached the newest block that was pushed, otherwise when to try again: log is only read when it is on disk. The
    /// push may come before that (SQL Server on Linux announces a write before it is flushed: measured, 4 reads too early per flush, 8 % of them by
    /// more than 63 ms), and the poll notices a write when it is <i>issued</i>. Nobody else asks for the change before the next one or the sweep,
    /// so it is tried for as long as a disk may reasonably take (8 s). Then it is given up: a pump may have good reasons not to read (held for its
    /// start point).
    /// </summary>
    public TimeSpan? RetryForBell()
    {
        lock (_gate)
        {
            var issued = _written && _unhardened;
            var ahead = !_bell.IsZero && _lastLsn < _bell;
            if (!issued && !ahead || _bellRetries >= MaxBellRetries)
            {
                if (_bellRetries >= MaxBellRetries) (_bell, BellsGivenUp) = (default, BellsGivenUp + 1);
                _bellRetries = 0;
                _written = false;
                return null;
            }
            return TimeSpan.FromMilliseconds(Math.Min(250, 1 << Math.Min(8, _bellRetries++)));
        }
    }

    /// <param name="databaseId">Database id.</param>
    /// <param name="name">Database name.</param>
    /// <param name="incarnation">Identity of the database's history.</param>
    /// <param name="checkpoint">Durable position, if any.</param>
    /// <param name="start">Where to start without a position.</param>
    /// <param name="holdFrom">Without a position: transactions that began before this may have been released by someone else earlier.</param>
    public DatabaseCapture(int databaseId, string name, string incarnation, Checkpoint? checkpoint, StartPosition start, Lsn holdFrom = default)
    {
        Database = new DatabaseRef(databaseId, name, incarnation);
        _start = start;
        _holdFrom = holdFrom;
        if (checkpoint is not null && !string.Equals(checkpoint.Incarnation, incarnation, StringComparison.Ordinal))
        {
            DiscardedCheckpoint = checkpoint; // restored / re-created database: its LSNs belong to another history
            checkpoint = null;
        }
        _durable = checkpoint;
        // Latest, and the end of the log is known: that is the start point, not wherever a catch-up happens to end
        if (checkpoint is null && start == StartPosition.Latest && !holdFrom.IsZero)
            _baseline = new Checkpoint(name, incarnation, default, new Lsn(holdFrom.Vlf, holdFrom.Block, (ushort)(holdFrom.Slot - 1)), default, DateTimeOffset.UtcNow);
        ResetToCheckpoint();
    }

    public DatabaseRef Database { get; }
    public int DatabaseId => Database.Id;
    public string Name => Database.Name;
    public string Incarnation => Database.Incarnation;
    /// <summary>The database was renamed, dropped, taken offline or restored: this capture is over, a new one starts from the durable state.</summary>
    public bool Superseded { get; private set; }
    /// <summary>A stored checkpoint that was ignored because it belongs to another incarnation of the database.</summary>
    public Checkpoint? DiscardedCheckpoint { get; }
    public CaptureState State { get; private set; }
    public string? Fault { get; private set; }
    public int ConsecutiveFaults { get; private set; }
    public DateTimeOffset RetryAt { get; private set; }
    public long EventsPublished { get; private set; }
    public long TransactionsPublished { get; private set; }
    public long RecordsRead { get; private set; }
    public long SchemaLoads { get; private set; }
    /// <summary>User tables of this database that are not CDC-enabled (not captured), from the last schema load.</summary>
    public IReadOnlyList<string> UntrackedTables => _schema?.UntrackedTables ?? [];
    /// <summary>Rows the last pump read from the source (including progress markers).</summary>
    public int LastPumpRows { get; private set; }
    /// <summary>Replicated records of tables that are someone else's (transactional replication); never of captured tables.</summary>
    public long SkippedForeignRecords { get; private set; }
    public DateTime? LastCommitTime { get; private set; }
    public DateTimeOffset LastPublishedAt { get; private set; }
    public Lsn LastLsn { get { lock (_gate) return _lastLsn; } }
    public int OpenTransactions { get { lock (_gate) return _assembler.OpenTransactions; } }
    public long BufferedBytes { get { lock (_gate) return _assembler.BufferedBytes; } }
    public Lsn AcknowledgedCommit { get { lock (_gate) return _ackedCommit; } }
    /// <summary>False only during the initial catch-up of <see cref="StartPosition.Latest"/>, while existing log is skipped.</summary>
    public bool Live => !_silent;
    /// <summary>A "Latest" start caught up and holds publishing until its start point is durable (the next checkpoint).</summary>
    public bool HoldingForBaseline { get { lock (_gate) return _durable is null && _baseline is not null; } }
    public Lsn ReleasedThrough { get; internal set; }
    public DateTimeOffset ReleasedAt { get; internal set; }
    /// <summary>What was releasable when it was last looked at: released at the next look.</summary>
    internal (Lsn Begin, Lsn Commit)? ReleaseNext { get; set; }
    /// <summary>Transaction up to which the log may be released: acknowledged, behind a durable checkpoint, and (normally) not the newest.</summary>
    public (Lsn Begin, Lsn Commit)? Releasable { get { lock (_gate) return Superseded ? null : _releasable; } }
    /// <summary>Gaps detected since the capture started (each is recorded durably before the stream goes on).</summary>
    public int Gaps { get; private set; }
    private readonly System.Collections.Concurrent.ConcurrentQueue<CaptureGap> _unreported = new();
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> _notices = new();
    private Lsn _switchesThrough;
    /// <summary>The next thing consumers have to know that is not in the stream (and was not reported yet).</summary>
    public bool TryTakeNotice([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? notice) => _notices.TryDequeue(out notice);
    /// <summary>The next detected gap that was not reported yet.</summary>
    public bool TryTakeGap([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out CaptureGap? gap) => _unreported.TryDequeue(out gap);

    // The source is pre-filtered to records of captured tables; everything is buffered and the table resolved at commit.
    private static TransactionAssembler NewAssembler() => new(_ => true);

    private void ResetToCheckpoint()
    {
        _assembler = NewAssembler();
        if (_changingObjects.Count > 0) _objectsChanged = true; // it may commit before where reading starts again
        _changingObjects.Clear();
        _ready.Clear();
        _preparing.Clear();
        _caughtUp = false;
        _lastLsn = default;
        _snapshot?.Start(null); // a chunk read before the fault is re-read later (rows already emitted are idempotent upserts)
        var cp = _durable ?? _baseline;
        if (_progress is { } p && (cp is null || p.LastCommitLsn > cp.LastCommitLsn)) cp = p;
        if (cp is not null)
        {
            // Never behind the last acknowledged commit while the bookmark keeps it in the log: every later commit is then read again, and each
            // says by itself where it began and how many records it has. What may be lost no longer depends on the restart position.
            _scanFrom = cp.RestartLsn.IsZero ? null : cp.Held && cp.LastCommitLsn < cp.RestartLsn ? cp.LastCommitLsn : cp.RestartLsn;
            _skipThrough = cp.LastCommitLsn;
            _ackedCommit = cp.LastCommitLsn;
            _ackedBegin = cp.LastBeginLsn;
            _held = cp.Held;
            if (cp.ResumedAt > _resumedAt) (_resumedAt, _resumedInGap) = (cp.ResumedAt, cp.ResumedInGap);
            _silent = false;
        }
        else
        {
            _scanFrom = null;
            _skipThrough = default;
            _ackedCommit = _ackedBegin = default;
            _held = false;
            _silent = _start == StartPosition.Latest;
        }
    }

    /// <summary>Checkpoint covering everything acknowledged so far, or null when nothing changed since the last durable one.</summary>
    public Checkpoint? PendingCheckpoint()
    {
        lock (_gate)
        {
            if (Superseded) return null;                                // whatever was read last may be another database's
            if (_silent) return null;                                   // a "Latest" catch-up decides nothing until it ends
            if (_durable is null && _baseline is { } live) return live; // the start point is saved before anything is published
            if (_ackedCommit.IsZero && _durable is null) return null;
            var cp = Position();
            if (_durable is { } d && d.LastCommitLsn == cp.LastCommitLsn && d.RestartLsn == cp.RestartLsn && d.Held == cp.Held && d.ResumedAt == cp.ResumedAt && d.ResumedInGap == cp.ResumedInGap) return null;
            return cp;
        }
    }

    /// <summary>What is acknowledged, and from where everything that is not must be read again.</summary>
    private Checkpoint Position()
    {
        var restart = _assembler.RestartLsn ?? (!_lastLsn.IsZero ? _lastLsn : _scanFrom ?? default);
        foreach (var t in _preparing) if (t.BeginLsn < restart) restart = t.BeginLsn;
        foreach (var r in _ready) if (!r.IsSnapshot && r.Begin < restart) restart = r.Begin; // built or in flight, not yet acknowledged
        return new Checkpoint(Name, Incarnation, restart, _ackedCommit, _ackedBegin, DateTimeOffset.UtcNow, _held && _fullRelease != _ackedCommit, _resumedAt, _resumedInGap);
    }

    /// <summary>The set of tables may have changed (CDC disabled on one, a new table): load the layouts again.</summary>
    public void RefreshSchema() { lock (_gate) _schemaStale = true; }

    private readonly HashSet<ulong> _changingObjects = []; // transactions that wrote to the catalog's table of objects and have not ended
    private bool _objectsChanged;

    // What committed before the layouts were read is in them (a start reads log that reaches back to when CDC was enabled).
    private void ObjectsMayHaveChanged(Lsn commit)
    {
        if (_schema is null || (!_schema.AsOf.IsZero && commit < _schema.AsOf)) return;
        _objectsChanged = true;
        _namesStale = true;
    }

    private bool _namesStale;

    /// <summary>Whether tables were created, dropped or renamed, or CDC was enabled or disabled, since the last call.</summary>
    public bool TakeObjectsChanged() { lock (_gate) { var changed = _objectsChanged; _objectsChanged = false; return changed; } }

    /// <summary>The database is published for transactional replication: its log is the Log Reader Agent's to release.</summary>
    public bool ReleasedByOthers { get; init; }

    /// <summary>
    /// Asks to release the newest transaction too (the log it holds has grown large and no captured change follows). It takes effect
    /// behind the next durable checkpoint, which says so; until the next captured change nothing is held back any more. Refused while the
    /// stream is behind: then later transactions hold the log anyway, and the bookmark is needed most.
    /// </summary>
    public bool RequestFullRelease()
    {
        lock (_gate)
        {
            if (!_held || !_caughtUp || _ready.Count > 0 || State == CaptureState.Faulted) return false;
            if (_ackedCommit.IsZero || _durable?.LastCommitLsn != _ackedCommit) return false;
            _fullRelease = _ackedCommit;
            return true;
        }
    }

    public void MarkDurable(Checkpoint cp)
    {
        lock (_gate)
        {
            if (_durable is null && _baseline is not null) _dirty = true; // publishing was held for this: pump again
            // the previous position may be released now; the newest transaction stays, as the bookmark, unless a full release was asked for
            if (_durable is { } old && !old.LastCommitLsn.IsZero && old.LastCommitLsn < cp.LastCommitLsn) _releasable = (old.LastBeginLsn, old.LastCommitLsn);
            if (cp.LastCommitLsn > _ackedCommit)
            {
                // the stream faulted and rewound to the previous checkpoint while this one was being saved: what it claims was acknowledged
                (_ackedCommit, _ackedBegin, _held) = (cp.LastCommitLsn, cp.LastBeginLsn, cp.Held);
                _skipThrough = cp.LastCommitLsn;
            }
            if (!cp.Held && !_fullRelease.IsZero && _fullRelease == cp.LastCommitLsn)
            {
                _releasable = (cp.LastBeginLsn, cp.LastCommitLsn);
                if (_ackedCommit == cp.LastCommitLsn) _held = false;
            }
            _durable = cp;
        }
    }
    public Checkpoint? Durable { get { lock (_gate) return _durable; } }

    /// <summary>Records a failure and rewinds to the last durable checkpoint (events after it are replayed; duplicates carry the same ids).</summary>
    public void SetFault(Exception ex)
    {
        lock (_gate)
        {
            State = CaptureState.Faulted;
            Fault = $"{ex.GetType().Name}: {ex.Message}";
            ConsecutiveFaults++;
            RetryAt = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, Math.Min(ConsecutiveFaults, 6)) / 4));
            _schemaStale = true;
            ResetToCheckpoint();
        }
    }

    /// <summary>Queues a historical load of a table (needs a primary key or unique clustered index). Runs interleaved with the stream.</summary>
    public void RequestSnapshot(TableSnapshot snapshot)
    {
        if (snapshot.Table.Key.Count == 0) throw new InvalidOperationException($"{snapshot.Table}: a snapshot needs a primary key or unique clustered index");
        lock (_gate) _snapshots.Enqueue(snapshot);
    }

    /// <summary>The snapshot in progress, if any.</summary>
    public TableSnapshot? ActiveSnapshot { get { lock (_gate) return _snapshot; } }
    public int QueuedSnapshots { get { lock (_gate) return _snapshots.Count; } }
    /// <summary>Raised (on the pumping thread) after a snapshot chunk was acknowledged by the sink.</summary>
    public event Action<TableSnapshot>? SnapshotProgress;

    /// <summary>Layouts currently known for this database (null before the first pump).</summary>
    public DatabaseSchema? Schema { get { lock (_gate) return _schema; } }

    /// <summary>
    /// Reads new records, publishes committed transactions and due snapshot chunks, waits for their acknowledgements.
    /// Returns true when more work is pending (unread log or a historical load in progress).
    /// </summary>
    public async Task<bool> PumpAsync(ILogSource source, EventEncoder encoder, ISink sink, CaptureSettings settings, CancellationToken ct,
                                      ISnapshotSource? snapshots = null, IGapLog? gaps = null)
    {
        try { return await PumpCoreAsync(source, encoder, sink, settings, snapshots, gaps, ct); }
        catch (DatabaseChangedException)
        {
            Supersede();
            throw;
        }
        catch (LogTruncatedException ex)
        {
            await ResumeAfterTruncationAsync(source, settings, gaps, ex, ct);
            return true;
        }
        catch (IncompleteTransactionException ex)
        {
            lock (_gate)
            {
                _rereadFor = ex.Transaction.CommitLsn;
                ResetToCheckpoint();
                if (_scanFrom is null || ex.Transaction.BeginLsn < _scanFrom) _scanFrom = ex.Transaction.BeginLsn;
                _schemaStale = true; // the layouts must reach back as far
            }
            return true;
        }
    }

    /// <summary>Ends this capture: it is not pumped again, and nothing more is saved or released for it.</summary>
    public void Supersede() { lock (_gate) Superseded = true; }

    /// <summary>
    /// The log no longer reaches back to where this stream must continue. If the held-back transaction is still unreleased, the truncated
    /// part held no captured change that was not already acknowledged, and reading goes on at the oldest record. Otherwise captured changes
    /// may be gone: that is recorded durably before the stream moves on - never skipped silently.
    /// </summary>
    private async Task ResumeAfterTruncationAsync(ILogSource source, CaptureSettings settings, IGapLog? gaps, LogTruncatedException truncated, CancellationToken ct)
    {
        Checkpoint position;
        bool published;
        lock (_gate)
        {
            published = !_silent;
            ResetToCheckpoint();
            position = _progress is { } p && p.LastCommitLsn > (_durable?.LastCommitLsn ?? default) ? p
                     : _durable ?? _baseline ?? new Checkpoint(Name, Incarnation, default, _ackedCommit, _ackedBegin, DateTimeOffset.UtcNow);
        }
        // nothing was promised yet (and SQL Server was not holding log for us either): start again
        if (_durable is null && _baseline is null && _progress is null && _rereadFor.IsZero && (!published || EventsPublished == 0) && (_holdFrom.IsZero || truncated.Oldest <= _holdFrom)) return;
        var (continuity, detail) = await source.CheckContinuityAsync(Database, position, ct);
        if (continuity != LogContinuity.Intact)
        {
            var gap = new CaptureGap(Name, Incarnation, position.LastCommitLsn, truncated.Oldest, continuity, detail, DateTimeOffset.UtcNow);
            if (gaps is not null) await gaps.RecordAsync(gap, settings.OnGap == GapPolicy.Resync, ct);
            Gaps++;
            _unreported.Enqueue(gap);
        }
        lock (_gate)
        {
            _scanFrom = truncated.Oldest;
            _resumedAt = truncated.Oldest;
            _resumedInGap = continuity != LogContinuity.Intact;
            if (_resumedInGap) _held = false; // the bookmark is gone; the next captured change becomes the new one
            _schemaStale = true;
        }
    }

    private async Task<bool> PumpCoreAsync(ILogSource source, EventEncoder encoder, ISink sink, CaptureSettings settings, ISnapshotSource? snapshots, IGapLog? gaps, CancellationToken ct)
    {
        lock (_gate)
        {
            if (Superseded) return false;
            if (_durable is null && _baseline is not null) return false; // held until the start point is durable (MarkDurable re-arms)
            State = CaptureState.Running;
        }
        _gapLog = gaps;
        _onGap = settings.OnGap;
        // a "Latest" catch-up decodes nothing: layouts are loaded when it ends, without the history of the log it skipped
        if (!_silent && (_schemaStale || _schema is null)) await ReloadSchemaAsync(source, null, ct);
        if (snapshots is not null && !_silent) await StartChunkAsync(snapshots, ct);
        var committed = new List<CommittedTransaction>();
        var rows = 0;
        var readyEvents = 0;
        await foreach (var r in source.ReadAsync(Database, _scanFrom, settings.MaxRowsPerRead, ct))
        {
            rows++;
            // Only the start point is durable, and the read began wherever the log begins now. The log has no holes, so it must reach back to that
            // point; otherwise somebody else released it before the first read.
            if (rows == 1 && _scanFrom is null && !_skipThrough.IsZero && r.Lsn > new Lsn(_skipThrough.Vlf, _skipThrough.Block, (ushort)(_skipThrough.Slot + 1)))
                throw new LogTruncatedException(_skipThrough, r.Lsn);
            lock (_gate)
            {
                if (r.Operation == LogOperation.ObjectsChanged)
                {
                    _changingObjects.Add(r.TransactionId);
                    _lastLsn = r.Lsn;
                    continue;
                }
                // the first record of a read may be the last of the one before: its transaction is not in the set any more
                if (_changingObjects.Count > 0 && r.Operation is LogOperation.CommitXact or LogOperation.AbortXact && _changingObjects.Remove(r.TransactionId) && r.Operation == LogOperation.CommitXact)
                    ObjectsMayHaveChanged(r.Lsn);
                if (r.Lsn == _lastLsn) continue; // a read starts with the last record of the one before (anything older: out of order, an error)
                _assembler.Process(r, committed);
                _lastLsn = r.Lsn;
                _preparing.AddRange(committed); // not for a moment out of sight: a checkpoint taken now must not claim it
            }
            RecordsRead++;
            if (_assembler.BufferedBytes > settings.MaxBufferedBytes)
                throw new InvalidOperationException($"{Name}: {_assembler.BufferedBytes} bytes of uncommitted row images buffered (limit {settings.MaxBufferedBytes})");
            foreach (var txn in committed)
            {
                // the stream reached the chunk's high watermark: the surviving chunk rows go out before this transaction
                if (_snapshot?.Pending is { } chunk && txn.CommitLsn >= chunk.High) readyEvents += CompleteChunk();
                try { readyEvents += await PrepareAsync(source, txn, ct); }
                catch (Exception ex) when (ex is not DatabaseChangedException && !ct.IsCancellationRequested)
                {
                    // what committed before it does not wait for it (for ever, if it stays like that), nor is it lost if it is given up
                    if (_ready.Count > 0)
                    {
                        await source.VerifyAsync(Database, ct);
                        await SendReadyAsync(encoder, sink, settings, ct);
                        lock (_gate) if (!_ackedCommit.IsZero && (_durable is not null || _baseline is null)) _progress = Position();
                    }
                    throw;
                }
            }
            committed.Clear();
            if (readyEvents >= settings.MaxBatchEvents)
            {
                await source.VerifyAsync(Database, ct); // the read itself only says so at its end
                await SendReadyAsync(encoder, sink, settings, ct);
                readyEvents = 0;
            }
        }
        if (rows < settings.MaxRowsPerRead && _snapshot?.Pending is not null) readyEvents += CompleteChunk(); // caught up past H
        await SendReadyAsync(encoder, sink, settings, ct);
        LastPumpRows = rows;
        lock (_gate) _unhardened = source.Unhardened;
        if (!_lastLsn.IsZero) _scanFrom = _lastLsn;
        var caughtUp = rows < settings.MaxRowsPerRead;
        if (caughtUp && _silent) await ReloadSchemaAsync(source, null, ct);
        bool snapshotWork;
        lock (_gate)
        {
            if (caughtUp && _silent)
            {
                // StartPosition.Latest: the catch-up is done. Publishing starts once this point is durable.
                _silent = false;
                var restart = _assembler.RestartLsn ?? (!_lastLsn.IsZero ? _lastLsn : _scanFrom ?? default);
                _baseline = new Checkpoint(Name, Incarnation, restart, _ackedCommit, _ackedBegin, DateTimeOffset.UtcNow, _held);
            }
            // a load in progress keeps the pump going, except while a failed chunk read waits for its retry
            snapshotWork = !_silent && (_snapshot is { } a ? a.Pending is not null || a.RetryAt <= DateTimeOffset.UtcNow : _snapshots.Count > 0);
            _caughtUp = caughtUp;
            State = CaptureState.Idle; ConsecutiveFaults = 0; Fault = null;
        }
        return !caughtUp || snapshotWork;
    }

    /// <summary>DDL that committed before the layouts were read is already in them, and records from before it are decoded with the
    /// layout history (no reload; this also makes catch-up through old DDL - e.g. CDC being enabled - free: measured ~120 ms per
    /// catalog read).</summary>
    private bool IsNewerThanSchema(CommittedTransaction txn) => _schema is null || _schema.AsOf.IsZero || txn.CommitLsn >= _schema.AsOf;

    /// <summary>Loads layouts valid for every record that may still be decoded: those of open transactions, of the transaction being
    /// prepared, and everything after the scan position.</summary>
    private async Task ReloadSchemaAsync(ILogSource source, CommittedTransaction? preparing, CancellationToken ct, IReadOnlySet<long>? unresolved = null)
    {
        Lsn? from;
        lock (_gate)
        {
            from = preparing is null ? _scanFrom : _lastLsn;
            if (_assembler.RestartLsn is { } open && from is { } f && open < f) from = open;
            if (preparing is { Changes.Count: > 0 } && from is { } g && preparing.Changes[0].Lsn < g) from = preparing.Changes[0].Lsn;
            if (preparing is { HasSchemaChange: true } && from is { } d && preparing.BeginLsn < d) from = preparing.BeginLsn; // what the DDL did to the catalog
        }
        var schema = await source.LoadSchemaAsync(Database, from, unresolved ?? new HashSet<long>(), ct);
        SchemaLoads++;
        DatabaseSchema? previous;
        Checkpoint? durable;
        Lsn acknowledged;
        lock (_gate) (previous, durable, acknowledged) = (_schema, _durable, _ackedCommit);
        if (previous is not null || durable is not null)
        {
            var position = durable is null ? default : durable.LastCommitLsn > durable.RestartLsn ? durable.LastCommitLsn : durable.RestartLsn;
            foreach (var (table, start, known) in schema.CapturedLater(previous, position)) await CapturedLaterAsync(table, start, known, ct);
            foreach (var (table, at) in schema.Switched(previous, Lsn.Max(acknowledged, _switchesThrough)))
            {
                await RecordGapAsync(new CaptureGap(Name, Incarnation, acknowledged, at, LogContinuity.Switched,
                    "rows came into or went out of the table by ALTER TABLE ... SWITCH; none of them is in the log", DateTimeOffset.UtcNow, $"{table.Schema}.{table.Table}"), ct);
            }
            if (schema.History.Moves.Count > 0) _switchesThrough = Lsn.Max(_switchesThrough, schema.History.Moves.Max(m => m.At));
            foreach (var what in schema.Rewritten(previous)) _notices.Enqueue(what + ". Consumers have to follow the DDL, or load the table again: rows nobody wrote are not published.");
            foreach (var t in schema.NoLongerCaptured(previous)) _notices.Enqueue($"{t} is NOT captured any more (dropped, or CDC was disabled)");
        }
        lock (_gate) { _schema = schema; _schemaStale = _namesStale = false; }
    }

    /// <summary>Recorded before the new layouts are used; a failure to record faults the stream and is tried again.</summary>
    private async Task CapturedLaterAsync(TableLayout table, Lsn start, bool known, CancellationToken ct)
    {
        Lsn acked;
        lock (_gate) acked = _ackedCommit;
        var gap = new CaptureGap(Name, Incarnation, acked, start, known ? LogContinuity.Broken : LogContinuity.NewTable,
            known ? "CDC was disabled on the table and enabled again: changes in between were not captured"
                  : $"the table is captured from {start} on: rows and changes from before (or while CDC was disabled) were not published",
            DateTimeOffset.UtcNow, $"{table.Schema}.{table.Table}");
        await RecordGapAsync(gap, ct);
    }

    private async Task RecordGapAsync(CaptureGap gap, CancellationToken ct)
    {
        if (_gapLog is not null) await _gapLog.RecordAsync(gap, _onGap == GapPolicy.Resync, ct);
        Gaps++;
        _unreported.Enqueue(gap);
    }

    /// <summary>Decodes a committed transaction into the ready list (no events when skipped). Returns its event count.</summary>
    private async Task<int> PrepareAsync(ILogSource source, CommittedTransaction txn, CancellationToken ct)
    {
        List<ChangeEvent> events;
        var skipped = txn.CommitLsn <= _skipThrough || _silent;
        var cut = !skipped && !txn.Complete && txn.BeginLsn < _resumedAt && txn.RecordsSeen < txn.RecordsLogged;
        if (cut && !_resumedInGap)
        {
            // the log was truncated up to the bookmark, which should have kept this transaction whole - but its beginning is gone
            await RecordGapAsync(new CaptureGap(Name, Incarnation, _ackedCommit, _resumedAt, LogContinuity.Unknown,
                $"transaction {Log.LogRecord.FormatTransactionId(txn.TransactionId)} (commit {txn.CommitLsn}) began at {txn.BeginLsn}, before the oldest record that is still in the log", DateTimeOffset.UtcNow), ct);
            _resumedInGap = true;
        }
        if (!skipped && !cut && !txn.Complete)
        {
            if (_rereadFor != txn.CommitLsn && txn.RecordsSeen < txn.RecordsLogged) throw new IncompleteTransactionException(txn);
            throw new UndecodableChangeException($"{Name}: SQL Server logged {txn.RecordsLogged} replicated records for transaction {Log.LogRecord.FormatTransactionId(txn.TransactionId)} " +
                                                 $"(begin {txn.BeginLsn}, commit {txn.CommitLsn}), but {txn.RecordsSeen} were read");
        }
        if (skipped || cut)
        {
            // published before a restart (or before the "Latest" start point, or cut by a gap): acknowledged in order, nothing published
            if (txn.HasSchemaChange && IsNewerThanSchema(txn)) _schemaStale = true;
            events = [];
        }
        else
        {
            // a transaction that changed the schema may also have changed rows before and after the DDL: load first, then each
            // record finds the layout that was valid at its own LSN
            if (_schemaStale || _namesStale || (txn.HasSchemaChange && IsNewerThanSchema(txn))) await ReloadSchemaAsync(source, txn, ct);
            events = await BuildAsync(source, txn, ct);
        }
        // rows read with the layout from before this DDL would be missing (or misreading) columns: read that chunk again
        if (txn.HasSchemaChange && _snapshot?.Pending is not null) _snapshot.Start(null);
        if (_snapshot?.Pending is not null)
            foreach (var e in events) _snapshot.Observe(txn.CommitLsn, e.Table, e.Before, e.After);
        lock (_gate)
        {
            _ready.Add(new Ready(txn.BeginLsn, txn.CommitLsn, events));
            _preparing.Remove(txn);
            if (_rereadFor == txn.CommitLsn) _rereadFor = default;
        }
        return events.Count;
    }

    /// <summary>Reads the next chunk of the active (or next queued) snapshot, unless one is pending.</summary>
    private async Task StartChunkAsync(ISnapshotSource snapshots, CancellationToken ct)
    {
        TableSnapshot? s;
        lock (_gate)
        {
            if (_snapshot is { Done: true }) _snapshot = null;
            if (_snapshot is null && _snapshots.Count > 0) _snapshot = _snapshots.Dequeue();
            s = _snapshot;
        }
        if (s is null || s.Pending is not null || s.Done || s.RetryAt > DateTimeOffset.UtcNow) return;
        var layout = _schema?.FindTable(s.Table.ObjectId);
        SnapshotChunk chunk;
        try
        {
            if (layout is null) throw new InvalidOperationException($"{s.Table} is no longer captured (dropped, or CDC disabled)");
            chunk = await snapshots.ReadChunkAsync(layout, s.NextAfterKey, s.ChunkSize, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // a failing table read must not fault the stream: the load retries with backoff and fails on its own after a few tries
            s.ChunkFailed(ex, DateTimeOffset.UtcNow);
            if (s.Done)
            {
                lock (_gate) if (ReferenceEquals(_snapshot, s)) _snapshot = null;
                SnapshotProgress?.Invoke(s);
            }
            return;
        }
        s.Start(chunk);
    }

    /// <summary>Emits the surviving rows of the pending chunk as snapshot reads (op "r").</summary>
    private int CompleteChunk()
    {
        var s = _snapshot!;
        if (s.MustReadAgain) { s.Start(null); return 0; }
        var high = s.Pending!.High;
        var (chunkId, read) = (s.ChunkSeq, s.ReadId);
        var (rows, lastKey, isLast) = s.Complete();
        var events = new List<ChangeEvent>(rows.Count);
        foreach (var row in rows)
            events.Add(new ChangeEvent
            {
                Table = row.Layout, Op = ChangeOp.Read, After = row, Lsn = high, CommitLsn = high, Sequence = events.Count,
                TransactionSize = rows.Count, SnapshotId = $"{s.SnapshotId}.{chunkId}", SnapshotRead = read, Incarnation = Incarnation,
            });
        lock (_gate) _ready.Add(new Ready(high, high, events, IsSnapshot: true, OnAcknowledged: () => AcknowledgeChunk(s, lastKey, isLast, events.Count)));
        return events.Count;
    }

    /// <summary>The sink acknowledged a chunk's rows: move the snapshot's resume key; a finished snapshot frees the slot for the next.</summary>
    private void AcknowledgeChunk(TableSnapshot s, object?[]? lastKey, bool isLast, int rows)
    {
        s.Acknowledge(lastKey, isLast, rows);
        lock (_gate) if (s.Done && ReferenceEquals(_snapshot, s)) _snapshot = null;
        SnapshotProgress?.Invoke(s);
    }

    /// <summary>
    /// Publishes every ready transaction and waits for its acknowledgement before returning: at most one unacknowledged call per database,
    /// so nothing can overtake a failed batch. A failed acknowledgement throws (the stream faults and rewinds to its durable checkpoint).
    /// A transaction of any size is sent in calls of a bounded size; it counts as acknowledged when all of them are.
    /// </summary>
    private async Task SendReadyAsync(EventEncoder encoder, ISink sink, CaptureSettings settings, CancellationToken ct)
    {
        List<Ready> batch;
        lock (_gate) batch = [.. _ready];
        if (batch.Count == 0) return;
        foreach (var events in batch.SelectMany(r => r.Events).Chunk(Math.Max(1, settings.MaxBatchEvents)))
        {
            // stopping does not wait for a destination that does not answer: nothing unacknowledged is ever counted as delivered
            var ack = await sink.SendAsync(encoder.Encode(events, sink.MaxPayloadBytes), ct).AsTask().WaitAsync(ct);
            await ack.WaitAsync(ct);
            EventsPublished += events.Length;
            LastCommitTime = events[^1].CommitTime ?? LastCommitTime;
            LastPublishedAt = DateTimeOffset.UtcNow;
        }
        TransactionsPublished += batch.Count(r => r.Events.Count > 0);
        foreach (var r in batch) r.OnAcknowledged?.Invoke(); // snapshot progress moves only behind acknowledgements
        lock (_gate)
        {
            _ready.RemoveRange(0, batch.Count);
            if (batch.LastOrDefault(r => !r.IsSnapshot) is { } lastTxn) // snapshot rows never move the stream position
            {
                // re-reading behind the restart point passes over what was published before: that must not take the position back
                if (lastTxn.Commit > _ackedCommit)
                {
                    _held = lastTxn.Begin >= _holdFrom; // a new newest transaction: not released by us
                    _ackedCommit = lastTxn.Commit;
                    _ackedBegin = lastTxn.Begin;
                }
            }
        }
    }

    /// <summary>With layouts loaded again as often as they turn out to lack something new. Whatever still cannot be attributed to a layout then
    /// stops the stream: nothing is ever skipped silently.</summary>
    private async Task<List<ChangeEvent>> BuildAsync(ILogSource source, CommittedTransaction txn, CancellationToken ct)
    {
        var unresolved = new HashSet<long>();
        var reloaded = false;
        while (true)
        {
            try { return Build(txn); }
            catch (UnknownRowsetException ex) when (unresolved.Add(ex.RowsetId)) { }
            catch (SchemaMismatchException) when (!reloaded) { reloaded = true; }
            catch (Exception ex) when (ex is UnknownRowsetException or SchemaMismatchException)
            {
                throw new UndecodableChangeException($"{Name}: transaction {Log.LogRecord.FormatTransactionId(txn.TransactionId)} (commit {txn.CommitLsn}) changed a row " +
                                                     $"whose layout cannot be determined: {ex.Message}", ex);
            }
            await ReloadSchemaAsync(source, txn, ct, unresolved);
        }
    }

    private List<ChangeEvent> Build(CommittedTransaction txn)
    {
        var layouts = new TableLayout?[txn.Changes.Count];
        var size = 0;
        for (var i = 0; i < layouts.Length; i++)
        {
            var c = txn.Changes[i];
            layouts[i] = _schema?.Find(c.PartitionId, c.Lsn);
            if (layouts[i] is not null) size++;
            else if (_schema?.IsNotCaptured(c.PartitionId) != true) throw c.Kind == ChangeKind.Unknown
                ? new UndecodableChangeException($"{Name}: {c.Description} in transaction {Log.LogRecord.FormatTransactionId(txn.TransactionId)} (commit {txn.CommitLsn})")
                : new UnknownRowsetException(c.PartitionId, c.Lsn);
        }
        SkippedForeignRecords += layouts.Length - size;
        var events = new List<ChangeEvent>(size);
        for (var i = 0; i < layouts.Length; i++)
        {
            var c = txn.Changes[i];
            if (layouts[i] is not { } layout) continue;
            if (c.Kind == ChangeKind.Unknown) throw new UndecodableChangeException($"{layout}: {c.Description} in transaction {Log.LogRecord.FormatTransactionId(txn.TransactionId)} (commit {txn.CommitLsn})");
            if (layout.UnsupportedReason is { } why) throw new NotSupportedException($"{layout}: {why}");
            if (c.Kind == ChangeKind.PartialUpdate)
                throw new NotSupportedException($"{layout}: partial update image (table not CDC-enabled); enable CDC on the table or use the lookup policy");
            var before = c.Before is { Length: > 0 } && c.Kind is ChangeKind.Delete or ChangeKind.Update ? RowDecoder.Decode(c.Before, layout, c.PossiblyTruncated) : null;
            var after = c.After is { Length: > 0 } && c.Kind is ChangeKind.Insert or ChangeKind.Update ? RowDecoder.Decode(c.After, layout, c.PossiblyTruncated) : null;
            events.Add(new ChangeEvent
            {
                Table = layout,
                Op = c.Kind switch { ChangeKind.Insert => ChangeOp.Create, ChangeKind.Delete => ChangeOp.Delete, _ => ChangeOp.Update },
                Before = before,
                After = after,
                Lsn = c.Lsn,
                CommitLsn = txn.CommitLsn,
                BeginLsn = txn.BeginLsn,
                CommitTime = txn.CommitTime,
                TransactionId = txn.TransactionId,
                Sequence = i, // the position among all changes the log has: it does not depend on what is (still) captured when it is read
                TransactionSize = size,
                Incarnation = Incarnation,
            });
        }
        return events;
    }

    public override string ToString() => $"{Name} ({DatabaseId})";
}

/// <summary>Persists advanced checkpoints, then - strictly behind durable checkpoints - lets the source release its log.</summary>
public static class CheckpointCoordinator
{
    public static async Task FlushAsync(IReadOnlyCollection<DatabaseCapture> captures, ICheckpointStore store, ILogReleaser? releaser,
        TimeSpan releaseInterval, Action<DatabaseCapture, Exception>? onReleaseError, CancellationToken ct)
    {
        await SaveAsync(captures, store, ct);
        if (releaser is not null) await ReleaseAsync(captures, releaser, releaseInterval, onReleaseError, ct);
    }

    public static async Task SaveAsync(IReadOnlyCollection<DatabaseCapture> captures, ICheckpointStore store, CancellationToken ct)
    {
        var batch = new List<(DatabaseCapture Capture, Checkpoint Cp)>();
        foreach (var c in captures)
            if (c.PendingCheckpoint() is { } cp) batch.Add((c, cp));
        if (batch.Count > 0)
        {
            await store.SaveAsync(batch.Select(b => b.Cp).ToArray(), ct);
            foreach (var (c, cp) in batch) c.MarkDurable(cp);
        }
    }

    /// <summary>
    /// Strictly behind what <see cref="SaveAsync"/> made durable, and not before it has been that for <paramref name="releaseInterval"/>: an
    /// acknowledgement may mean no more than that the destination has the event in memory (JetStream between two syncs). What it loses when its
    /// host loses power is then still in SQL Server's log.
    /// </summary>
    public static async Task ReleaseAsync(IReadOnlyCollection<DatabaseCapture> captures, ILogReleaser releaser, TimeSpan releaseInterval,
        Action<DatabaseCapture, Exception>? onReleaseError, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var c in captures)
        {
            if (c.ReleasedByOthers || c.Releasable is not { } target || target.Commit <= c.ReleasedThrough) continue;
            if (now - c.ReleasedAt < releaseInterval) continue;
            if (releaseInterval > TimeSpan.Zero)
            {
                var aged = c.ReleaseNext;
                c.ReleaseNext = target;
                c.ReleasedAt = now;
                if (aged is not { } due || due.Commit > target.Commit || due.Commit <= c.ReleasedThrough) continue;
                target = due;
            }
            try
            {
                await releaser.ReleaseAsync(c.Database, target.Begin, target.Commit, ct);
                c.ReleasedThrough = target.Commit;
                c.ReleasedAt = now;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                c.ReleasedAt = now;
                onReleaseError?.Invoke(c, ex);
            }
        }
    }
}
