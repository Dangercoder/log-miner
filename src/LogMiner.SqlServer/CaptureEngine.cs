using System.Collections.Concurrent;
using System.Threading.Channels;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LogMiner.SqlServer;

public sealed class CaptureOptions
{
    public required string ConnectionString { get; init; }
    public CaptureScope Scope { get; init; } = CaptureScope.CdcTrackedTables;
    public CaptureSettings Settings { get; init; } = new();
    /// <summary>Concurrent log readers (one SQL connection each). Databases are multiplexed over them.</summary>
    public int Workers { get; init; } = 8;
    public TimeSpan CheckpointInterval { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>How often each database's log is released with sp_repldone (always behind durable checkpoints).</summary>
    public TimeSpan ReleaseInterval { get; init; } = TimeSpan.FromSeconds(10);
    public bool ReleaseLog { get; init; } = true;
    /// <summary>Safety net: every database is re-read at this interval even without a signal.</summary>
    public TimeSpan SweepInterval { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan DatabaseRefreshInterval { get; init; } = TimeSpan.FromSeconds(60);
    /// <summary>Include filter on database names (null = all CDC-enabled user databases).</summary>
    public Func<string, bool>? DatabaseFilter { get; init; }
    /// <summary>On-demand historical loads from logminer.dbo.snapshot_requests (null = API only).</summary>
    public SnapshotRequests? SnapshotRequests { get; init; }
    /// <summary>Durable record of gaps (null = log only).</summary>
    public IGapLog? Gaps { get; init; }
    /// <summary>The newest captured transaction of every database is held back as a bookmark. When the log it holds grows beyond this
    /// (heavy activity on tables that are not captured), it is released too.</summary>
    public int MaxHeldLogMb { get; init; } = 64;
    /// <summary>How often the held log's size and the bookmarks (someone else releasing the log) are checked.</summary>
    public TimeSpan HoldCheckInterval { get; init; } = TimeSpan.FromSeconds(60);
}

/// <summary>
/// Multiplexes every database of an instance over a small pool of log readers. A database is (re)queued when the activity
/// signal reports log writes for it, when a read returned a full batch, after a fault's back-off, or on the periodic sweep.
/// </summary>
public sealed class CaptureEngine(CaptureOptions options, ILogActivitySignal signal, EventEncoder encoder, ISink sink, ICheckpointStore checkpoints, ILogger? logger = null) : IAsyncDisposable
{
    private readonly ILogger _log = logger ?? NullLogger.Instance;
    private readonly ConcurrentDictionary<int, DatabaseCapture> _captures = new();
    private readonly Channel<DatabaseCapture> _queue = Channel.CreateUnbounded<DatabaseCapture>();
    private readonly SqlLogReleaser _releaser = new(options.ConnectionString);
    private readonly SqlSnapshotSource _snapshotSource = new(options.ConnectionString);
    private readonly ConcurrentDictionary<long, ActiveRequest> _activeRequests = new();
    private readonly ConcurrentDictionary<string, string> _notOpened = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _missing = new(); // captured before, gone or closed now: looked for more often for a while
    private readonly ConcurrentDictionary<string, DatabaseCapture> _pumping = new(); // by name: what identifies a stream at the destination
    private CancellationTokenSource? _cts;
    private Task? _run;
    private readonly TaskCompletionSource _failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile bool _refreshWanted;

    public IReadOnlyCollection<DatabaseCapture> Captures => _captures.Values.ToArray();
    public long SignalFailures { get; private set; }
    /// <summary>Reads that did not reach the log block they were started for (and were repeated).</summary>
    public long BellRetries { get; private set; }
    /// <summary>Databases whose log was released by someone else (last check). Changes will be lost if that goes on while the miner is behind.</summary>
    public IReadOnlyCollection<string> ForeignReleases { get; private set; } = [];
    /// <summary>Recent (queue wait, pump duration) samples in milliseconds, for diagnostics.</summary>
    public ConcurrentQueue<(double QueueMs, double PumpMs, int Rows, long SchemaLoads, string Db)> Samples { get; } = new();
    public ISink Sink => sink;
    /// <summary>Why no log is released at the moment: the destination does not hold what it acknowledged.</summary>
    public string? AuditProblem { get; private set; }
    /// <summary>Databases that should be captured and cannot be opened (name, reason). They are tried again; SQL Server keeps their log.</summary>
    public IReadOnlyCollection<(string Database, string Reason)> NotOpened => _notOpened.Select(x => (x.Key, x.Value)).ToArray();
    /// <summary>Databases the filter includes that cannot be captured as they are (offline, read-only, CDC not enabled), as of the last refresh.</summary>
    public IReadOnlyCollection<(string Database, string Reason)> NotCapturable { get; private set; } = [];
    /// <summary>The longest a read-and-publish has been running: a destination or a server that does not answer.</summary>
    public (string Database, TimeSpan Since)? LongestPump =>
        _pumping.IsEmpty ? null : _pumping.Values.Select(c => (c.Name, System.Diagnostics.Stopwatch.GetElapsedTime(c.PumpStarted))).OrderByDescending(x => x.Item2).Select(x => ((string, TimeSpan)?)x).FirstOrDefault();
    /// <summary>Faults when a part of the engine has stopped that must not stop: the rest would go on without it and nobody would notice
    /// (no checkpoints, no retries, no doorbell). Never completes otherwise.</summary>
    public Task Failure => _failure.Task;

    public async Task StartAsync(CancellationToken ct)
    {
        if (options.Scope != CaptureScope.CdcTrackedTables) // rows a page split moves are logged by a transaction of its own: they would be published as inserts, committed or not
            throw new NotSupportedException("only tables with CDC enabled can be captured");
        if (options.Workers < 1 || options.Settings.MaxRowsPerRead < 2 || options.Settings.MaxBatchEvents < 1 || options.Settings.MaxBufferedBytes < 1)
            throw new ArgumentException("Workers and MaxBatchEvents must be at least 1, MaxRowsPerRead at least 2 (a read starts with the last record of the one before)");
        if (options.CheckpointInterval <= TimeSpan.Zero || options.SweepInterval <= TimeSpan.Zero || options.HoldCheckInterval <= TimeSpan.Zero || options.DatabaseRefreshInterval <= TimeSpan.Zero)
            throw new ArgumentException("intervals must be positive");
        await sink.StartAsync(ct);
        await RefreshDatabasesAsync(ct);
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _cts.Token;
        var tasks = new List<Task>
        {
            SuperviseAsync("the doorbell", SignalLoopAsync, token),
            SuperviseAsync("checkpointing", CheckpointLoopAsync, token),
            SuperviseAsync("releasing log", ReleaseLoopAsync, token),
            SuperviseAsync("housekeeping", HousekeepingLoopAsync, token),
            SuperviseAsync("historical load requests", SnapshotRequestLoopAsync, token),
            SuperviseAsync("the held log check", HoldLoopAsync, token),
        };
        for (var i = 0; i < options.Workers; i++)
        {
            var id = i;
            tasks.Add(SuperviseAsync($"worker {id}", t => WorkerLoopAsync(id, t), token));
        }
        _run = Task.WhenAll(tasks);
    }

    /// <summary>Every loop handles the errors it expects and goes on. One that ends all the same takes the engine down loudly.</summary>
    private async Task SuperviseAsync(string what, Func<CancellationToken, Task> loop, CancellationToken ct)
    {
        Exception? error = null;
        try { await Task.Run(() => loop(ct), CancellationToken.None); }
        catch (Exception ex) { error = ex; }
        if (ct.IsCancellationRequested) return;
        _log.LogCritical(error, "{What} stopped; capture cannot go on without it", what);
        _failure.TrySetException(error ?? new InvalidOperationException($"{what} stopped"));
    }

    public async Task StopAsync()
    {
        if (_cts is null) return;
        await _cts.CancelAsync();
        // cancelling in-flight SqlClient commands surfaces as SqlException/InvalidOperationException, not only as
        // OperationCanceledException: after cancellation every loop exit is a clean stop
        try { if (_run is not null) await _run; }
        catch (Exception ex) when (_cts.IsCancellationRequested) { _log.LogDebug(ex, "loop ended during shutdown"); }
        try { await FlushAsync(releaseLog: false, CancellationToken.None); }
        catch (Exception ex) { _log.LogWarning(ex, "the final checkpoint could not be saved; what was published since the last one is published again"); }
        _cts.Dispose();
        _cts = null;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _releaser.DisposeAsync();
    }

    private void MarkDirty(DatabaseCapture c)
    {
        c.Dirty = true;
        if (c.TryMarkQueued()) _queue.Writer.TryWrite(c);
    }

    private bool IsCurrent(DatabaseCapture c) => _captures.TryGetValue(c.DatabaseId, out var current) && ReferenceEquals(current, c);

    /// <summary>The database was renamed, dropped, taken offline or restored. Whatever has its id or its name now is found by the next refresh.</summary>
    private void Superseded(DatabaseCapture c, Exception why)
    {
        c.Supersede();
        if (!_captures.TryRemove(KeyValuePair.Create(c.DatabaseId, c))) return;
        _missing[c.Name] = DateTimeOffset.UtcNow;
        _log.LogWarning("{Why}; looking for it again", why.Message);
        _refreshWanted = true;
    }

    private async Task SignalLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var changed in signal.WatchAsync(ct))
                {
                    if (changed.Count == 0) { foreach (var c in _captures.Values) MarkDirty(c); continue; }
                    foreach (var (id, block) in changed)
                    {
                        if (!_captures.TryGetValue(id, out var c)) continue;
                        c.Ring(block);
                        MarkDirty(c);
                    }
                }
            }
            catch (Exception) when (ct.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                // Never degrade silently to the slow sweep: log loudly and restart the signal.
                SignalFailures++;
                _log.LogError(ex, "activity signal failed; restarting it (until then only the {Sweep} sweep detects changes)", options.SweepInterval);
                try { await Task.Delay(TimeSpan.FromSeconds(1), ct); } catch (OperationCanceledException) { return; }
            }
        }
    }

    private async Task WorkerLoopAsync(int id, CancellationToken ct)
    {
        SqlConnection? conn = null;
        try
        {
            await foreach (var c in _queue.Reader.ReadAllAsync(ct))
            {
                var more = false;
                // one stream, one pump: a capture that was replaced may still be finishing its last one
                if (!_pumping.TryAdd(c.Name, c))
                {
                    c.ClearQueued();
                    _ = Task.Delay(50, ct).ContinueWith(_ => MarkDirty(c), ct, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
                    continue;
                }
                c.PumpStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                try
                {
                    if (!IsCurrent(c) || (c.State == CaptureState.Faulted && DateTimeOffset.UtcNow < c.RetryAt)) continue;
                    var started = System.Diagnostics.Stopwatch.GetTimestamp();
                    var queueMs = System.Diagnostics.Stopwatch.GetElapsedTime(c.QueuedAt, started).TotalMilliseconds;
                    c.Dirty = false;
                    if (conn is null || conn.State != System.Data.ConnectionState.Open)
                    {
                        var broken = conn;
                        conn = null;
                        if (broken is not null) await broken.DisposeAsync();
                        conn = new SqlConnection(options.ConnectionString);
                        await conn.OpenAsync(ct);
                    }
                    var loadsBefore = c.SchemaLoads;
                    more = await c.PumpAsync(new EngineLogSource(conn, options.ConnectionString, options.Scope), encoder, sink, options.Settings, ct, _snapshotSource, options.Gaps);
                    ReportGap(c);
                    if (c.SchemaLoads != loadsBefore && c.UntrackedTables.Count > 0)
                        _log.LogWarning("{Database}: {Count} user table(s) are NOT CDC-enabled and their changes are NOT captured: {Tables}. Enable CDC on them (or exclude the database).",
                            c.Name, c.UntrackedTables.Count, string.Join(", ", c.UntrackedTables.Take(20)));
                    if (c.SchemaLoads != loadsBefore)
                        foreach (var t in c.Schema?.Tables.Where(t => t.UnsupportedReason is not null).DistinctBy(t => t.ObjectId) ?? [])
                            _log.LogError("{Table} cannot be captured: {Why}. The first change to it will stop this database's stream.", t, t.UnsupportedReason);
                    Samples.Enqueue((queueMs, System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, c.LastPumpRows, c.SchemaLoads - loadsBefore, c.Name));
                    while (Samples.Count > 200_000) Samples.TryDequeue(out _);
                }
                catch (Exception) when (ct.IsCancellationRequested) { return; } // shutdown, not a fault: the checkpoint is untouched
                catch (DatabaseChangedException ex)
                {
                    ReportGap(c);
                    Superseded(c, ex);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "worker {Worker}: {Database} faulted; rewinding to its last durable checkpoint", id, c.Name);
                    ReportGap(c);
                    c.SetFault(ex);
                    if (ex is SqlException or InvalidOperationException && conn is not null)
                    {
                        try { await conn.DisposeAsync(); }
                        catch (Exception e) when (e is SqlException or InvalidOperationException) { _log.LogDebug(e, "closing a broken connection"); }
                        conn = null;
                    }
                }
                finally
                {
                    _pumping.TryRemove(c.Name, out _);
                    c.ClearQueued();
                    if (!IsCurrent(c)) { }
                    else if ((more || c.Dirty) && c.State != CaptureState.Faulted) MarkDirty(c);
                    else if (c.State != CaptureState.Faulted && c.RetryForBell() is { } wait)
                    {
                        BellRetries++;
                        _ = Task.Delay(wait, ct).ContinueWith(_ => MarkDirty(c), ct, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
                    }
                }
            }
        }
        finally { if (conn is not null) await conn.DisposeAsync(); }
    }

    private void ReportGap(DatabaseCapture c)
    {
        while (c.TryTakeNotice(out var notice)) _log.LogWarning("{Notice}", notice);
        while (c.TryTakeGap(out var gap))
        {
            if (gap.Continuity == LogContinuity.NewTable)
                _log.LogWarning("{Database}.{Table}: {Reason}. Policy: {Policy}.", c.Name, gap.Table, gap.Reason, options.Settings.OnGap);
            else
                _log.LogCritical("{Database} {Table}: CAPTURED CHANGES MAY BE MISSING after {After} (resumed at {Resumed}): {Reason}. Policy: {Policy}.",
                    c.Name, gap.Table, gap.AfterCommit, gap.ResumedAt, gap.Reason, options.Settings.OnGap);
        }
    }

    /// <summary>
    /// Keeps the bookmarks honest: releases one whose held log grew too large, and reports databases where somebody else (a CDC capture
    /// job, a Log Reader Agent, a manual sp_repldone) releases log behind the miner's back - while there is still time to stop it.
    /// </summary>
    private async Task HoldLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.HoldCheckInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { await CheckHoldsAsync(ct); }
                catch (Exception ex) when (!ct.IsCancellationRequested) { _log.LogWarning(ex, "checking the held log failed"); }
            }
        }
        catch (Exception) when (ct.IsCancellationRequested) { }
    }

    public async Task CheckHoldsAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(options.ConnectionString);
        await conn.OpenAsync(ct);
        var large = new List<int>();
        await using (var cmd = new SqlCommand("""
            SELECT d.database_id FROM sys.databases d CROSS APPLY sys.dm_db_log_stats(d.database_id) s
            WHERE d.log_reuse_wait = 6 AND d.state = 0 AND s.active_log_size_mb > @mb
            """, conn) { CommandTimeout = 60 })
        {
            cmd.Parameters.AddWithValue("@mb", options.MaxHeldLogMb);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct)) large.Add(r.GetInt32(0));
        }
        foreach (var id in large)
            if (options.ReleaseLog && _captures.TryGetValue(id, out var c) && c.RequestFullRelease())
                _log.LogInformation("{Database}: releasing the held-back transaction too (its log exceeds {Mb} MB and no captured change follows)", c.Name, options.MaxHeldLogMb);

        var foreign = new List<string>();
        foreach (var c in _captures.Values)
        {
            var held = c.Durable;
            Lsn? oldest;
            try
            {
                // CDC disabled on one table, or a table that was never enabled: nothing that is captured says so
                if (c.TakeObjectsChanged()) { c.RefreshSchema(); MarkDirty(c); }
                if (c.Durable is not { Held: true }) continue;
                oldest = await ReplDoneClient.OldestUnreleasedAsync(conn, c.Database, ct);
            }
            catch (DatabaseChangedException ex) { Superseded(c, ex); continue; }
            catch (SqlException ex) when (conn.State == System.Data.ConnectionState.Open) { _log.LogWarning(ex, "{Database}: the bookmark cannot be checked", c.Name); continue; }
            if (held is not { Held: true } || ReplDoneClient.Explain(oldest, held) is not { } why || c.Durable != held) continue; // moved on meanwhile: check again next time
            foreign.Add(c.Name);
            _log.LogError("{Database}: {Why}. Changes will be lost if log is truncated before the miner reads it.", c.Name, why);
        }
        ForeignReleases = foreign;
    }

    private async Task CheckpointLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.CheckpointInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { await FlushAsync(releaseLog: false, ct); }
                catch (Exception) when (ct.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    _log.LogError(ex, "checkpoint flush failed; the log is retained until it succeeds");
                }
            }
        }
        catch (Exception) when (ct.IsCancellationRequested) { }
    }

    /// <summary>Apart from the checkpoints: a release that has to wait in one database must not keep every database's position from being saved.</summary>
    private async Task ReleaseLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.CheckpointInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                if (!options.ReleaseLog) continue;
                try
                {
                    // the point of no return: not on the strength of the acknowledgements alone
                    var due = CheckpointCoordinator.Due(Captures, options.ReleaseInterval, DateTimeOffset.UtcNow);
                    if (due.Count == 0) continue;
                    var problems = await sink.AuditAsync(due.Select(c => c.Name).ToHashSet(StringComparer.Ordinal), ct);
                    AuditProblem = problems.Count == 0 ? null : $"{problems.Count} database(s), e.g. {problems.First().Key}: {problems.First().Value}";
                    if (AuditProblem is not null)
                        _log.LogCritical("THE LOG OF {Count} DATABASE(S) IS NOT RELEASED, e.g. {Database}: {Problem}. SQL Server keeps everything since the last release; consumers may have lost events.", problems.Count, problems.First().Key, problems.First().Value);
                    await ReleaseAsync(due.Where(c => !problems.ContainsKey(c.Name)).ToList(), ct);
                    if (problems.Count > 0) await Task.Delay(TimeSpan.FromSeconds(5), ct);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested) { _log.LogError(ex, "releasing log failed; it is retained until it succeeds"); }
            }
        }
        catch (Exception) when (ct.IsCancellationRequested) { }
    }

    private Task ReleaseAsync(IReadOnlyCollection<DatabaseCapture> captures, CancellationToken ct) => CheckpointCoordinator.ReleaseAsync(captures, _releaser, options.ReleaseInterval,
        (c, ex) =>
        {
            if (ex is DatabaseChangedException) Superseded(c, ex);
            else _log.LogWarning(ex, "sp_repldone failed for {Database} (is a CDC capture job or Log Reader Agent running?)", c.Name);
        }, ct);

    public async Task FlushAsync(bool releaseLog, CancellationToken ct)
    {
        await CheckpointCoordinator.SaveAsync(Captures, checkpoints, ct, sink.ReceiptOf);
        if (releaseLog) await ReleaseAsync(Captures, ct);
        foreach (var c in Captures) if (c.Dirty) MarkDirty(c); // e.g. a "Latest" start whose start point just became durable
    }

    private async Task HousekeepingLoopAsync(CancellationToken ct)
    {
        var lastRefresh = DateTimeOffset.UtcNow;
        var tick = TimeSpan.FromMilliseconds(250);
        var sweepIndex = 0;
        var sweepDue = 0.0; // databases due since the last tick that swept one (fractions add up)
        using var timer = new PeriodicTimer(tick);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var now = DateTimeOffset.UtcNow;
                foreach (var c in _captures.Values)
                    if (c.State == CaptureState.Faulted && now >= c.RetryAt) MarkDirty(c);
                // Safety-net sweep, spread over the interval: a few databases per tick, so it never parks real changes behind
                // a queue of 1,200 no-op reads (measured: an all-at-once sweep pushed p99 latency from ~130 ms to ~420 ms).
                var all = _captures.Values.ToArray();
                sweepDue += all.Length * tick.TotalMilliseconds / Math.Max(tick.TotalMilliseconds, options.SweepInterval.TotalMilliseconds);
                for (; sweepDue >= 1 && all.Length > 0; sweepDue--) MarkDirty(all[sweepIndex = (sweepIndex + 1) % all.Length]);
                // sooner when one is known to have changed, or could not be opened
                foreach (var (name, since) in _missing) if (now - since > TimeSpan.FromMinutes(10)) _missing.TryRemove(name, out _);
                var wait = _refreshWanted ? TimeSpan.FromSeconds(1) : _notOpened.IsEmpty && _missing.IsEmpty ? options.DatabaseRefreshInterval : TimeSpan.FromSeconds(5);
                if (now - lastRefresh >= (wait < options.DatabaseRefreshInterval ? wait : options.DatabaseRefreshInterval))
                {
                    lastRefresh = now;
                    _refreshWanted = false;
                    try { await RefreshDatabasesAsync(ct); }
                    catch (Exception ex) when (!ct.IsCancellationRequested) { _log.LogWarning(ex, "database refresh failed"); }
                }
            }
        }
        catch (Exception) when (ct.IsCancellationRequested) { }
    }

    /// <summary>
    /// Queues a historical load of one table (schema.table) or, with <paramref name="table"/> null, of every captured table of the
    /// database. Runs interleaved with the live stream; returns the queued table snapshots.
    /// </summary>
    public IReadOnlyList<TableSnapshot> RequestSnapshot(string database, string? table, int chunkSize = 5000, string? snapshotId = null)
    {
        var c = FindCapture(database) ?? throw new InvalidOperationException($"{database} is not captured");
        if (c.Schema is null) throw new InvalidOperationException($"{database}: layouts not loaded yet (retry after its first read)");
        var id = snapshotId ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
        var (queued, problems) = QueueSnapshots(c, table, chunkSize, id);
        if (queued.Count == 0) throw new InvalidOperationException(problems.Count > 0 ? string.Join("; ", problems) : $"{database}: no captured table {table}");
        return queued;
    }

    private DatabaseCapture? FindCapture(string database) =>
        _captures.Values.FirstOrDefault(x => string.Equals(x.Name, database, StringComparison.OrdinalIgnoreCase));

    private (List<TableSnapshot> Queued, List<string> Problems) QueueSnapshots(DatabaseCapture c, string? table, int chunkSize, string id)
    {
        var queued = new List<TableSnapshot>();
        var problems = new List<string>();
        var layouts = c.Schema!.Tables.GroupBy(l => l.ObjectId).Select(g => g.First())
            .Where(l => table is null || string.Equals($"{l.Schema}.{l.Table}", table, StringComparison.OrdinalIgnoreCase));
        foreach (var l in layouts)
        {
            if (l.Key.Count == 0) { problems.Add($"{l}: no primary key or unique clustered index"); continue; }
            var s = new TableSnapshot(l, $"{id}.{l.Schema}.{l.Table}", chunkSize);
            c.RequestSnapshot(s);
            queued.Add(s);
        }
        if (queued.Count > 0) MarkDirty(c);
        return (queued, problems);
    }

    /// <summary>A claimed snapshot_requests row: databases still to queue (layouts not loaded yet) and the queued table loads.</summary>
    private sealed class ActiveRequest(SnapshotRequest request)
    {
        public SnapshotRequest Request { get; } = request;
        public DateTimeOffset Claimed { get; } = DateTimeOffset.UtcNow;
        public HashSet<string> PendingDatabases { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<(DatabaseCapture Capture, TableSnapshot Snapshot)> Tables { get; } = [];
        public List<string> Problems { get; } = [];
    }

    private async Task SnapshotRequestLoopAsync(CancellationToken ct)
    {
        if (options.SnapshotRequests is not { } requests)
        {
            try { await Task.Delay(Timeout.Infinite, ct); } catch (OperationCanceledException) { }
            return;
        }
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        var ready = false;
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try
                {
                    if (!ready) { await requests.EnsureSchemaAsync(ct); ready = true; } // retried until the state database answers
                    await ProcessSnapshotRequestsAsync(requests, ct);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    if (ready) _log.LogWarning(ex, "snapshot request polling failed");
                    else _log.LogWarning(ex, "snapshot requests table unavailable (retrying)");
                }
            }
        }
        catch (Exception) when (ct.IsCancellationRequested) { }
    }

    private async Task ProcessSnapshotRequestsAsync(SnapshotRequests requests, CancellationToken ct)
    {
        foreach (var req in await requests.ClaimAsync(ct))
        {
            var a = new ActiveRequest(req);
            if (req.Database == "*") foreach (var c in _captures.Values) a.PendingDatabases.Add(c.Name);
            else a.PendingDatabases.Add(req.Database);
            _activeRequests[req.Id] = a;
            _log.LogInformation("historical load {Id}: {Table} in {Dbs} database(s)", req.Id, req.Table ?? "all captured tables", a.PendingDatabases.Count);
        }
        foreach (var (id, a) in _activeRequests)
        {
            var all = a.Request.Database == "*";
            foreach (var db in a.PendingDatabases.ToArray())
            {
                var c = FindCapture(db);
                if (c is null)
                {
                    // it may be about to be captured again (a gap's resync is requested before that), or come back from maintenance
                    if (DateTimeOffset.UtcNow - a.Claimed < TimeSpan.FromMinutes(10)) continue;
                    if (!all) a.Problems.Add($"{db}: not captured");
                    a.PendingDatabases.Remove(db);
                    continue;
                }
                if (c.Schema is null) continue; // not read yet: queued on a later tick
                var (queued, problems) = QueueSnapshots(c, a.Request.Table, a.Request.ChunkSize, $"req{id}");
                a.Tables.AddRange(queued.Select(s => (c, s)));
                a.Problems.AddRange(problems);
                if (queued.Count == 0 && problems.Count == 0 && !all) a.Problems.Add($"{db}: no captured table {a.Request.Table}");
                a.PendingDatabases.Remove(db);
            }
            foreach (var (c, s) in a.Tables)
                if (!s.Done && !(_captures.TryGetValue(c.DatabaseId, out var current) && ReferenceEquals(current, c)))
                    s.Abandon("the database was removed or re-created during the load");
            var rows = a.Tables.Sum(t => t.Snapshot.RowsEmitted);
            var finished = a.PendingDatabases.Count == 0 && a.Tables.All(t => t.Snapshot.Done);
            var problemsNow = a.Problems.Concat(a.Tables.Where(t => t.Snapshot.Failed).Select(t => $"{t.Capture.Name}.{t.Snapshot.Table.Schema}.{t.Snapshot.Table.Table}: {t.Snapshot.Error}")).ToList();
            string? status = !finished ? null
                : a.Tables.Count == 0 || a.Tables.All(t => t.Snapshot.Failed) ? "failed"
                : problemsNow.Count > 0 ? "partial" : "done";
            if (finished && a.Tables.Count == 0 && problemsNow.Count == 0) problemsNow.Add($"no captured table {a.Request.Table} in any database");
            var error = problemsNow.Count == 0 ? null : string.Join("; ", problemsNow);
            if (error is { Length: > 2000 }) error = error[..1990] + " ...";
            await requests.ReportAsync(id, rows, status, error, ct);
            if (finished)
            {
                _activeRequests.TryRemove(id, out _);
                if (status == "done") _log.LogInformation("historical load {Id} done: {Rows} rows from {Tables} table(s)", id, rows, a.Tables.Count);
                else _log.LogError("historical load {Id} {Status}: {Rows} rows from {Tables} table(s) - {Problems}. Consumers do NOT have all rows: request it again.", id, status, rows, a.Tables.Count, error);
            }
        }
    }

    /// <summary>
    /// No usable checkpoint: if the database has replicated transactions that were never released with sp_repldone it was being
    /// captured before (checkpoint lost, or a new host), so start from the oldest of them - SQL Server kept that log for us and
    /// nothing is skipped. Otherwise start at the end of the log. Also returns from where on transactions are known to be unreleased.
    /// </summary>
    private async Task<(StartPosition Start, Lsn HoldFrom)> ResolveStartAsync(SqlConnection conn, DatabaseRef database, CancellationToken ct)
    {
        var start = options.Settings.StartPosition;
        if (start != StartPosition.Latest && await ReplDoneClient.OldestUnreleasedAsync(conn, database, ct) is { } oldest)
        {
            if (start == StartPosition.Auto) _log.LogInformation("{Database}: no checkpoint, resuming from SQL Server's oldest unreleased replicated transaction {Lsn}", database.Name, oldest);
            return (start == StartPosition.Auto ? StartPosition.Earliest : start, oldest);
        }
        await using var cmd = DatabaseIdentity.Held(conn, database, "SELECT log_end_lsn FROM sys.dm_db_log_stats(@dbid);");
        string? end;
        try { end = await cmd.ExecuteScalarAsync(ct) as string; }
        catch (SqlException ex) when (DatabaseIdentity.IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
        return (start == StartPosition.Auto ? StartPosition.Latest : start, end is null ? default : Lsn.Parse(end));
    }

    /// <summary>
    /// Before anything is sent: does the destination still have what these positions stand for? It may have acknowledged messages it had in memory
    /// only and lost power (with or without this host). Then the stream goes back to its safe position, durably, and publishes from there again.
    /// </summary>
    private async Task CheckReceiptsAsync(Dictionary<string, Checkpoint> positions, IReadOnlyDictionary<string, string> incarnations, CancellationToken ct)
    {
        var receipts = positions.Values.Where(p => p.Receipt is not null && incarnations.GetValueOrDefault(p.Database) == p.Incarnation).ToDictionary(p => p.Database, p => p.Receipt!, StringComparer.Ordinal);
        if (receipts.Count == 0) return;
        var states = await sink.VerifyAsync(receipts, ct);
        var back = new List<Checkpoint>();
        foreach (var (name, state) in states.Where(s => s.Value is ReceiptState.Lost or ReceiptState.OtherDestination))
        {
            var cp = positions[name];
            if (state == ReceiptState.OtherDestination)
            {
                var why = $"the destination ({sink.Name}) is not the one this database was published to: what was published before is not there";
                _log.LogCritical("{Database}: {Why}", name, why);
                if (options.Gaps is not null)
                    await options.Gaps.RecordAsync(new CaptureGap(name, cp.Incarnation, cp.LastCommitLsn, cp.Safe?.CommitLsn ?? cp.LastCommitLsn, LogContinuity.Broken, why, DateTimeOffset.UtcNow), options.Settings.OnGap == GapPolicy.Resync, ct);
            }
            if (cp.Rewound() is { } earlier)
            {
                _log.LogCritical("{Database}: {Sink} does not have what it acknowledged; publishing again from {Safe} (the position was {Position})", name, sink.Name, earlier.LastCommitLsn, cp.LastCommitLsn);
                positions[name] = earlier;
                back.Add(earlier);
            }
            else
            {
                _log.LogCritical("{Database}: {Sink} does not have what it acknowledged, and there is no safe position yet; publishing everything again that SQL Server still holds", name, sink.Name);
                positions[name] = cp with { RestartLsn = default, LastCommitLsn = default, LastBeginLsn = default, Held = false, Settled = false, Receipt = null };
                back.Add(positions[name]);
            }
        }
        if (back.Count > 0) await checkpoints.SaveAsync(back, ct);
        if (options.SnapshotRequests is { } requests && states.Any(s => s.Value == ReceiptState.Lost))
            await requests.RequestAgainAsync(states.Where(s => s.Value == ReceiptState.Lost).Select(s => s.Key).ToList(), options.ReleaseInterval * 2 + TimeSpan.FromMinutes(1), ct);
    }

    /// <summary>Adds captures for new (e.g. newly created tenant) databases and drops captures of removed ones.
    /// A database whose incarnation changed (restored / re-created) gets a fresh capture. One that cannot be opened (single-user, being
    /// restored, an unreadable replica) is tried again later and does not keep the others waiting.</summary>
    public async Task RefreshDatabasesAsync(CancellationToken ct)
    {
        var listed = new List<(int Id, string Name, string Created, bool Published)>();
        var found = new Dictionary<int, (string Name, string Incarnation, bool Published)>();
        var notCapturable = new List<(string, string)>();
        var closed = new Dictionary<string, string>(StringComparer.Ordinal);
        await using (var conn = new SqlConnection(options.ConnectionString))
        {
            await conn.OpenAsync(ct);
            // only what is known about a database without touching it
            await using var cmd = new SqlCommand("""
                SELECT d.database_id, d.name, '-' + CONVERT(char(17), d.create_date, 121),
                       CASE WHEN d.state <> 0 THEN d.state_desc WHEN d.is_read_only = 1 THEN N'READ_ONLY' WHEN d.source_database_id IS NOT NULL THEN N'SNAPSHOT'
                            WHEN d.is_cdc_enabled = 0 THEN N'CDC is not enabled' END, CONVERT(bit, d.is_published | d.is_merge_published)
                FROM sys.databases d
                WHERE d.database_id > 4
                """, conn);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var name = r.GetString(1);
                if (name == SelfTest.DatabaseName || !(options.DatabaseFilter?.Invoke(name) ?? true)) continue;
                if (!r.IsDBNull(3)) { if (options.DatabaseFilter is not null || r.GetString(3) != "CDC is not enabled") notCapturable.Add((name, r.GetString(3))); }
                else listed.Add((r.GetInt32(0), name, r.GetString(2).Replace(" ", "", StringComparison.Ordinal), r.GetBoolean(4)));
            }
        }
        NotCapturable = notCapturable;
        SqlConnection? open = null;
        async Task<SqlConnection> OpenAsync()
        {
            if (open is { State: System.Data.ConnectionState.Open }) return open;
            if (open is not null) await open.DisposeAsync();
            open = new SqlConnection(options.ConnectionString);
            await open.OpenAsync(ct); // the server itself is gone: the whole refresh fails
            return open;
        }
        try
        {
            foreach (var (id, name, created, published) in listed)
            {
                // a capture that is running proves its recovery fork with every read (a restore in place shows there, not here)
                if (_captures.TryGetValue(id, out var c) && c.Name == name && c.Incarnation.EndsWith(created, StringComparison.Ordinal)) { found[id] = (name, c.Incarnation, published); continue; }
                var conn = await OpenAsync();
                try
                {
                    if (await DatabaseIdentity.RecoveryForkAsync(conn, id, name, ct) is { } fork) found[id] = (name, fork + created, published);
                    else closed[name] = "not started: it has no recovery fork to identify it by";
                }
                catch (Exception ex) when (ex is DatabaseChangedException or SqlException && !ct.IsCancellationRequested) { closed[name] = ex.Message; }
            }
            await TakeOverAsync(found, notCapturable, closed, OpenAsync, ct);
        }
        finally { if (open is not null) await open.DisposeAsync(); }
    }

    private async Task TakeOverAsync(Dictionary<int, (string Name, string Incarnation, bool Published)> found, List<(string, string)> notCapturable, Dictionary<string, string> closed,
                                     Func<Task<SqlConnection>> openAsync, CancellationToken ct)
    {
        // first whatever is over: a stale capture must not save a position under a name that belongs to another database now
        foreach (var (id, c) in _captures.ToArray())
        {
            if (found.TryGetValue(id, out var now) && now.Name == c.Name && now.Incarnation == c.Incarnation) continue;
            c.Supersede();
            if (!_captures.TryRemove(KeyValuePair.Create(id, c))) continue;
            _missing[c.Name] = DateTimeOffset.UtcNow;
            if (found.ContainsKey(id)) _log.LogWarning("{Database}: the database was restored, re-created or renamed", c.Name);
            else _log.LogError("{Database} is NOT captured any more: {Why}", c.Name, notCapturable.FirstOrDefault(x => x.Item1 == c.Name).Item2 ?? "the database is gone, or excluded");
        }
        foreach (var name in _notOpened.Keys.Where(n => !closed.ContainsKey(n) && found.Values.All(f => f.Name != n)).ToArray()) _notOpened.TryRemove(name, out _);
        foreach (var (name, why) in closed)
        {
            if (_notOpened.TryAdd(name, why)) _log.LogError("{Database}: cannot be opened ({Why}), so it is NOT captured yet; trying again (SQL Server keeps its log meanwhile)", name, why);
            _notOpened[name] = why;
        }
        var positions = new Dictionary<string, Checkpoint>(StringComparer.Ordinal);
        foreach (var (id, (name, incarnation, _)) in found)
            if (!_captures.ContainsKey(id) && await checkpoints.LoadAsync(name, ct) is { } position) positions[name] = position;
        await CheckReceiptsAsync(positions, found.Values.ToDictionary(f => f.Name, f => f.Incarnation, StringComparer.Ordinal), ct);
        foreach (var (id, (name, incarnation, published)) in found)
        {
            if (_captures.ContainsKey(id)) continue;
            var database = new DatabaseRef(id, name, incarnation);
            var cp = positions.GetValueOrDefault(name);
            StartPosition start;
            Lsn holdFrom;
            if (cp is not null && cp.Incarnation == incarnation) (start, holdFrom) = (options.Settings.StartPosition, default);
            else
            {
                var open = await openAsync();
                try { (start, holdFrom) = await ResolveStartAsync(open, database, ct); }
                catch (Exception ex) when (ex is DatabaseChangedException or SqlException && !ct.IsCancellationRequested)
                {
                    if (_notOpened.TryAdd(name, ex.Message)) _log.LogError(ex, "{Database}: cannot be opened, so it is NOT captured yet; trying again (SQL Server keeps its log meanwhile)", name);
                    _notOpened[name] = ex.Message;
                    continue;
                }
            }
            if (_notOpened.TryRemove(name, out _)) _log.LogInformation("{Database}: opened", name);
            if (published) _log.LogError("{Database} is published for replication: its log is the Log Reader Agent's to release, so the miner does not release it and cannot keep it either", name);
            var c = new DatabaseCapture(id, name, incarnation, cp, start, holdFrom) { ReleasedByOthers = published };
            if (c.DiscardedCheckpoint is { } old)
            {
                _log.LogCritical("{Database}: ignoring checkpoint of incarnation {Old} (now {New}): the database was restored or re-created, its contents no longer follow from the published events. " +
                                 "Capture restarts at {Start}; policy: {Policy}.", name, old.Incarnation, incarnation, start, options.Settings.OnGap);
                if (options.Gaps is not null)
                    await options.Gaps.RecordAsync(new CaptureGap(name, incarnation, old.LastCommitLsn, default, LogContinuity.Broken,
                        $"the database was restored or re-created (was {old.Incarnation})", DateTimeOffset.UtcNow), options.Settings.OnGap == GapPolicy.Resync, ct);
            }
            _captures[id] = c;
            _missing.TryRemove(name, out _);
            MarkDirty(c);
        }
    }
}
