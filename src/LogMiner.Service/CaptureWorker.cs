using System.Text.RegularExpressions;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogMiner.Service;

/// <summary>Hosts the capture engine: builds the sink chain from configuration, runs, and logs health.</summary>
public sealed class CaptureWorker(IOptions<LogMinerSettings> settings, IConfiguration configuration, IEnumerable<ISinkProvider> providers,
                                  ILoggerFactory loggers, IHostApplicationLifetime lifetime, ILogger<CaptureWorker> log) : BackgroundService
{
    private CaptureEngine? _engine;
    private DoorbellSignal? _signal;
    private ISink? _sink;
    private ICheckpointStore? _checkpoints;

    /// <summary>The server restarted, the lease was lost, the destination was not there at boot, a part of the engine stopped: whatever ends a
    /// run, the next one starts from the durable state. Nobody else has to notice.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var provider = providers.FirstOrDefault(p => string.Equals(p.Type, settings.Value.Sink.Type, StringComparison.OrdinalIgnoreCase));
        if (Problem(settings.Value, provider) is { } problem)
        {
            log.LogCritical("configuration: {Problem}", problem);
            Stop(2);
            return;
        }
        var delay = TimeSpan.FromSeconds(1);
        while (!stoppingToken.IsCancellationRequested)
        {
            var started = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                if (!await RunAsync(settings.Value, provider!, stoppingToken)) return;
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                if (System.Diagnostics.Stopwatch.GetElapsedTime(started) > TimeSpan.FromMinutes(1)) delay = TimeSpan.FromSeconds(1);
                log.LogCritical(ex, "capture stopped ({Why}); starting over from the last saved position in {Delay} s (SQL Server keeps the log meanwhile)", ex.Message, delay.TotalSeconds);
            }
            if (!await TearDownAsync())
            {
                // something of the old run is still alive and might publish after the new one: only the end of the process ends that
                log.LogCritical("the previous run does not come to an end; stopping the process");
                Environment.Exit(5);
            }
            try { await Task.Delay(delay, stoppingToken); } catch (OperationCanceledException) { return; }
            delay = TimeSpan.FromSeconds(Math.Min(30, delay.TotalSeconds * 2));
        }
    }

    private void Stop(int exitCode)
    {
        Environment.ExitCode = exitCode;
        lifetime.StopApplication();
    }

    /// <summary>What would lose events or never work, whatever the state of the world.</summary>
    internal static string? Problem(LogMinerSettings s, ISinkProvider? provider)
    {
        if (provider is null) return $"unknown sink type '{s.Sink.Type}'";
        if (s.State.Type == StateStoreType.SqlServer && s.Sink.Spool.Enabled && s.Sink.Spool.AckOn == SpoolAck.Spooled)
            return "State=SqlServer with Spool.AckOn=Spooled would release SQL Server's log behind a local-only copy: losing this host's disk could lose events. Use Spool.AckOn=Delivered, or State=File for host-local mode.";
        if (s.Scope != CaptureScope.CdcTrackedTables) return $"Scope {s.Scope} is not supported: only tables with CDC enabled can be captured";
        if (s.Workers < 1 || s.MaxRowsPerRead < 2) return "Workers must be at least 1 and MaxRowsPerRead at least 2";
        if (s.CheckpointIntervalMs < 1 || s.SweepIntervalSeconds < 1 || s.ReleaseIntervalMs < 0) return "CheckpointIntervalMs and SweepIntervalSeconds must be positive, ReleaseIntervalMs not negative";
        if (s.Signal.FullPollIntervalMs < 1 || s.Signal.HotPollIntervalMs < 1) return "the doorbell's poll intervals must be positive";
        if (string.IsNullOrWhiteSpace(s.ServerName)) return "ServerName must not be empty";
        if (EventEncoder.TemplateProblem(s.Routing.DestinationTemplate) is { } template) return template;
        if (s.Sink.Spool is { Enabled: true } spool && spool.MaxBytes < 2 * spool.SegmentBytes) return "Spool.MaxBytes must be at least twice Spool.SegmentBytes";
        return null;
    }

    /// <summary>Until it is stopped (false: for good) or something fails (an exception).</summary>
    private async Task<bool> RunAsync(LogMinerSettings s, ISinkProvider provider, CancellationToken stoppingToken)
    {
        _sink = provider.Create(configuration.GetSection("LogMiner:Sink").GetSection(SectionName(s.Sink.Type)));
        if (s.Sink.Spool.Enabled)
        {
            _sink = new SpoolSink(_sink, new SpoolOptions
            {
                Directory = s.Sink.Spool.Directory, AckOn = s.Sink.Spool.AckOn, MaxBytes = s.Sink.Spool.MaxBytes, SegmentBytes = s.Sink.Spool.SegmentBytes,
            });
        }
        else if (SpoolSink.HasUndelivered(s.Sink.Spool.Directory))
            throw new InvalidOperationException($"the spool in {s.Sink.Spool.Directory} holds messages that were never delivered, but it is disabled: enable it until it is empty (or delete it, if they are not needed)");
        SqlStateStore? sqlState = null;
        IGapLog gaps = new FileGapLog(Path.Combine(Path.GetDirectoryName(Path.GetFullPath(s.State.File))!, "gaps.jsonl"));
        if (s.State.Type == StateStoreType.SqlServer)
        {
            var stateCs = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(s.ConnectionString) { InitialCatalog = s.State.Database, ApplicationName = "logminer-state" }.ConnectionString;
            _checkpoints = sqlState = new SqlStateStore(stateCs, s.ServerName);
            await sqlState.EnsureSchemaAsync(stoppingToken);
            var gapLog = new SqlGapLog(stateCs, s.ServerName);
            await gapLog.EnsureSchemaAsync(stoppingToken);
            await new SnapshotRequests(stateCs, s.ServerName).EnsureTableAsync(stoppingToken); // a gap's resync is requested there
            gaps = gapLog;
            var selfTests = new SelfTestLog(stateCs, s.ServerName);
            await selfTests.EnsureSchemaAsync(stoppingToken);
            log.LogInformation("waiting for the lease {Resource} (another instance may be active)", sqlState.Resource);
            await sqlState.AcquireLeaseAsync(() => log.LogDebug("standby: lease held by another instance"), stoppingToken);
            log.LogInformation("lease acquired: this instance is active");
            if (!await VerifyServerAsync(s, selfTests, stoppingToken)) return false; // under the lease: only one instance uses the scratch database
        }
        else
        {
            if (!await VerifyServerAsync(s, null, stoppingToken)) return false;
            _checkpoints = new FileCheckpointStore(s.State.File);
        }
        var encoder = new EventEncoder(new EncoderOptions { ServerName = s.ServerName, DestinationTemplate = s.Routing.DestinationTemplate, PartitionKey = s.Routing.PartitionKey });
        _signal = new DoorbellSignal(new DoorbellOptions
        {
            ConnectionString = s.ConnectionString,
            Push = s.Signal.Push,
            DispatchInterval = TimeSpan.FromMilliseconds(Math.Max(1, s.Signal.DispatchIntervalMs)),
            SessionName = s.Signal.EventSession is { Length: > 0 } name ? name : DoorbellOptions.DefaultSessionName(s.ServerName),
            AuditInterval = TimeSpan.FromMilliseconds(Math.Max(100, s.Signal.AuditIntervalMs)),
            FullPollInterval = TimeSpan.FromMilliseconds(s.Signal.FullPollIntervalMs),
            HotPollInterval = TimeSpan.FromMilliseconds(s.Signal.HotPollIntervalMs),
            HotWindow = TimeSpan.FromSeconds(s.Signal.HotWindowSeconds),
        }, loggers.CreateLogger<DoorbellSignal>());
        _engine = new CaptureEngine(new CaptureOptions
        {
            ConnectionString = s.ConnectionString,
            Scope = s.Scope,
            Settings = new CaptureSettings { StartPosition = s.StartPosition, MaxRowsPerRead = s.MaxRowsPerRead, MaxBufferedBytes = Math.Max(1, s.MaxBufferedMb) * 1024L * 1024, OnGap = s.OnGap },
            Gaps = gaps,
            MaxHeldLogMb = s.MaxHeldLogMb,
            Workers = s.Workers,
            CheckpointInterval = TimeSpan.FromMilliseconds(s.CheckpointIntervalMs),
            ReleaseInterval = TimeSpan.FromMilliseconds(s.ReleaseIntervalMs),
            ReleaseLog = s.ReleaseLog,
            SweepInterval = TimeSpan.FromSeconds(s.SweepIntervalSeconds),
            DatabaseFilter = BuildFilter(s.Databases, s.ExcludeDatabases),
            SnapshotRequests = s.State.Type == StateStoreType.SqlServer
                ? new SnapshotRequests(new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(s.ConnectionString) { InitialCatalog = s.State.Database }.ConnectionString, s.ServerName)
                : null,
        }, _signal, encoder, _sink, _checkpoints!, loggers.CreateLogger<CaptureEngine>());

        log.LogInformation("logminer starting: server {Server}, sink {Sink}, scope {Scope}, start {Start}", s.ServerName, _sink.Name, s.Scope, s.StartPosition);
        await _engine.StartAsync(stoppingToken);
        log.LogInformation("capturing {Count} databases", _engine.Captures.Count);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        var lastHealth = DateTimeOffset.UtcNow;
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            if (_engine.Failure.IsFaulted) await _engine.Failure; // half an engine must not go on looking healthy
            if (_sink.Health is { Failed: true } failed) throw new InvalidOperationException($"the sink failed: {failed.LastError}");
            // fencing: another instance may take over now; checkpoint writes are refused anyway
            if (sqlState is not null && !await sqlState.CheckLeaseAsync(stoppingToken)) throw new InvalidOperationException("the lease is lost: another instance may be active");
            if (DateTimeOffset.UtcNow - lastHealth >= TimeSpan.FromSeconds(Math.Max(1, s.HealthLogIntervalSeconds)))
            {
                lastHealth = DateTimeOffset.UtcNow;
                LogHealth();
            }
        }
        return false;
    }

    /// <summary>Ends the run, in bounded time. False: a part of it is still there.</summary>
    private async Task<bool> TearDownAsync()
    {
        var ended = true;
        // the engine first (it saves what was acknowledged); if it waits for the destination, the end of that releases it
        var engine = _engine is null ? Task.CompletedTask : Task.Run(async () => await _engine.DisposeAsync());
        ended &= await EndsAsync(engine, TimeSpan.FromSeconds(10), "the capture engine", last: false);
        if (_sink is not null) ended &= await EndsAsync(Task.Run(async () => await _sink.DisposeAsync()), TimeSpan.FromSeconds(10), "the sink", last: true);
        if (!engine.IsCompleted) ended = await EndsAsync(engine, TimeSpan.FromSeconds(5), "the capture engine", last: true);
        else ended = true;
        switch (_checkpoints)
        {
            case IAsyncDisposable a: ended &= await EndsAsync(Task.Run(async () => await a.DisposeAsync()), TimeSpan.FromSeconds(5), "the state store", last: true); break;
            case IDisposable d: d.Dispose(); break;
        }
        (_engine, _signal, _sink, _checkpoints) = (null, null, null, null);
        return ended;
    }

    private async Task<bool> EndsAsync(Task task, TimeSpan within, string what, bool last)
    {
        try { await task.WaitAsync(within); }
        catch (TimeoutException)
        {
            if (last) log.LogError("{What} did not end within {Seconds} s", what, within.TotalSeconds);
            return false;
        }
        catch (Exception ex) { log.LogWarning(ex, "{What} ended with an error", what); }
        return true;
    }

    /// <summary>The log's format is not a documented interface: prove once per server version and miner build that it is understood.</summary>
    private async Task<bool> VerifyServerAsync(LogMinerSettings s, SelfTestLog? remembered, CancellationToken ct)
    {
        if (s.SelfTest == SelfTestMode.Off) return true;
        var version = await SelfTest.VersionAsync(s.ConnectionString, ct);
        if (remembered is not null && await remembered.IsVerifiedAsync(version, ct)) return true;
        log.LogInformation("self-test: verifying SQL Server {Version} with this build in a scratch database", version);
        var result = await SelfTest.RunAsync(s.ConnectionString, ct);
        if (remembered is not null) await remembered.SaveAsync(result, ct);
        if (result.DoorbellProblem is { } bell) log.LogWarning("self-test: log flushes are not pushed on SQL Server {Version} ({Problem}); the doorbell will poll", result.ProductVersion, bell);
        if (result.Passed)
        {
            log.LogInformation("self-test: SQL Server {Version} verified in {Seconds:F1} s", result.ProductVersion, result.Elapsed.TotalSeconds);
            return true;
        }
        foreach (var p in result.Problems) log.LogCritical("self-test: {Problem}", p);
        if (s.SelfTest == SelfTestMode.Warn) return true;
        log.LogCritical("self-test FAILED on SQL Server {Version}: not capturing (the log is retained; set LogMiner:SelfTest to Warn to override)", result.ProductVersion);
        Stop(4);
        return false;
    }

    private void LogHealth()
    {
        if (_engine is null) return;
        var captures = _engine.Captures;
        var faulted = captures.Where(c => c.State == CaptureState.Faulted).ToList();
        var h = _engine.Sink.Health;
        if (h.LastError is not null && h.LastErrorAt > DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5)) log.LogWarning("health: sink error at {At:O}: {Error}", h.LastErrorAt, h.LastError);
        if (_engine.AuditProblem is { } audit) log.LogCritical("health: no log is released: {Problem}", audit);
        if (_engine.LongestPump is { } pump && pump.Since > TimeSpan.FromSeconds(60)) log.LogError("health: {Database} has been waiting for the destination or the server for {Seconds:F0} s", pump.Database, pump.Since.TotalSeconds);
        log.LogInformation("health: {Dbs} databases, {Events} events, {Open} open txns, {Faulted} faulted; sink {Sink} healthy={Healthy} pending={Pending} oldest={Oldest}",
            captures.Count, captures.Sum(c => c.EventsPublished), captures.Sum(c => c.OpenTransactions), faulted.Count, h.Sink, h.Healthy, h.PendingMessages, h.OldestPendingAge);
        if (_signal is { Pushing: true } d) log.LogInformation("health: doorbell pushing, {Pushed} log flushes, {Audit} found by the audit, {Retries} early reads repeated, {GivenUp} given up", d.Pushed, d.FoundByAudit, _engine.BellRetries, captures.Sum(c => c.BellsGivenUp));
        else if (_signal is { PushProblem: { } why }) log.LogWarning("health: doorbell POLLING (higher latency and CPU): {Problem}", why);
        if (captures.Sum(c => c.Gaps) is > 0 and var gapCount) log.LogCritical("health: {Gaps} gap(s) detected since start, see logminer.dbo.gaps", gapCount);
        if (_engine.ForeignReleases is { Count: > 0 } foreign) log.LogError("health: someone else releases the log of {Count} database(s), e.g. {Db}", foreign.Count, foreign.First());
        if (_engine.NotCapturable is { Count: > 0 } no) log.LogWarning("health: {Count} database(s) are included but cannot be captured as they are, e.g. {Db}: {Reason}", no.Count, no.First().Database, no.First().Reason);
        if (_engine.NotOpened is { Count: > 0 } closed) log.LogError("health: {Count} database(s) cannot be opened and are NOT captured, e.g. {Db}: {Reason}", closed.Count, closed.First().Database, closed.First().Reason);
        foreach (var f in faulted.Take(5)) log.LogWarning("faulted: {Database}: {Fault} (retry at {Retry:O})", f.Name, f.Fault, f.RetryAt);
        var untracked = captures.Where(c => c.UntrackedTables.Count > 0).ToList();
        if (untracked.Count > 0)
            log.LogWarning("{Count} captured database(s) have tables that are not CDC-enabled (not captured), e.g. {Db}: {Tables}",
                untracked.Count, untracked[0].Name, string.Join(", ", untracked[0].UntrackedTables.Take(5)));
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await TearDownAsync();
    }

    private static string SectionName(string type) => string.Concat(type.Split('-').Select(p => p.Length == 0 ? p : char.ToUpperInvariant(p[0]) + p[1..]));

    private static Func<string, bool>? BuildFilter(string[] include, string[] exclude)
    {
        if (include.Length == 0 && exclude.Length == 0) return null;
        static Regex Glob(string p) => new("^" + Regex.Escape(p).Replace("\\*", ".*", StringComparison.Ordinal).Replace("\\?", ".", StringComparison.Ordinal) + "$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        var inc = include.Select(Glob).ToArray();
        var exc = exclude.Select(Glob).ToArray();
        return name => (inc.Length == 0 || inc.Any(r => r.IsMatch(name))) && !exc.Any(r => r.IsMatch(name));
    }
}
