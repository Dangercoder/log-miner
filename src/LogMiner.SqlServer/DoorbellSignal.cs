using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using LogMiner.Capture;
using LogMiner.Log;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LogMiner.SqlServer;

public sealed class DoorbellOptions
{
    public required string ConnectionString { get; init; }
    /// <summary>Let SQL Server push log flushes (extended events, see <see cref="LogFlushStream"/>). Off: poll only.</summary>
    public bool Push { get; init; } = true;
    /// <summary>How often pushed events are collected: adds half of it to the latency on average and costs ~55 µs of server CPU each time.
    /// Other event sessions' buffers are dispatched along with it (see <see cref="LogFlushStream"/>).</summary>
    public TimeSpan DispatchInterval { get; init; } = TimeSpan.FromMilliseconds(4);
    /// <summary>Name of the event session, server-wide: one per miner.</summary>
    public required string SessionName { get; init; }
    /// <summary>While the push works: poll of all databases that checks it.</summary>
    public TimeSpan AuditInterval { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>While it does not: poll of all databases.</summary>
    public TimeSpan FullPollInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    /// <summary>While it does not: poll of the databases that wrote within <see cref="HotWindow"/>.</summary>
    public TimeSpan HotPollInterval { get; init; } = TimeSpan.FromMilliseconds(5);
    public TimeSpan HotWindow { get; init; } = TimeSpan.FromSeconds(10);

    public static string DefaultSessionName(string serverName)
    {
        var name = "logminer_doorbell_" + string.Concat(serverName.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_'));
        return name.Length <= 100 ? name : name[..100];
    }
}

/// <summary>
/// The doorbell: which databases wrote to their log.
/// <list type="bullet">
/// <item><b>Push</b> - SQL Server reports every log flush (<see cref="LogFlushStream"/>). Cost and latency do not depend on the number of
/// databases, and a tenant that wakes up is noticed as fast as a busy one.</item>
/// <item><b>Audit</b> - a relaxed poll of all log files' bytes written compares what was pushed with what was written
/// (<see cref="LogWriteAudit"/>): a database with writes the push did not report is read, and a push that fell silent is restarted.</item>
/// <item><b>Fallback</b> - until the push is attached, and whenever it fails, everything is polled: all databases at a relaxed interval
/// (~2 µs of server CPU per log file), the recently active ones every few milliseconds.</item>
/// </list>
/// Whatever happens to the push, polling goes on; it is only ever about latency.
/// </summary>
public sealed class DoorbellSignal(DoorbellOptions options, ILogger? logger = null) : ILogActivitySignal
{
    private const string FullSql = "SELECT database_id, num_of_bytes_written FROM sys.dm_io_virtual_file_stats(NULL, 2) WHERE database_id > 4 /*logminer-doorbell*/";
    private const string HotSql = """
        SELECT CONVERT(int, v.value), s.num_of_bytes_written
        FROM STRING_SPLIT(@ids, ',') v CROSS APPLY sys.dm_io_virtual_file_stats(CONVERT(int, v.value), 2) s /*logminer-doorbell-hot*/
        """;
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(60), AttachTimeout = TimeSpan.FromSeconds(10), PollTickWhilePushing = TimeSpan.FromMilliseconds(20);

    private readonly ILogger _log = logger ?? NullLogger.Instance;
    private volatile bool _pushing;
    private long _fullPolls, _hotPolls, _audited, _pushed, _pushFailures;

    /// <summary>The push is attached and working (otherwise everything is polled).</summary>
    public bool Pushing => _pushing;
    /// <summary>Log flushes pushed.</summary>
    public long Pushed => Interlocked.Read(ref _pushed);
    /// <summary>Times a poll found a database with writes that had not been pushed while the push was working.</summary>
    public long FoundByAudit => Interlocked.Read(ref _audited);
    /// <summary>Times the push could not be attached, broke or fell silent.</summary>
    public long PushFailures => Interlocked.Read(ref _pushFailures);
    /// <summary>Why the push does not work (null while it does).</summary>
    public string? PushProblem { get; private set; }
    public long FullPolls => Interlocked.Read(ref _fullPolls);
    public long HotPolls => Interlocked.Read(ref _hotPolls);

    [DllImport("winmm.dll")] private static extern uint timeBeginPeriod(uint ms);

    public async IAsyncEnumerable<IReadOnlyCollection<LogActivity>> WatchAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (OperatingSystem.IsWindows()) _ = timeBeginPeriod(1); // 1 ms timer resolution for this process (default 15.6 ms)
        var bells = Channel.CreateUnbounded<IReadOnlyCollection<LogActivity>>(new UnboundedChannelOptions { SingleReader = true });
        var audit = new LogWriteAudit();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var push = options.Push ? Task.Run(() => PushAsync(bells.Writer, audit, stop.Token), CancellationToken.None) : Task.CompletedTask;
        var poll = Task.Run(async () =>
        {
            // polling is what everything else rests on: if it fails, the whole signal fails (and the engine starts it again)
            try { await PollAsync(bells.Writer, audit, stop.Token); bells.Writer.TryComplete(); }
            catch (Exception ex) { bells.Writer.TryComplete(ex); }
        }, CancellationToken.None);
        try
        {
            await foreach (var set in bells.Reader.ReadAllAsync(ct)) yield return set;
        }
        finally
        {
            await stop.CancelAsync();
            await Task.WhenAll(poll, push); // neither throws
            _pushing = false;
        }
    }

    private async Task PollAsync(ChannelWriter<IReadOnlyCollection<LogActivity>> bells, LogWriteAudit audit, CancellationToken ct)
    {
        var hotUntil = new Dictionary<int, long>(); // database id -> Stopwatch timestamp until which it is polled fast
        var rows = new List<(int, long)>();
        await using var conn = new SqlConnection(options.ConnectionString);
        await conn.OpenAsync(ct);
        await using var full = new SqlCommand(FullSql, conn) { CommandTimeout = 30 };
        await using var hot = new SqlCommand(HotSql, conn) { CommandTimeout = 30 };
        var ids = hot.Parameters.Add("@ids", System.Data.SqlDbType.VarChar, -1);
        var windowTicks = (long)(options.HotWindow.TotalSeconds * Stopwatch.Frequency);
        long lastFull = 0;
        var first = true;
        while (!ct.IsCancellationRequested)
        {
            var now = Stopwatch.GetTimestamp();
            var pushing = _pushing;
            var read = new List<int>();
            if (first || Stopwatch.GetElapsedTime(lastFull, now) >= (pushing ? options.AuditInterval : options.FullPollInterval))
            {
                lastFull = now;
                await ReadAsync(full, rows, ct);
                read = audit.Polled(rows, pushing);
                Interlocked.Increment(ref _fullPolls);
                if (pushing) Interlocked.Add(ref _audited, read.Count);
            }
            else if (!pushing && hotUntil.Count > 0)
            {
                ids.Value = string.Join(',', hotUntil.Keys);
                await ReadAsync(hot, rows, ct);
                read = audit.Polled(rows, pushing, complete: false);
                Interlocked.Increment(ref _hotPolls);
            }
            foreach (var db in read) hotUntil[db] = now + windowTicks;
            foreach (var (db, until) in hotUntil.ToArray()) if (until < now) hotUntil.Remove(db);
            if (first) bells.TryWrite([]); // nothing is known yet: read every database
            else if (read.Count > 0) bells.TryWrite(read.ConvertAll(db => new LogActivity(db, default)));
            first = false;
            await Task.Delay(pushing ? PollTickWhilePushing : options.HotPollInterval, ct);
        }
    }

    private static async Task ReadAsync(SqlCommand cmd, List<(int, long)> rows, CancellationToken ct)
    {
        rows.Clear();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) rows.Add((Convert.ToInt32(r.GetValue(0), System.Globalization.CultureInfo.InvariantCulture), r.GetInt64(1)));
    }

    /// <summary>Keeps the push attached. Never throws: whatever goes wrong is logged, polled over, and tried again.</summary>
    private async Task PushAsync(ChannelWriter<IReadOnlyCollection<LogActivity>> bells, LogWriteAudit audit, CancellationToken ct)
    {
        var failuresInARow = 0;
        try
        {
            if (await LogFlushStream.NeighboursAsync(options.ConnectionString, ct) is { Count: > 0 } neighbours)
                _log.LogInformation("doorbell: event sessions and audits that write files will do so every {Interval} ms while they collect events: {Sessions}",
                    options.DispatchInterval.TotalMilliseconds, string.Join(", ", neighbours));
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException or OperationCanceledException) { } // only information
        while (!ct.IsCancellationRequested)
        {
            var attachedAt = 0L;
            try
            {
                using var run = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var stream = new LogFlushStream(options.ConnectionString, options.SessionName, options.DispatchInterval);
                var streaming = stream.RunAsync(() =>
                {
                    attachedAt = Stopwatch.GetTimestamp();
                    PushProblem = null;
                    _pushing = true;
                    _log.LogInformation("doorbell: SQL Server pushes log flushes (event session {Session}, collected every {Interval} ms)", options.SessionName, options.DispatchInterval.TotalMilliseconds);
                }, flushes => Ring(bells, audit, flushes), run.Token);
                var problem = await WatchAsync(streaming, audit, run.Token);
                if (problem is not null)
                {
                    _pushing = false;
                    await run.CancelAsync();
                }
                await streaming;
                if (problem is not null) throw new IOException(problem);
            }
            catch (Exception) when (ct.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                _pushing = false;
                Interlocked.Increment(ref _pushFailures);
                if (attachedAt != 0 && Stopwatch.GetElapsedTime(attachedAt) > MaxRetryDelay) failuresInARow = 0; // it had been working
                var delay = ex is EventStreamFormatException ? MaxRetryDelay : TimeSpan.FromSeconds(Math.Min(MaxRetryDelay.TotalSeconds, Math.Pow(2, failuresInARow++)));
                var why = Explain(ex);
                // never degrade silently; the same problem over and over is not news
                _log.Log(why == PushProblem ? LogLevel.Debug : LogLevel.Error, ex,
                    "doorbell: SQL Server does not push log flushes ({Problem}); polling every {Poll} ms instead, trying again in {Delay} s",
                    why, options.FullPollInterval.TotalMilliseconds, delay.TotalSeconds);
                PushProblem = why;
                try { await Task.Delay(delay, ct); }
                catch (OperationCanceledException) { return; }
            }
        }
    }

    private void Ring(ChannelWriter<IReadOnlyCollection<LogActivity>> bells, LogWriteAudit audit, List<LogFlush> flushes)
    {
        audit.Pushed(flushes);
        Interlocked.Add(ref _pushed, flushes.Count);
        // an empty set would mean "every database": a packet always has flushes here, one bell per database with its newest block
        var newest = new Dictionary<int, Lsn>(flushes.Count);
        foreach (var f in flushes) newest[f.DatabaseId] = newest.TryGetValue(f.DatabaseId, out var known) && known > f.Block ? known : f.Block;
        if (newest.Count > 0) bells.TryWrite(newest.Select(n => new LogActivity(n.Key, n.Value)).ToArray());
    }

    /// <summary>Returns what is wrong with a stream that goes on as if nothing were, or null when it ended by itself.</summary>
    private async Task<string?> WatchAsync(Task streaming, LogWriteAudit audit, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        while (!streaming.IsCompleted && !ct.IsCancellationRequested)
        {
            if (_pushing && audit.Stalled) return "log is written but nothing is pushed any more";
            if (!_pushing && Stopwatch.GetElapsedTime(started) > AttachTimeout) return $"the live stream did not start within {AttachTimeout.TotalSeconds} s";
            await Task.WhenAny(streaming, Task.Delay(options.AuditInterval, ct));
        }
        return null;
    }

    private static string Explain(Exception ex) => ex is SqlException { Number: 297 or 300 or 15247 } s
        ? $"missing permission, needs ALTER ANY EVENT SESSION and VIEW SERVER STATE: {s.Message}"
        : ex is EventStreamFormatException ? $"this SQL Server build's event stream is not understood: {ex.Message}" : $"{ex.GetType().Name}: {ex.Message}";
}
