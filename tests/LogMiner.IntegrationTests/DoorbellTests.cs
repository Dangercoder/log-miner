using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Hegel;
using LogMiner.Capture;
using LogMiner.Events;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>The push doorbell against the real engine: what it reports, how fast, and what happens when it is taken away.</summary>
public sealed class DoorbellTests : IAsyncLifetime
{
    private const string Ddl = "CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);";
    private string _db = "";
    private int _dbId;

    public async Task InitializeAsync()
    {
        _db = await TestServer.CreateDatabaseAsync(Ddl, "t");
        _dbId = (await TestServer.IdentityAsync(_db)).Id;
    }

    public Task DisposeAsync() => TestServer.DropDatabaseAsync(_db);

    private static string NewSession() => "lm_it_bell_" + Guid.NewGuid().ToString("N")[..8];

    private static DoorbellOptions Options(string session, string? connectionString = null) => new()
    {
        ConnectionString = connectionString ?? TestServer.ConnectionString(), SessionName = session, AuditInterval = TimeSpan.FromMilliseconds(200),
    };

    /// <summary>A running signal and everything it yielded.</summary>
    private sealed class Watch : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _run;
        public DoorbellSignal Signal { get; }
        public Task Run => _run;
        public ConcurrentQueue<(long At, IReadOnlyCollection<LogActivity> Set)> Sets { get; } = new();

        public Watch(DoorbellOptions options)
        {
            Signal = new DoorbellSignal(options);
            _run = Task.Run(async () =>
            {
                await foreach (var set in Signal.WatchAsync(_stop.Token)) Sets.Enqueue((Stopwatch.GetTimestamp(), set));
            });
        }

        public async Task<(double Ms, Lsn Block)> BellAsync(int dbId, long since, double timeoutMs = 5000)
        {
            while (Stopwatch.GetElapsedTime(since).TotalMilliseconds < timeoutMs)
            {
                foreach (var (at, set) in Sets)
                    if (at >= since)
                        foreach (var a in set)
                            if (a.DatabaseId == dbId) return (Stopwatch.GetElapsedTime(since, at).TotalMilliseconds, a.Block);
                await Task.Delay(1);
            }
            throw new TimeoutException($"no bell within {timeoutMs} ms (pushing: {Signal.Pushing}, {Signal.PushProblem})");
        }

        public async Task UntilAsync(Func<bool> condition, string what, int timeoutMs = 15_000)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (_run.IsFaulted) await _run;
                Assert.True(sw.ElapsedMilliseconds < timeoutMs, $"{what}: not within {timeoutMs} ms (pushing: {Signal.Pushing}, {Signal.PushProblem})");
                await Task.Delay(5);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            try { await _run; } catch (Exception ex) when (ex is OperationCanceledException || _run.IsFaulted) { }
            _stop.Dispose();
        }
    }

    private Task InsertAsync(int id) => TestServer.ExecAsync($"INSERT dbo.t (id, v) VALUES ({id}, {id});", _db);

    private static async Task<T> ScalarAsync<T>(string sql, string db = "master")
    {
        await using var conn = new SqlConnection(TestServer.ConnectionString(db));
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        return (T)(await cmd.ExecuteScalarAsync())!;
    }

    private static Task<int> SessionsAsync(string session) => ScalarAsync<int>($"SELECT COUNT(*) FROM sys.server_event_sessions WHERE name = '{session}'");

    /// <summary>Kills the miner's connection that runs a statement (program name and marker comment).</summary>
    private static async Task<bool> KillAsync(string program, string marker)
    {
        var spid = await ScalarAsync<object>($"""
            SELECT ISNULL((SELECT TOP (1) c.session_id FROM sys.dm_exec_connections c JOIN sys.dm_exec_sessions s ON s.session_id = c.session_id
            CROSS APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) t WHERE s.program_name = '{program}' AND t.text LIKE '%{marker}%' AND t.text NOT LIKE '%dm_exec_connections%'), 0)
            """);
        if (Convert.ToInt32(spid, System.Globalization.CultureInfo.InvariantCulture) == 0) return false;
        await TestServer.ExecAsync($"KILL {spid};");
        return true;
    }

    [Fact]
    public async Task Every_commit_is_pushed_within_milliseconds_with_its_log_block()
    {
        var session = NewSession();
        await using (var w = new Watch(Options(session)))
        {
            await w.UntilAsync(() => w.Signal.Pushing, "attach");
            await Task.Delay(400);
            Assert.Empty(w.Sets.First().Set); // at the start: read everything
            var ms = new List<double>();
            await using var conn = new SqlConnection(TestServer.ConnectionString(_db));
            await conn.OpenAsync();
            for (var i = 1; i <= 100; i++)
            {
                await using var cmd = new SqlCommand($"INSERT dbo.t (id, v) VALUES ({i}, {i});", conn);
                var since = Stopwatch.GetTimestamp();
                await cmd.ExecuteNonQueryAsync();
                var (elapsed, block) = await w.BellAsync(_dbId, since);
                ms.Add(elapsed);
                if (i % 20 == 0)
                {
                    await using var last = new SqlCommand("SELECT MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT'", conn);
                    var commit = Lsn.Parse((string)(await last.ExecuteScalarAsync())!);
                    await Task.Delay(30); // background writes of the database come with their own, later blocks
                    Assert.Contains(w.Sets.SelectMany(s => s.Set), a => a.DatabaseId == _dbId && a.Block.Vlf == commit.Vlf && a.Block.Block == commit.Block);
                    Assert.True(block <= commit);
                }
                await Task.Delay(5);
            }
            ms.Sort();
            Assert.True(ms[50] < 20, $"p50 {ms[50]:F1} ms"); // 4 ms dispatch interval; generous for a busy test host
            Assert.True(w.Signal.FoundByAudit <= 10, $"{w.Signal.FoundByAudit} of 100 commits were found by the audit");
            Assert.Equal(0, w.Signal.HotPolls);
            Assert.DoesNotContain(w.Sets.Skip(1), s => s.Set.Count == 0); // an empty set would have every database read
            Assert.Equal(1, await SessionsAsync(session));
        }
        Assert.Equal(0, await SessionsAsync(session));
    }

    [Theory]
    [InlineData("drop")]
    [InlineData("stop")]
    [InlineData("kill stream")]
    [InlineData("kill dispatch")]
    [InlineData("remove event")]
    public async Task Whatever_is_taken_away_commits_are_still_noticed_and_the_push_comes_back(string what)
    {
        var session = NewSession();
        var program = "lm-it-" + session;
        await using var w = new Watch(Options(session, TestServer.ConnectionString() + $";Application Name={program}"));
        await w.UntilAsync(() => w.Signal.Pushing, "attach");
        await Task.Delay(300);
        switch (what)
        {
            case "drop": await TestServer.ExecAsync($"DROP EVENT SESSION {session} ON SERVER;"); break;
            case "stop": await TestServer.ExecAsync($"ALTER EVENT SESSION {session} ON SERVER STATE = STOP;"); break;
            case "kill stream": Assert.True(await KillAsync(program, "logminer-doorbell-stream")); break;
            case "kill dispatch": Assert.True(await KillAsync(program, "logminer-doorbell-dispatch")); break;
            case "remove event": // attached, dispatched, and nothing comes: only the audit can tell
                await TestServer.ExecAsync($"ALTER EVENT SESSION {session} ON SERVER DROP EVENT sqlserver.log_flush_complete;");
                break;
        }
        var (before, hotPolls) = (w.Sets.Count, w.Signal.HotPolls);
        for (var i = 1; i <= 8; i++)
        {
            var since = Stopwatch.GetTimestamp();
            await InsertAsync(i);
            await w.BellAsync(_dbId, since, 2000); // pushed, polled or found by the audit, but found
            await Task.Delay(250);
        }
        await w.UntilAsync(() => w.Signal.PushFailures >= 1 && w.Signal.Pushing, "push is back");
        Assert.True(w.Signal.HotPolls > hotPolls, "recently active databases were not polled fast while the push was away");
        Assert.Equal(1, w.Signal.PushFailures); // back at the first attempt
        await Task.Delay(300);
        var (pushed, polls) = (w.Signal.Pushed, w.Signal.HotPolls);
        var again = Stopwatch.GetTimestamp();
        await InsertAsync(100);
        await w.BellAsync(_dbId, again, 500);
        Assert.True(w.Signal.Pushed > pushed);
        Assert.Equal(polls, w.Signal.HotPolls); // and the fast polling is off again
        Assert.Null(w.Signal.PushProblem);
        Assert.DoesNotContain(w.Sets.Skip(before), s => s.Set.Count == 0);
    }

    /// <summary>The audit and the fallback rest on the poll: losing it must not go unnoticed just because the push still works. (A connection that
    /// is merely killed is replaced by SqlClient itself: ConnectRetryCount=0 turns that off here.)</summary>
    [Fact]
    public async Task A_poll_that_fails_takes_the_signal_down_and_the_engine_starts_it_again()
    {
        var session = NewSession();
        var program = "lm-it-" + session;
        await using (var w = new Watch(Options(session, TestServer.ConnectionString() + $";ConnectRetryCount=0;Application Name={program}")))
        {
            await w.UntilAsync(() => w.Signal.Pushing, "attach");
            await Task.Delay(300);
            Assert.True(await KillAsync(program, "dm_io_virtual_file_stats"));
            Assert.Same(w.Run, await Task.WhenAny(w.Run, Task.Delay(5000)));
            Assert.IsType<SqlException>(w.Run.Exception?.InnerException);
            Assert.False(w.Signal.Pushing);
            try { await w.Run; } catch (SqlException) { }
        }
        session = NewSession();
        program = "lm-it-" + session;
        var signal = new DoorbellSignal(Options(session, TestServer.ConnectionString() + $";ConnectRetryCount=0;Application Name={program}"));
        var sink = new MemorySink();
        await using var engine = Engine(signal, sink);
        await engine.StartAsync(CancellationToken.None);
        for (var i = 0; i < 1000 && !signal.Pushing; i++) await Task.Delay(10);
        await Task.Delay(300);
        Assert.True(await KillAsync(program, "dm_io_virtual_file_stats"));
        for (var i = 0; i < 500 && engine.SignalFailures == 0; i++) await Task.Delay(10);
        Assert.Equal(1, engine.SignalFailures);
        await InsertAsync(1); // while the signal is down
        for (var i = 0; i < 500 && sink.Snapshot().Count == 0; i++) await Task.Delay(10);
        Assert.Single(sink.Snapshot());
        for (var i = 0; i < 1000 && !signal.Pushing; i++) await Task.Delay(10);
        Assert.True(signal.Pushing);
    }

    [Fact]
    public async Task Without_the_permission_it_polls_and_says_why()
    {
        var login = "lm_it_low_" + Guid.NewGuid().ToString("N")[..8];
        var password = Guid.NewGuid() + "aA1!";
        await TestServer.ExecAsync($"CREATE LOGIN {login} WITH PASSWORD = '{password}', CHECK_POLICY = OFF; GRANT VIEW SERVER STATE TO {login};");
        try
        {
            var cs = new SqlConnectionStringBuilder(TestServer.ConnectionString()) { IntegratedSecurity = false, UserID = login, Password = password, Pooling = false }.ConnectionString;
            await using var w = new Watch(Options(NewSession(), cs));
            await w.UntilAsync(() => w.Signal.PushFailures > 0, "refused");
            Assert.Contains("ALTER ANY EVENT SESSION", w.Signal.PushProblem, StringComparison.Ordinal);
            Assert.False(w.Signal.Pushing);
            for (var i = 1; i <= 5; i++)
            {
                var since = Stopwatch.GetTimestamp();
                await InsertAsync(i);
                await w.BellAsync(_dbId, since, 1000);
            }
            Assert.Equal(0, w.Signal.Pushed);
        }
        finally { await TestServer.ExecAsync($"DROP LOGIN {login};"); }
    }

    [Fact]
    public async Task It_stops_promptly_and_leaves_nothing_behind()
    {
        for (var round = 0; round < 5; round++)
        {
            var session = NewSession();
            var w = new Watch(Options(session));
            if (round > 0) await w.UntilAsync(() => w.Signal.Pushing, "attach");
            if (round > 1) await InsertAsync(round);
            var sw = Stopwatch.StartNew();
            await w.DisposeAsync();
            Assert.True(sw.ElapsedMilliseconds < 2500, $"stopping took {sw.ElapsedMilliseconds} ms");
            Assert.Equal(0, await SessionsAsync(session));
        }
    }

    [Theory]
    [InlineData("sql01", "logminer_doorbell_sql01")]
    [InlineData("SQL-01\\PROD; DROP", "logminer_doorbell_SQL_01_PROD__DROP")]
    public async Task The_default_session_name_is_a_valid_one(string server, string expected)
    {
        var name = DoorbellOptions.DefaultSessionName(server);
        Assert.Equal(expected, name);
        _ = new LogFlushStream(TestServer.ConnectionString(), name, TimeSpan.Zero);
        _ = new LogFlushStream(TestServer.ConnectionString(), DoorbellOptions.DefaultSessionName(new string('x', 300)), TimeSpan.Zero);
        Assert.Throws<ArgumentException>(() => new LogFlushStream(TestServer.ConnectionString(), server + "]", TimeSpan.Zero));
        await Task.CompletedTask;
    }

    [Fact]
    public async Task A_session_left_behind_by_a_crash_is_replaced()
    {
        var session = NewSession();
        await TestServer.ExecAsync($"CREATE EVENT SESSION {session} ON SERVER ADD EVENT sqlserver.sql_batch_completed; ALTER EVENT SESSION {session} ON SERVER STATE = START;");
        await using var w = new Watch(Options(session));
        await w.UntilAsync(() => w.Signal.Pushing, "attach");
        Assert.Equal(0, w.Signal.PushFailures);
        await Task.Delay(300);
        var since = Stopwatch.GetTimestamp();
        await InsertAsync(1);
        await w.BellAsync(_dbId, since, 500);
    }

    /// <summary>The stream has no end: what goes wrong while it is read must not be followed by an attempt to read the rest of it.</summary>
    [Fact]
    public async Task What_goes_wrong_while_the_stream_is_read_ends_it()
    {
        var stream = new LogFlushStream(TestServer.ConnectionString(), NewSession(), TimeSpan.FromMilliseconds(4));
        using var stop = new CancellationTokenSource();
        var running = stream.RunAsync(() => { }, _ => throw new InvalidDataException("not understood"), stop.Token);
        using var writing = new CancellationTokenSource();
        var writer = Task.Run(async () => { while (!writing.IsCancellationRequested) { await InsertAsync(Interlocked.Increment(ref _nextId)); } });
        var first = await Task.WhenAny(running, Task.Delay(10_000));
        await writing.CancelAsync();
        await writer;
        var ended = first == running;
        await stop.CancelAsync();
        Assert.True(ended, "still reading 10 s after the error");
        await Assert.ThrowsAsync<InvalidDataException>(() => running);
    }

    /// <summary>Nothing in the engine may have to wait for the miner: a live stream that is attached but never read loses events, not time.</summary>
    [Fact]
    public async Task A_miner_that_stops_reading_does_not_hold_up_the_writers()
    {
        var session = NewSession();
        await using var stuck = new SqlConnection(TestServer.ConnectionString() + ";Packet Size=512;Application Name=lm-it-stuck");
        await stuck.OpenAsync();
        await TestServer.ExecAsync(LogFlushStream.StartSql(session));
        try
        {
            async Task<double> RateAsync(double seconds)
            {
                var n = 0;
                var sw = Stopwatch.StartNew();
                await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
                {
                    await using var conn = new SqlConnection(TestServer.ConnectionString(_db));
                    await conn.OpenAsync();
                    await using var cmd = new SqlCommand("INSERT dbo.t (id, v) VALUES (@id, 1);", conn);
                    var id = cmd.Parameters.Add("@id", System.Data.SqlDbType.Int);
                    while (sw.Elapsed.TotalSeconds < seconds)
                    {
                        id.Value = Interlocked.Increment(ref _nextId);
                        await cmd.ExecuteNonQueryAsync();
                        Interlocked.Increment(ref n);
                    }
                })));
                return n / sw.Elapsed.TotalSeconds;
            }
            var alone = await RateAsync(2);
            await using var cmd = new SqlCommand($"SELECT type, data FROM sys.fn_MSxe_read_event_stream(N'{session}', 0)", stuck) { CommandTimeout = 0 };
            await using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync()); // attached; never read again
            var address = await ScalarAsync<byte[]>($"SELECT address FROM sys.dm_xe_sessions WHERE name = '{session}'");
            using var stop = new CancellationTokenSource();
            var dispatch = Task.Run(async () =>
            {
                await using var conn = new SqlConnection(TestServer.ConnectionString());
                await conn.OpenAsync();
                await using var force = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = @a", conn) { CommandTimeout = 5 };
                force.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = address;
                try { while (!stop.IsCancellationRequested) { await force.ExecuteScalarAsync(); await Task.Delay(4); } }
                catch (SqlException) { } // a dispatch that waits for the reader may time out; the writers must not
            });
            var blocked = await RateAsync(6);
            await stop.CancelAsync();
            cmd.Cancel(); // closing it first would read the stream to its end, and it has none
            stuck.Close();
            await dispatch;
            Assert.True(blocked > alone * 0.5, $"writers: {alone:F0}/s alone, {blocked:F0}/s with a stuck reader");
        }
        finally { await TestServer.ExecAsync($"IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '{session}') DROP EVENT SESSION {session} ON SERVER;"); }
    }

    private static int _nextId = 1_000_000;

    private sealed class ScriptedSignal : ILogActivitySignal
    {
        public Channel<LogActivity> Bells { get; } = Channel.CreateUnbounded<LogActivity>();

        public async IAsyncEnumerable<IReadOnlyCollection<LogActivity>> WatchAsync([EnumeratorCancellation] CancellationToken ct)
        {
            yield return [];
            await foreach (var b in Bells.Reader.ReadAllAsync(ct)) yield return [b];
        }
    }

    private CaptureEngine Engine(ILogActivitySignal signal, ISink sink, int workers = 2) => new(new CaptureOptions
    {
        ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n == _db, Workers = workers, CheckpointInterval = TimeSpan.FromMilliseconds(100),
        Settings = new CaptureSettings { StartPosition = StartPosition.Earliest },
        SweepInterval = TimeSpan.FromHours(24), HoldCheckInterval = TimeSpan.FromHours(1), DatabaseRefreshInterval = TimeSpan.FromHours(1), // only the doorbell can wake it
    }, signal, new EventEncoder(new EncoderOptions { ServerName = "it" }), sink, new MemoryCheckpointStore());

    /// <summary>A bell that comes before the block can be read must not be forgotten.</summary>
    [Fact]
    public async Task A_read_that_does_not_reach_the_pushed_block_is_repeated()
    {
        var signal = new ScriptedSignal();
        var sink = new MemorySink();
        await using var engine = Engine(signal, sink, 1);
        await engine.StartAsync(CancellationToken.None);
        await InsertAsync(1);
        await signal.Bells.Writer.WriteAsync(new LogActivity(_dbId, default));
        for (var i = 0; i < 100 && sink.Snapshot().Count < 1; i++) await Task.Delay(20);
        Assert.Single(sink.Snapshot());
        var capture = engine.Captures.Single();
        await Task.Delay(500); // checkpoint writes settle
        for (var round = 0; round < 5; round++)
        {
            await signal.Bells.Writer.WriteAsync(new LogActivity(_dbId, default));
            await Task.Delay(50); // everything up to here is read
            var last = capture.LastLsn;
            var retries = engine.BellRetries;
            await signal.Bells.Writer.WriteAsync(new LogActivity(_dbId, new Lsn(last.Vlf, last.Block + 1, 0))); // a block nobody has written yet
            await Task.Delay(5);
            await InsertAsync(10 + round);                                                                      // ... and no further bell
            for (var i = 0; i < 25 && sink.Snapshot().Count < 2 + round; i++) await Task.Delay(20);
            Assert.Equal(2 + round, sink.Snapshot().Count);
            Assert.True(engine.BellRetries > retries);
        }
        // a block that never becomes readable is given up
        await signal.Bells.Writer.WriteAsync(new LogActivity(_dbId, new Lsn(uint.MaxValue, 0, 0)));
        await Task.Delay(500);
        var settled = engine.BellRetries;
        await Task.Delay(300);
        Assert.Equal(settled, engine.BellRetries);
    }

    /// <summary>The poll sees a write when it is issued, and only what is on disk is read: the read then comes too early, and there is no second bell.</summary>
    [Fact]
    public async Task A_write_that_is_noticed_before_it_is_on_disk_is_read_as_soon_as_it_is()
    {
        var signal = new ScriptedSignal();
        var sink = new MemorySink();
        await using var engine = Engine(signal, sink, 1);
        await engine.StartAsync(CancellationToken.None);
        await InsertAsync(1);
        await signal.Bells.Writer.WriteAsync(new LogActivity(_dbId, default));
        for (var i = 0; i < 100 && sink.Snapshot().Count < 1; i++) await Task.Delay(20);
        Assert.Single(sink.Snapshot());
        for (var round = 1; round <= 3; round++)
        {
            await Task.Delay(500); // checkpoint writes settle
            await using var freezer = new SqlConnection(TestServer.ConnectionString() + ";Pooling=false");
            await freezer.OpenAsync();
            await using (var cmd = new SqlCommand($"ALTER DATABASE [{_db}] SET SUSPEND_FOR_SNAPSHOT_BACKUP = ON", freezer)) await cmd.ExecuteNonQueryAsync();
            Task insert;
            try
            {
                insert = InsertAsync(10 + round);
                await Task.Delay(300);
                Assert.False(insert.IsCompleted);
                await signal.Bells.Writer.WriteAsync(new LogActivity(_dbId, default)); // bytes were written: which block, the poll does not know
                await Task.Delay(round == 3 ? 3000 : 200);                             // ... a disk that takes its time
                Assert.Equal(round, sink.Snapshot().Count);
            }
            finally { await using var cmd = new SqlCommand($"ALTER DATABASE [{_db}] SET SUSPEND_FOR_SNAPSHOT_BACKUP = OFF", freezer); await cmd.ExecuteNonQueryAsync(); }
            await insert;
            var sw = Stopwatch.StartNew();
            while (sink.Snapshot().Count < 1 + round && sw.ElapsedMilliseconds < 1500) await Task.Delay(5);
            Assert.Equal(1 + round, sink.Snapshot().Count);
        }
        // it does not go on for ever (a record that nothing flushes may stay in memory)
        long settled = -1;
        for (var i = 0; i < 30 && settled != engine.BellRetries; i++)
        {
            settled = engine.BellRetries;
            await Task.Delay(600);
        }
        Assert.Equal(settled, engine.BellRetries);
        Assert.True(settled < 3 * 41, $"{settled} retries");
    }

    /// <summary>
    /// Stateful, on the real engine: the doorbell is the only thing that wakes the capture (no sweep), and it is stopped, dropped, robbed of
    /// its event and killed while rows are written. Every event arrives, in order, and soon.
    /// </summary>
    [Fact]
    public void The_capture_never_sleeps_through_a_commit() =>
        Property.Test(tc => LogMiner.Tests.Blocking.Run(() => RunAsync(tc)), new Settings { TestCases = TestServer.Cases(12), SuppressHealthCheck = HealthCheck.TooSlow });

    private async Task RunAsync(TestCase tc)
    {
        await TestServer.ExecAsync("IF EXISTS (SELECT 1 FROM dbo.t) DELETE dbo.t;", _db);
        var session = NewSession();
        var program = "lm-it-" + session;
        var signal = new DoorbellSignal(Options(session, TestServer.ConnectionString() + $";ConnectRetryCount=0;Application Name={program}"));
        var sink = new MemorySink();
        await using var engine = Engine(signal, sink);
        await engine.StartAsync(CancellationToken.None);
        for (var i = 0; i < 1000 && !signal.Pushing; i++) await Task.Delay(10);
        Assert.True(signal.Pushing, signal.PushProblem);
        var expected = new List<int>();
        int[] Got() => sink.Snapshot().Select(m =>
        {
            using var doc = JsonDocument.Parse(m.Payload);
            return doc.RootElement.GetProperty("op").GetString() == "c" ? doc.RootElement.GetProperty("after").GetProperty("id").GetInt32() : 0;
        }).Where(id => id > 0).ToArray();
        var start = _nextId; // rows of earlier cases are in the log as well
        await IdleAsync();   // a catch-up that is still running would find the rows by itself
        for (var step = tc.Draw(Generators.Integers().Min(3).Max(14), "steps"); step > 0; step--)
        {
            switch (tc.Draw(Generators.Integers().Min(0).Max(9), "what"))
            {
                case 5: await KillAsync(program, "dm_io_virtual_file_stats"); break; // the poll everything rests on: the whole signal starts over
                case 0: try { await TestServer.ExecAsync($"DROP EVENT SESSION {session} ON SERVER;"); } catch (SqlException) { } break;
                case 1: try { await TestServer.ExecAsync($"ALTER EVENT SESSION {session} ON SERVER STATE = STOP;"); } catch (SqlException) { } break;
                case 2: try { await TestServer.ExecAsync($"ALTER EVENT SESSION {session} ON SERVER DROP EVENT sqlserver.log_flush_complete;"); } catch (SqlException) { } break;
                case 3: await KillAsync(program, "logminer-doorbell-stream"); break;
                case 4: await KillAsync(program, "logminer-doorbell-dispatch"); break;
                default:
                    if (tc.Draw(Generators.Booleans(), "idle before")) await IdleAsync();
                    for (var n = tc.Draw(Generators.Integers().Min(1).Max(4), "rows"); n > 0; n--)
                    {
                        var id = Interlocked.Increment(ref _nextId);
                        await InsertAsync(id);
                        expected.Add(id);
                    }
                    if (tc.Draw(Generators.Booleans(), "wait")) await ArrivedAsync();
                    break;
            }
        }
        await ArrivedAsync();

        async Task IdleAsync()
        {
            // a pump comes to its end, and nothing was pumped for longer than an audit or a poll takes
            for (int seen = -1, same = 0, i = 0; same < 6 && i < 100; i++)
            {
                same = engine.Samples.Count == seen ? same + 1 : 0;
                seen = engine.Samples.Count;
                await Task.Delay(50);
            }
        }

        async Task ArrivedAsync()
        {
            var sw = Stopwatch.StartNew();
            while (Got().Count(id => id > start) < expected.Count)
            {
                Assert.True(sw.ElapsedMilliseconds < 4000, $"{expected.Count - Got().Count(id => id > start)} event(s) still missing after 4 s (pushing: {signal.Pushing}, {signal.PushProblem})");
                await Task.Delay(5);
            }
            Assert.Equal(expected, Got().Where(id => id > start));
        }
    }
}
