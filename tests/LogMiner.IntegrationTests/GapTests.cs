using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// Log that disappears before the miner read it: ordinary truncation behind the replication hold is harmless and silent; log released
/// by someone else is a gap, recorded before the stream goes on.
/// </summary>
public class GapTests
{
    private const string Ddl = "CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL); CREATE TABLE dbo.noise (id int IDENTITY PRIMARY KEY, pad char(4000) NOT NULL);";

    private static Task InsertAsync(CaptureHarness h, int id) => h.DmlAsync("c", id, $"INSERT dbo.t (id, v) VALUES ({id}, {id});");

    private static async Task<CaptureHarness> LiveAsync()
    {
        var h = await CaptureHarness.CreateAsync(Ddl, ["t"]);
        for (var i = 1; i <= 3; i++)
        {
            await InsertAsync(h, i);
            await h.PumpAllAsync();
            await h.CheckpointAsync(release: true);
        }
        Assert.Empty(h.JournalDiff());
        Assert.True((await h.Store.LoadAsync(h.Db, CancellationToken.None))!.Held);
        return h;
    }

    /// <summary>Activity on a table that is not captured, with log backups, until the log no longer reaches back to <paramref name="position"/>.</summary>
    private static async Task<bool> TruncatePastAsync(CaptureHarness h, Lsn position, int rounds = 12)
    {
        for (var i = 0; i < rounds; i++)
        {
            await h.SqlAsync("INSERT dbo.noise (pad) SELECT TOP (1500) 'x' FROM sys.all_objects a CROSS JOIN sys.all_objects b; TRUNCATE TABLE dbo.noise; CHECKPOINT;");
            await TestServer.ExecAsync($"BACKUP LOG [{h.Db}] TO DISK = 'NUL';");
            await using var conn = new SqlConnection(TestServer.ConnectionString(h.Db));
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID())", conn);
            if (Lsn.Parse((string)(await cmd.ExecuteScalarAsync())!) > position) return true;
        }
        return false;
    }

    private static async Task<Lsn> PositionAsync(CaptureHarness h) => (await h.Store.LoadAsync(h.Db, CancellationToken.None))!.RestartLsn;

    [Fact]
    public async Task The_newest_transaction_is_held_back_and_keeps_its_log()
    {
        await using var h = await LiveAsync();
        var cp = (await h.Store.LoadAsync(h.Db, CancellationToken.None))!;
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        var oldest = await ReplDoneClient.OldestUnreleasedAsync(conn, h.Capture.Database, CancellationToken.None);
        // all but the newest are released. The engine reports a log block: its own, or that of a checkpoint which was running just before it
        Assert.True(oldest > h.Capture.ReleasedThrough && oldest <= cp.LastBeginLsn, $"oldest {oldest}, released {h.Capture.ReleasedThrough}, held back {cp.LastBeginLsn}");
        Assert.Null(ReplDoneClient.Explain(oldest, cp));
        Assert.False(await TruncatePastAsync(h, cp.LastBeginLsn, rounds: 3));
    }

    [Fact]
    public async Task A_full_release_lets_go_of_the_log_and_later_truncation_cannot_be_verified()
    {
        await using var h = await LiveAsync();
        h.Capture.SetFault(new IOException("sink is down"));
        Assert.False(h.Capture.RequestFullRelease()); // behind: the bookmark is needed most, and later transactions hold the log anyway
        await h.PumpAllAsync();
        Assert.True(h.Capture.RequestFullRelease());
        await h.CheckpointAsync(release: true);
        var cp = (await h.Store.LoadAsync(h.Db, CancellationToken.None))!;
        Assert.False(cp.Held);
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        Assert.Null(await ReplDoneClient.OldestUnreleasedAsync(conn, h.Capture.Database, CancellationToken.None));
        Assert.True(await TruncatePastAsync(h, cp.RestartLsn));
        await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal(LogContinuity.Unknown, Assert.Single(h.Gaps).Continuity); // conservative: reported, not assumed away
        await InsertAsync(h, 10);
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        Assert.Empty(h.JournalDiff());
        Assert.True((await h.Store.LoadAsync(h.Db, CancellationToken.None))!.Held); // the next captured change is the new bookmark
        Assert.Single(h.Gaps);
    }

    /// <summary>A transaction that is open at the checkpoint, began before the bookmark and ends while the miner is down: SQL Server must keep
    /// its beginning (it is an unreleased replicated transaction), whatever else is truncated.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_transaction_spanning_the_checkpoint_ends_while_the_miner_is_down(bool commit)
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t"]);
        await h.SqlAsync("INSERT dbo.t (id, v) VALUES (1, 1);");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        await using var conn = new SqlConnection(TestServer.ConnectionString(h.Db));
        await conn.OpenAsync();
        await using (var tx = (SqlTransaction)await conn.BeginTransactionAsync())
        {
            await using (var cmd = new SqlCommand("INSERT dbo.t (id, v) VALUES (100, 0);", conn, tx)) await cmd.ExecuteNonQueryAsync();
            await h.SqlAsync("INSERT dbo.noise (pad) SELECT TOP (3000) 'x' FROM sys.all_objects a CROSS JOIN sys.all_objects b;"); // ~12 MB of log
            for (var i = 2; i <= 4; i++) { await h.SqlAsync($"INSERT dbo.t (id, v) VALUES ({i}, {i});"); await h.PumpAllAsync(); await h.CheckpointAsync(release: true); }
            Assert.True(await PositionAsync(h) < (await h.Store.LoadAsync(h.Db, CancellationToken.None))!.LastBeginLsn);
            // the miner is down from here on
            await using (var cmd = new SqlCommand("INSERT dbo.t (id, v) VALUES (101, 0);", conn, tx)) await cmd.ExecuteNonQueryAsync();
            if (commit) await tx.CommitAsync(); else await tx.RollbackAsync();
        }
        var truncated = await TruncatePastAsync(h, await PositionAsync(h));
        Assert.False(commit && truncated, "SQL Server dropped the beginning of an unreleased transaction");
        await h.RestartAsync();
        await h.SqlAsync("INSERT dbo.t (id, v) VALUES (5, 5);");
        await h.PumpAllAsync();
        Assert.Empty(h.Gaps);
        Assert.Empty(await h.DiffAsync("t"));
        Assert.Equal(commit ? 7 : 5, h.Events().Count());
    }

    /// <summary>The whole way: the log is released and truncated by someone else while the service is down; it records the gap, loads the
    /// database again by itself, and consumers have every current row.</summary>
    [Fact]
    public async Task A_gap_is_recorded_and_resynchronised_by_the_engine()
    {
        var state = "lm_state_" + Guid.NewGuid().ToString("N")[..10];
        await TestServer.ExecAsync($"CREATE DATABASE [{state}];");
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t"]);
        try
        {
            var sink = new MemorySink();
            async Task<(CaptureEngine Engine, SqlStateStore Store)> StartAsync()
            {
                var store = new SqlStateStore(TestServer.ConnectionString(state), "srv");
                await store.EnsureSchemaAsync(CancellationToken.None);
                await store.AcquireLeaseAsync(null, CancellationToken.None);
                var requests = new SnapshotRequests(TestServer.ConnectionString(state), "srv");
                var gaps = new SqlGapLog(TestServer.ConnectionString(state), "srv");
                await requests.EnsureTableAsync(CancellationToken.None);
                await gaps.EnsureSchemaAsync(CancellationToken.None);
                var engine = new CaptureEngine(new CaptureOptions
                {
                    ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n == h.Db, Workers = 2, CheckpointInterval = TimeSpan.FromMilliseconds(100),
                    ReleaseInterval = TimeSpan.Zero, Settings = new CaptureSettings { StartPosition = StartPosition.Auto }, SnapshotRequests = requests, Gaps = gaps,
                }, new DmvPollSignal(TestServer.ConnectionString(), TimeSpan.FromMilliseconds(20)), new EventEncoder(new EncoderOptions { ServerName = "it" }), sink, store);
                await engine.StartAsync(CancellationToken.None);
                return (engine, store);
            }

            var (engine, store) = await StartAsync();
            for (var i = 1; i <= 3; i++) { await h.SqlAsync($"INSERT dbo.t (id, v) VALUES ({i}, {i});"); await Task.Delay(400); }
            for (var i = 0; i < 50 && engine.Captures.Single().Durable is not { Held: true }; i++) await Task.Delay(100);
            await engine.DisposeAsync();
            await store.DisposeAsync();
            var position = (await Position(state))!.Value;

            await h.SqlAsync("INSERT dbo.t (id, v) VALUES (50, 50); UPDATE dbo.t SET v = 22 WHERE id = 2;");
            await h.SqlAsync("EXEC sys.sp_repldone @xactid = NULL, @xact_seqno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sys.sp_replflush;");
            Assert.True(await TruncatePastAsync(h, position));

            (engine, store) = await StartAsync();
            await h.SqlAsync("INSERT dbo.t (id, v) VALUES (51, 51);");
            var status = "";
            for (var i = 0; i < 150 && status is not ("done" or "partial" or "failed"); i++)
            {
                await Task.Delay(200);
                status = await Scalar(state, "SELECT MAX(status) FROM dbo.snapshot_requests") as string ?? "";
            }
            await Task.Delay(500);
            await engine.DisposeAsync();
            await store.DisposeAsync();
            Assert.Equal("done", status);
            Assert.Equal(1, await Scalar(state, "SELECT COUNT(*) FROM dbo.gaps WHERE continuity = 'Broken' AND resync = 1 AND table_name IS NULL"));

            foreach (var m in sink.Snapshot()) await h.Sink.SendAsync([m], CancellationToken.None);
            Assert.Empty(await h.DiffAsync("t"));
        }
        finally { await TestServer.DropDatabaseAsync(state); }

        static async Task<object?> Scalar(string db, string sql)
        {
            await using var conn = new SqlConnection(TestServer.ConnectionString(db));
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(sql, conn);
            return await cmd.ExecuteScalarAsync();
        }
        static async Task<Lsn?> Position(string db) => await Scalar(db, "SELECT restart_lsn FROM dbo.checkpoints") is byte[] b ? Lsn.FromBinary(b) : null;
    }

    [Theory]
    [InlineData("EXEC sys.sp_repldone @xactid = NULL, @xact_seqno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sys.sp_replflush;")]
    [InlineData("""
        EXEC sys.sp_cdc_disable_db; CHECKPOINT;
        """)]
    public async Task Log_released_by_someone_else_is_a_gap(string release)
    {
        await using var h = await LiveAsync();
        var position = await PositionAsync(h);
        await h.SqlAsync("INSERT dbo.t (id, v) VALUES (50, 50);"); // committed while the miner is down ...
        await h.SqlAsync(release);                                  // ... and released by someone else ...
        Assert.True(await TruncatePastAsync(h, position));          // ... and gone
        if (release.Contains("disable_db", StringComparison.Ordinal))
        {
            await h.SqlAsync("EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL;");
            await h.SqlAsync("EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';");
        }
        await h.RestartAsync();
        await InsertAsync(h, 51);
        await h.PumpAllAsync();
        Assert.Equal(LogContinuity.Broken, Assert.Single(h.Gaps, g => g.Table is null).Continuity); // (CDC off and on again is a gap of the table, too)
        Assert.Empty(h.JournalDiff()); // everything outside the gap, including what came after
        // the position moves past the gap: it is not reported again
        await h.CheckpointAsync(release: true);
        await h.RestartAsync();
        await InsertAsync(h, 52);
        await h.PumpAllAsync();
        Assert.Single(h.Gaps, g => g.Table is null);
        Assert.Empty(h.JournalDiff());
    }

    private static string Enable(string instance = "dbo_t") =>
        $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL, @capture_instance = N'{instance}'; " +
        "IF OBJECT_ID(N'msdb.dbo.cdc_jobs') IS NOT NULL EXEC(N'" + // only exists while some database has CDC jobs
        "IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N''capture'') EXEC sys.sp_cdc_drop_job @job_type = N''capture''; " +
        "IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N''cleanup'') EXEC sys.sp_cdc_drop_job @job_type = N''cleanup'';');";

    private static string Disable(string instance) => $"EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N't', @capture_instance = N'{instance}';";

    /// <summary>CDC disabled on one table and enabled again (e.g. to change its column list): changes in between leave no trace in the log.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Cdc_disabled_and_enabled_again_on_a_table_is_a_gap(bool restart)
    {
        await using var h = await LiveAsync();
        await h.SqlAsync(Disable("all"));
        await h.SqlAsync("INSERT dbo.t (id, v) VALUES (50, 50);"); // not captured at all
        await h.SqlAsync(Enable());
        if (restart) await h.RestartAsync();
        await InsertAsync(h, 51);
        await h.PumpAllAsync();
        var gap = Assert.Single(h.Gaps);
        Assert.Equal(("dbo.t", restart ? LogContinuity.NewTable : LogContinuity.Broken), (gap.Table, gap.Continuity)); // after a restart it looks like a new table: same remedy
        Assert.Empty(h.JournalDiff());
        await h.CheckpointAsync(release: true);
        await h.RestartAsync();
        await InsertAsync(h, 52);
        await h.PumpAllAsync();
        Assert.Single(h.Gaps); // once
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Replacing_the_capture_instance_without_a_break_is_not_a_gap(bool restart)
    {
        await using var h = await LiveAsync();
        await h.SqlAsync(Enable("second"));
        await InsertAsync(h, 50);
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        await h.SqlAsync(Disable("dbo_t"));
        if (restart) await h.RestartAsync();
        await InsertAsync(h, 51);
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        await h.SqlAsync(Enable("third")); // an additional instance while down: the second one still covers the table
        if (restart) await h.RestartAsync();
        await h.SqlAsync("ALTER TABLE dbo.t ADD x int NULL;"); // forces a reload
        await InsertAsync(h, 52);
        await h.PumpAllAsync();
        Assert.Empty(h.Gaps);
        Assert.Empty(h.JournalDiff());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_table_that_is_captured_later_is_reported(bool restart)
    {
        await using var h = await LiveAsync();
        await h.SqlAsync("CREATE TABLE dbo.late (id int NOT NULL PRIMARY KEY); INSERT dbo.late VALUES (1);");
        await h.SqlAsync("EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'late', @role_name = NULL;");
        if (restart) await h.RestartAsync();
        await h.SqlAsync("INSERT dbo.late VALUES (2);");
        await h.PumpAllAsync();
        var gap = Assert.Single(h.Gaps);
        Assert.Equal(("dbo.late", LogContinuity.NewTable), (gap.Table, gap.Continuity));
        Assert.Equal(["2"], h.Replica("late").Keys);
        await h.CheckpointAsync(release: true);
        await h.RestartAsync();
        await h.SqlAsync("INSERT dbo.late VALUES (3);");
        await h.PumpAllAsync();
        Assert.Single(h.Gaps);
    }

    [Fact]
    public async Task A_foreign_release_is_reported_while_the_log_is_still_there()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t"]);
        var sink = new MemorySink();
        await using var engine = new CaptureEngine(new CaptureOptions
        {
            ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n == h.Db, Workers = 1, CheckpointInterval = TimeSpan.FromMilliseconds(100),
            ReleaseInterval = TimeSpan.Zero, Settings = new CaptureSettings { StartPosition = StartPosition.Auto }, HoldCheckInterval = TimeSpan.FromHours(1),
        }, new DmvPollSignal(TestServer.ConnectionString(), TimeSpan.FromMilliseconds(20)), new EventEncoder(new EncoderOptions { ServerName = "it" }), sink, new MemoryCheckpointStore());
        await engine.StartAsync(CancellationToken.None);
        for (var i = 1; i <= 3; i++)
        {
            await h.SqlAsync($"INSERT dbo.t (id, v) VALUES ({i}, 0);");
            await Task.Delay(400);
        }
        var capture = engine.Captures.Single();
        for (var i = 0; i < 50 && capture.Durable is not { Held: true }; i++) await Task.Delay(100);
        Assert.True(capture.Durable!.Held);
        await engine.CheckHoldsAsync(CancellationToken.None);
        Assert.Empty(engine.ForeignReleases);
        await h.SqlAsync("EXEC sys.sp_repldone @xactid = NULL, @xact_seqno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sys.sp_replflush;");
        await engine.CheckHoldsAsync(CancellationToken.None);
        Assert.Equal([h.Db], engine.ForeignReleases);
        Assert.Equal(3, sink.Snapshot().Count);
    }

    [Fact]
    public async Task A_gap_and_its_resync_request_are_recorded_together()
    {
        var db = "lm_state_" + Guid.NewGuid().ToString("N")[..10];
        await TestServer.ExecAsync($"CREATE DATABASE [{db}];");
        try
        {
            var requests = new SnapshotRequests(TestServer.ConnectionString(db), "srv");
            var log = new SqlGapLog(TestServer.ConnectionString(db), "srv");
            await requests.EnsureSchemaAsync(CancellationToken.None);
            await log.EnsureSchemaAsync(CancellationToken.None);
            var gap = new CaptureGap("tenant_1", "inc", new Lsn(1, 2, 3), new Lsn(1, 9, 1), LogContinuity.Broken, "released", DateTimeOffset.UtcNow);
            await log.RecordAsync(gap, resync: true, CancellationToken.None);
            await log.RecordAsync(gap, resync: true, CancellationToken.None); // still requested: not twice
            await log.RecordAsync(gap with { Table = "dbo.t" }, resync: true, CancellationToken.None); // covered by the whole database
            var claimed = Assert.Single(await requests.ClaimAsync(CancellationToken.None));
            Assert.Equal(("tenant_1", null), (claimed.Database, claimed.Table));
            await using var conn = new SqlConnection(TestServer.ConnectionString(db));
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("SELECT COUNT(*) FROM dbo.gaps WHERE database_name = 'tenant_1' AND continuity = 'Broken' AND resync = 1", conn);
            Assert.Equal(3, await cmd.ExecuteScalarAsync());
        }
        finally { await TestServer.DropDatabaseAsync(db); }
    }
}
