using LogMiner.Capture;
using LogMiner.Log;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>What the stream's completeness rests on, against the real engine (experiments/28-31).</summary>
public class StreamIntegrityTests
{
    private const string Ddl = """
        CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v varchar(100) NULL);
        CREATE TABLE dbo.ix (id int NOT NULL PRIMARY KEY, a int NOT NULL, b int NOT NULL); CREATE UNIQUE INDEX ux ON dbo.ix (a);
        CREATE TABLE dbo.h (id int NOT NULL, a int NOT NULL, v varchar(7000) NULL); CREATE UNIQUE INDEX uh ON dbo.h (a);
        CREATE TABLE dbo.doc (id int NOT NULL PRIMARY KEY, d nvarchar(max) NULL);
        CREATE TABLE dbo.noise (id int IDENTITY PRIMARY KEY, pad char(4000) NOT NULL);
        """;
    private static readonly string[] Tables = ["t", "ix", "h", "doc"];

    private static async Task<List<LogRecord>> ReadAsync(CaptureHarness h, Lsn? start, int maxRows = 1_000_000)
    {
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        var records = new List<LogRecord>();
        await foreach (var r in new EngineLogReader(conn).ReadAsync(h.Capture.Database, start, maxRows, CaptureScope.CdcTrackedTables, CancellationToken.None)) records.Add(r);
        return records;
    }

    /// <summary>The log function also returns blocks that are still in memory. If the server died now, that transaction would never have
    /// happened, and its LSNs would be used again.</summary>
    [Fact]
    public async Task A_commit_that_is_not_on_disk_yet_is_not_published()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, Tables);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'before')");
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        Assert.Single(h.Events());
        await using var freezer = new SqlConnection(TestServer.ConnectionString() + ";Pooling=false");
        await freezer.OpenAsync();
        await using (var cmd = new SqlCommand($"ALTER DATABASE [{h.Db}] SET SUSPEND_FOR_SNAPSHOT_BACKUP = ON", freezer)) await cmd.ExecuteNonQueryAsync();
        Task insert;
        try
        {
            insert = h.SqlAsync("INSERT dbo.t VALUES (2, 'hardening')");
            await Task.Delay(700);
            Assert.False(insert.IsCompleted);
            await using (var conn = new SqlConnection(TestServer.ConnectionString()))
            {
                await conn.OpenAsync();
                await using var look = new SqlCommand($"""
                    DECLARE @id int = DB_ID(N'{h.Db}'), @s nvarchar(46) = (SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID(N'{h.Db}')));
                    SELECT COUNT(*) FROM sys.fn_full_dblog('0x' + @s, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                    WHERE Operation = N'LOP_COMMIT_XACT' AND [Current LSN] >= (SELECT log_end_lsn FROM sys.dm_db_log_stats(@id))
                    """, conn);
                Assert.True((int)(await look.ExecuteScalarAsync())! > 0, "the premise: the function shows the commit already");
            }
            for (var i = 0; i < 3; i++) await h.PumpOnceAsync();
            await h.CheckpointAsync();
            Assert.Single(h.Events());
        }
        finally
        {
            await using var cmd = new SqlCommand($"ALTER DATABASE [{h.Db}] SET SUSPEND_FOR_SNAPSHOT_BACKUP = OFF", freezer);
            await cmd.ExecuteNonQueryAsync();
        }
        await insert;
        await h.PumpAllAsync();
        Assert.Equal(2, h.Events().Count());
    }

    [Fact]
    public async Task A_start_that_is_not_in_the_log_is_refused_never_moved()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, Tables);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'x')");
        await h.PumpAllAsync();
        var all = await ReadAsync(h, null);
        Assert.Equal(all[^1].Lsn, (await ReadAsync(h, all[^1].Lsn))[0].Lsn); // the last record is still there
        await Assert.ThrowsAsync<LogTruncatedException>(() => ReadAsync(h, new Lsn(1, 16, 1)));
        await Assert.ThrowsAsync<LogTruncatedException>(() => ReadAsync(h, new Lsn(all[0].Lsn.Vlf, all[0].Lsn.Block - 1, 1)));
        var beyond = await Assert.ThrowsAsync<SqlException>(() => ReadAsync(h, new Lsn(all[^1].Lsn.Vlf + 1000, 16, 1)));
        Assert.Contains("put back to an earlier state", beyond.Message, StringComparison.Ordinal);
        Assert.Equal(all.Select(r => r.Lsn), all.Select(r => r.Lsn).Order().Distinct());
        // cut anywhere, the reads together see the same records
        foreach (var max in new[] { 2, 3, 7, 50 })
        {
            var pieces = new List<Lsn>();
            Lsn? from = null;
            while (true)
            {
                var piece = await ReadAsync(h, from, max);
                pieces.AddRange(piece.Select(r => r.Lsn).Where(l => pieces.Count == 0 || l > pieces[^1]));
                if (piece.Count < max) break;
                from = piece[^1].Lsn;
            }
            Assert.Equal(all.Select(r => r.Lsn), pieces.Take(all.Count));
        }
    }

    /// <summary>Other activity and log backups move the start of the log past a caught-up reader's position (a block marker) by a few slots.
    /// Nothing is missing then.</summary>
    [Fact]
    public async Task Ordinary_truncation_behind_a_caught_up_reader_is_not_a_gap()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, Tables);
        await h.SqlAsync($"DECLARE @i int = 0; WHILE @i < 600 AND NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID() AND encryption_state = 3) BEGIN WAITFOR DELAY '00:00:00.1'; SET @i += 1; END"); // the encryption scan logs, too
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'x')");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        for (var i = 0; i < 25; i++)
        {
            await h.SqlAsync("INSERT dbo.noise (pad) VALUES ('y'); CHECKPOINT;");
            await h.PumpAllAsync();
            await h.CheckpointAsync(release: true);
            if (i % 3 == 0) await h.RestartAsync();
            await h.PumpAllAsync();
        }
        Assert.Empty(h.Gaps);
        await h.SqlAsync("INSERT dbo.t VALUES (2, 'y')");
        await h.PumpAllAsync();
        Assert.Equal(2, h.Events().Count());
    }

    /// <summary>The oldest record is exact to the slot, a caught-up reader's position is the beginning of a block: in the block of the oldest
    /// record nothing is missing that is needed.</summary>
    [Fact]
    public async Task A_position_in_the_block_of_the_oldest_record_is_still_in_the_log()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, Tables);
        await h.SqlAsync($"DECLARE @i int = 0; WHILE @i < 600 AND NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID() AND encryption_state = 3) BEGIN WAITFOR DELAY '00:00:00.1'; SET @i += 1; END");
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'x')");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        Assert.True(h.Capture.RequestFullRelease()); // nothing is held back: the log begins at a checkpoint, wherever that is in its block
        await h.PumpAllAsync();
        async Task<Lsn> OldestAsync()
        {
            await using var conn = new SqlConnection(TestServer.ConnectionString(h.Db));
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID())", conn);
            return Lsn.Parse((string)(await cmd.ExecuteScalarAsync())!);
        }
        var oldest = default(Lsn);
        for (var i = 0; i < 40 && oldest.Slot <= 1; i++)
        {
            await h.CheckpointAsync(release: true);
            await h.SqlAsync("BEGIN TRAN; INSERT dbo.noise (pad) VALUES ('z'); ROLLBACK; CHECKPOINT;"); // a rollback does not flush: the checkpoint begins behind it in the block
            await TestServer.ExecAsync($"BACKUP LOG [{h.Db}] TO DISK = 'NUL';");
            oldest = await OldestAsync();
        }
        Assert.True(oldest.Slot > 1, $"the premise: the log begins in the middle of a block ({oldest})");
        Assert.Equal(oldest, (await ReadAsync(h, new Lsn(oldest.Vlf, oldest.Block, 1)))[0].Lsn);
        await Assert.ThrowsAsync<LogTruncatedException>(() => ReadAsync(h, new Lsn(oldest.Vlf, oldest.Block - 1, 1)));
    }

    /// <summary>A replicated transaction of which nothing is left has no events, but SQL Server holds its log until it is released.</summary>
    [Fact]
    public async Task A_transaction_of_which_nothing_is_left_is_released_too()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, Tables);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'x')");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        await h.SqlAsync("BEGIN TRAN; SAVE TRAN s; INSERT dbo.t VALUES (2, 'x'); ROLLBACK TRAN s; COMMIT;");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        Assert.True(h.Capture.RequestFullRelease());
        await h.CheckpointAsync(release: true);
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        Assert.Null(await ReplDoneClient.OldestUnreleasedAsync(conn, h.Capture.Database, CancellationToken.None));
        Assert.Single(h.Events());
    }

    /// <summary>Partial rollbacks of every kind, however the engine logs them, and SQL Server's own record count at every commit.</summary>
    [Theory]
    [InlineData("")]
    [InlineData("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE; ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;")]
    [InlineData("ALTER DATABASE CURRENT SET ACCELERATED_DATABASE_RECOVERY = ON WITH ROLLBACK IMMEDIATE;")]
    [InlineData("ALTER DATABASE CURRENT SET ACCELERATED_DATABASE_RECOVERY = ON WITH ROLLBACK IMMEDIATE; ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;")]
    public async Task What_is_rolled_back_is_not_published_and_everything_else_is(string options)
    {
        await using var h = await CaptureHarness.CreateAsync(options + Ddl, Tables);
        string[] batches =
        [
            "INSERT dbo.t VALUES (1, 'one'), (2, 'two'), (3, 'three'); INSERT dbo.ix VALUES (500, 500, 0); INSERT dbo.h VALUES (500, 500, 'x'); INSERT dbo.doc VALUES (1, REPLICATE(CONVERT(nvarchar(max), N'd'), 9000));",
            "BEGIN TRAN; BEGIN TRY INSERT dbo.ix VALUES (1, 7, 1), (2, 7, 2); END TRY BEGIN CATCH END CATCH; INSERT dbo.t VALUES (10, 'after'); COMMIT;",
            "BEGIN TRAN; BEGIN TRY INSERT dbo.ix VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 500, 5); END TRY BEGIN CATCH END CATCH; INSERT dbo.ix VALUES (6, 6, 6); COMMIT;",
            "BEGIN TRAN; BEGIN TRY UPDATE dbo.ix SET a = a + 1 WHERE id = 500; UPDATE dbo.t SET v = CONVERT(varchar(100), 1 / (id - 3)); END TRY BEGIN CATCH END CATCH; COMMIT;",
            "BEGIN TRAN; INSERT dbo.t VALUES (20, 'keep'); SAVE TRAN s; INSERT dbo.t VALUES (21, 'x'); UPDATE dbo.t SET v = 'y' WHERE id = 20; DELETE dbo.t WHERE id = 1; UPDATE dbo.t SET v = 'z' WHERE id = 20; ROLLBACK TRAN s; UPDATE dbo.t SET v = 'kept' WHERE id = 20; COMMIT;",
            "BEGIN TRAN; SAVE TRAN s; UPDATE dbo.t SET v = 'a' WHERE id = 2; UPDATE dbo.t SET v = 'b' WHERE id = 2; UPDATE dbo.t SET v = 'c' WHERE id = 2; ROLLBACK TRAN s; COMMIT;",
            "BEGIN TRAN; BEGIN TRY INSERT dbo.h VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 500, 'c'); END TRY BEGIN CATCH END CATCH; INSERT dbo.h VALUES (4, 4, 'd'); COMMIT;",
            "BEGIN TRAN; SAVE TRAN s; INSERT dbo.h VALUES (7, 7, 'x'); UPDATE dbo.h SET v = REPLICATE('y', 6000) WHERE id = 7; DELETE dbo.h WHERE id = 7; ROLLBACK TRAN s; COMMIT;",
            "BEGIN TRAN; INSERT dbo.t VALUES (30, 'x'), (31, 'y'); UPDATE dbo.t SET v = 'q' WHERE id = 3; ROLLBACK;",
            "BEGIN TRAN; SAVE TRAN s; INSERT dbo.t SELECT 1000 + n.n, 'w' FROM (SELECT TOP (300) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns) n; ROLLBACK TRAN s; INSERT dbo.t VALUES (999, 'kept'); COMMIT;",
            "BEGIN TRAN; INSERT dbo.t SELECT 5000 + n.n, 'w' FROM (SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b) n; ROLLBACK;",
            "BEGIN TRAN; INSERT dbo.t VALUES (60000, 'keep'); SAVE TRAN s; INSERT dbo.t SELECT 5000 + n.n, 'w' FROM (SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b) n; ROLLBACK TRAN s; INSERT dbo.t VALUES (60001, 'keep'); COMMIT;",
            "BEGIN TRAN; INSERT dbo.t VALUES (61000, 'keep'); SAVE TRAN s1; INSERT dbo.t VALUES (61001, 'x'); SAVE TRAN s2; INSERT dbo.t VALUES (61002, 'x'); ROLLBACK TRAN s2; INSERT dbo.t VALUES (61003, 'x'); ROLLBACK TRAN s1; INSERT dbo.t VALUES (61004, 'keep'); SAVE TRAN s3; INSERT dbo.t VALUES (61005, 'x'); ROLLBACK TRAN s3; COMMIT;",
            "SET XACT_ABORT ON; BEGIN TRY BEGIN TRAN; INSERT dbo.t VALUES (62000, 'x'); INSERT dbo.t VALUES (62000, 'dup'); COMMIT; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK; END CATCH; SET XACT_ABORT OFF;",
            "BEGIN TRAN; UPDATE dbo.doc SET d = d + N'e'; SAVE TRAN s; UPDATE dbo.doc SET d.WRITE(N'zz', 5, 2); UPDATE dbo.doc SET d = NULL; ROLLBACK TRAN s; UPDATE dbo.doc SET d.WRITE(N'yy', 5, 2); COMMIT;",
            "BEGIN TRAN; DELETE dbo.t WHERE id = 10; SAVE TRAN s; UPDATE dbo.t SET id = id + 100 WHERE id IN (2, 3); ROLLBACK TRAN s; UPDATE dbo.t SET id = id + 200 WHERE id IN (2, 3); COMMIT;",
            "MERGE dbo.ix AS t USING (VALUES (6, 60), (8, 80)) AS s (id, a) ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = s.a WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, 0);",
        ];
        foreach (var b in batches) await h.SqlAsync(b);
        await h.PumpAllAsync();
        Assert.Empty(await h.DiffAsync("t"));
        Assert.Empty(await h.DiffAsync("ix"));
        Assert.Empty(await h.DiffAsync("h"));
        Assert.Empty(h.Gaps);
        Assert.Equal(4, h.Events().Count(e => e.GetProperty("source").GetProperty("table").GetString() == "doc") + 1); // insert, two updates that stayed
        Assert.True(h.Capture.RecordsRead > 0);
        Assert.Equal(0, h.Capture.OpenTransactions);
    }
}
