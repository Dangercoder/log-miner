using LogMiner.Capture;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>Changes of a table's rows that are not in the log as rows (experiments/32): they cannot be published, so they must be reported.</summary>
public class UnloggedChangeTests
{
    private const string Ddl = """
        CREATE PARTITION FUNCTION pf (int) AS RANGE RIGHT FOR VALUES (100, 200); CREATE PARTITION SCHEME ps AS PARTITION pf ALL TO ([PRIMARY]);
        CREATE TABLE dbo.p (id int NOT NULL PRIMARY KEY, v varchar(20) NULL) ON ps (id);
        CREATE TABLE dbo.p1 (id int NOT NULL PRIMARY KEY, v varchar(20) NULL, CHECK (id < 100));
        CREATE TABLE dbo.p2 (id int NOT NULL PRIMARY KEY, v varchar(20) NULL, CHECK (id >= 100 AND id < 200));
        CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v varchar(20) NULL);
        CREATE TABLE dbo.s (id int NOT NULL PRIMARY KEY, v varchar(20) NULL);
        CREATE TABLE dbo.r (id int NOT NULL PRIMARY KEY, d decimal(9,4) NULL, s varchar(20) NULL);
        CREATE TABLE dbo.other (id int NOT NULL PRIMARY KEY, v int NULL);
        """;

    [Theory]
    [InlineData("ALTER TABLE dbo.t SWITCH TO dbo.s", "dbo.t", true)]
    [InlineData("ALTER TABLE dbo.t SWITCH TO dbo.s", "dbo.t", false)]
    [InlineData("ALTER TABLE dbo.p SWITCH PARTITION 1 TO dbo.p1", "dbo.p", true)]
    [InlineData("ALTER TABLE dbo.p SWITCH PARTITION 1 TO dbo.p1", "dbo.p", false)]
    [InlineData("INSERT dbo.p2 VALUES (160, 'in'); DELETE dbo.p WHERE id BETWEEN 100 AND 199; ALTER TABLE dbo.p2 SWITCH TO dbo.p PARTITION 2", "dbo.p", true)]
    [InlineData("INSERT dbo.p2 VALUES (160, 'in'); DELETE dbo.p WHERE id BETWEEN 100 AND 199; ALTER TABLE dbo.p2 SWITCH TO dbo.p PARTITION 2", "dbo.p", false)]
    [InlineData("ALTER TABLE dbo.p SWITCH PARTITION 1 TO dbo.p1; DROP TABLE dbo.p1", "dbo.p", false)]
    public async Task A_switch_is_recorded_whether_the_miner_is_there_or_not(string ddl, string table, bool live)
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t", "p", "r"]);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'a'); INSERT dbo.p VALUES (1, 'a'), (150, 'b'), (250, 'c');");
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        Assert.Empty(h.Gaps);
        await h.SqlAsync(ddl);
        if (!live) await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Contains(h.Gaps, g => g.Continuity == LogContinuity.Switched && g.Table == table);
        Assert.All(h.Gaps, g => Assert.Equal(table, g.Table));
        var recorded = h.Gaps.Count;
        await h.CheckpointAsync();
        await h.SqlAsync("INSERT dbo.t VALUES (900, 'later'); INSERT dbo.p VALUES (901, 'later');"); // the stream goes on, and it is recorded once
        await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal(recorded, h.Gaps.Count);
        Assert.Equal(2, h.Events().Count(e => e.GetProperty("op").GetString() == "c" && e.GetProperty("after").GetProperty("v").GetString() == "later"));
    }

    [Theory]
    [InlineData("ALTER INDEX ALL ON dbo.p REBUILD")]
    [InlineData("ALTER TABLE dbo.p ADD x int NULL")]
    [InlineData("ALTER TABLE dbo.s SWITCH TO dbo.s2", "CREATE TABLE dbo.s2 (id int NOT NULL PRIMARY KEY, v varchar(20) NULL)")]
    [InlineData("ALTER PARTITION FUNCTION pf() SPLIT RANGE (300)")]
    [InlineData("ALTER PARTITION FUNCTION pf() MERGE RANGE (200)")]
    public async Task What_moves_no_rows_between_tables_is_not_taken_for_a_switch(string ddl, string before = "SELECT 1")
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t", "p", "r"]);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'a'); INSERT dbo.p VALUES (1, 'a'), (150, 'b'), (250, 'c'); " + before);
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        var events = h.Events().Count();
        await h.SqlAsync(ddl);
        await h.PumpAllAsync();
        await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Empty(h.Gaps);
        Assert.Equal(events, h.Events().Count()); // maintenance is not a change
        await h.SqlAsync("UPDATE dbo.p SET v = 'z'");
        await h.PumpAllAsync();
        Assert.Equal(events + 3, h.Events().Count());
        Assert.Empty(await h.DiffAsync("p"));
    }

    [Theory]
    [InlineData("ALTER TABLE dbo.r ALTER COLUMN d decimal(9,1) NULL", "the values of column d were converted")]
    [InlineData("ALTER TABLE dbo.r ADD f int NOT NULL DEFAULT 5", "column f was added with a value")]
    [InlineData("ALTER TABLE dbo.r ADD f int NULL", null)]
    [InlineData("ALTER TABLE dbo.r ALTER COLUMN s varchar(40) NULL", null)]
    [InlineData("ALTER TABLE dbo.r DROP COLUMN s", null)]
    public async Task DDL_that_changes_values_of_rows_nobody_wrote_is_reported(string ddl, string? notice)
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t", "p", "r"]);
        await h.SqlAsync("INSERT dbo.r VALUES (1, 1.2345, 'x');");
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        await h.SqlAsync(ddl);
        await h.PumpAllAsync();
        var notices = new List<string>();
        while (h.Capture.TryTakeNotice(out var n)) notices.Add(n);
        if (notice is null) Assert.Empty(notices); else Assert.Contains(notice, Assert.Single(notices), StringComparison.Ordinal);
        await h.SqlAsync("UPDATE dbo.r SET d = 2"); // and what is written afterwards is exact
        await h.PumpAllAsync();
        Assert.Empty(await h.DiffAsync("r"));
        Assert.Empty(h.Gaps);
    }

    /// <summary>A table that is too large to be dropped or rebuilt at once (over 128 extents: deallocated later), while the miner is down.</summary>
    [Theory]
    [InlineData("ALTER INDEX ALL ON dbo.big REBUILD")]
    [InlineData("DROP TABLE dbo.big")]
    [InlineData("ALTER TABLE dbo.big ALTER COLUMN pad char(1200) NOT NULL")]
    public async Task Large_tables_are_decoded_after_a_rebuild_or_drop(string ddl)
    {
        await using var h = await CaptureHarness.CreateAsync("CREATE TABLE dbo.big (id int NOT NULL PRIMARY KEY, pad char(1000) NOT NULL, v int NOT NULL);", ["big"]);
        await h.SqlAsync("INSERT dbo.big SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'x', 0 FROM sys.all_columns a CROSS JOIN sys.all_columns b;");
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        await h.SqlAsync("UPDATE dbo.big SET v = 1 WHERE id % 1000 = 0;");
        if (ddl.StartsWith("DROP", StringComparison.Ordinal)) await h.SqlAsync("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'big', @capture_instance = N'all';");
        await h.SqlAsync(ddl);
        await Task.Delay(1500); // the deferred part runs in the background
        if (!ddl.StartsWith("DROP", StringComparison.Ordinal)) await h.SqlAsync("UPDATE dbo.big SET v = 2 WHERE id % 2000 = 0;");
        await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal(20, h.Events().Count(e => e.GetProperty("op").GetString() == "u" && e.GetProperty("after").GetProperty("v").GetInt32() == 1));
        if (!ddl.StartsWith("DROP", StringComparison.Ordinal)) Assert.Equal(10, h.Events().Count(e => e.GetProperty("op").GetString() == "u" && e.GetProperty("after").GetProperty("v").GetInt32() == 2));
    }

    /// <summary>A DDL transaction that was open before the held-back one began and commits after it. SQL Server keeps its beginning in the log, too
    /// (experiments/30): what was open then can always be read whole.</summary>
    [Fact]
    public async Task A_DDL_transaction_that_overlaps_the_held_back_one_keeps_its_beginning_in_the_log()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl + "CREATE TABLE dbo.noise (id int IDENTITY PRIMARY KEY, pad char(4000) NOT NULL);", ["t", "p", "r"]);
        await h.SqlAsync("INSERT dbo.t VALUES (0, 'first');");
        await h.PumpAllAsync(); // the layouts are loaded before anything holds catalog rows
        await using var other = new Microsoft.Data.SqlClient.SqlConnection(TestServer.ConnectionString(h.Db) + ";Pooling=false");
        await other.OpenAsync();
        async Task OtherAsync(string sql) { await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, other); await cmd.ExecuteNonQueryAsync(); }
        await OtherAsync("BEGIN TRAN; CREATE TABLE dbo.x (a int);");
        decimal began;
        await using (var cmd = new Microsoft.Data.SqlClient.SqlCommand("SELECT database_transaction_begin_lsn FROM sys.dm_tran_database_transactions WHERE database_id = DB_ID() AND transaction_id = CURRENT_TRANSACTION_ID()", other)) began = (decimal)(await cmd.ExecuteScalarAsync())!;
        await h.SqlAsync("INSERT dbo.t VALUES (1, 'bookmark');");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        await OtherAsync("ALTER TABLE dbo.x ADD b int; COMMIT;");
        async Task<decimal> OldestAsync()
        {
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand("SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID())", other);
            var lsn = Lsn.Parse((string)(await cmd.ExecuteScalarAsync())!);
            return lsn.Vlf * 1_000_000_000_000_000m + lsn.Block * 100_000m + lsn.Slot;
        }
        for (var i = 0; i < 6; i++)
        {
            await h.SqlAsync("INSERT dbo.noise (pad) SELECT TOP (1500) 'x' FROM sys.all_objects a CROSS JOIN sys.all_objects b; TRUNCATE TABLE dbo.noise; CHECKPOINT;");
            await TestServer.ExecAsync($"BACKUP LOG [{h.Db}] TO DISK = 'NUL';");
        }
        Assert.True(await OldestAsync() <= began, "its beginning is gone");
        await h.SqlAsync("INSERT dbo.t VALUES (2, 'after');");
        await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal([0, 1, 2], h.Events().Select(e => e.GetProperty("after").GetProperty("id").GetInt32()));
        Assert.Empty(h.Gaps);
    }
}
