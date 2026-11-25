using LogMiner.Sinks;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// The log being read can be older than the catalog: the miner was down, the sink was out, a fault rewound the stream, or a migration
/// ran faster than the miner. Every change must still be delivered, decoded with the layout it was written with. The oracle is a
/// journal of each row's exact image at the time of each change (<see cref="CaptureHarness.DmlAsync"/>).
/// </summary>
public class SchemaEvolutionTests
{
    private const string Ddl = "CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL, s varchar(40) NULL);";
    private static readonly Func<CaptureHarness, Task> Rebuild = h => h.DdlAsync("ALTER INDEX ALL ON dbo.t REBUILD;");

    private static Func<CaptureHarness, Task> Sql(string ddl) => h => h.DdlAsync(ddl);

    private static async Task Dml1(CaptureHarness h)
    {
        await h.DmlAsync("c", 6, "INSERT dbo.t (id, v, s) VALUES (6, 60, 'six');");
        await h.DmlAsync("c", 7, "INSERT dbo.t (id, v, s) VALUES (7, 70, NULL);");
        await h.DmlAsync("u", 1, "UPDATE dbo.t SET v = 11 WHERE id = 1;");
        await h.DmlAsync("d", 2, "DELETE dbo.t WHERE id = 2;");
    }

    private static async Task Dml2(CaptureHarness h)
    {
        await h.DmlAsync("c", 8, "INSERT dbo.t (id, v, s) VALUES (8, 80, 'eight');");
        await h.DmlAsync("u", 6, "UPDATE dbo.t SET s = 'sechs' WHERE id = 6;");
        await h.DmlAsync("u", 1, "UPDATE dbo.t SET v = v + 1 WHERE id = 1;");
    }

    private static async Task Dml3(CaptureHarness h)
    {
        await h.DmlAsync("c", 9, "INSERT dbo.t (id, v, s) VALUES (9, 90, 'nine');");
        await h.DmlAsync("u", 3, "UPDATE dbo.t SET v = 33 WHERE id = 3;");
        await h.DmlAsync("d", 1, "DELETE dbo.t WHERE id = 1;");
    }

    private static async Task<CaptureHarness> LiveAsync(string ddl = Ddl, params string[] tables)
    {
        var h = await CaptureHarness.CreateAsync(ddl, tables.Length == 0 ? ["t"] : tables);
        await h.DmlAsync("c", 1, "INSERT dbo.t (id, v, s) VALUES (1, 10, 'one');");
        await h.DmlAsync("c", 2, "INSERT dbo.t (id, v, s) VALUES (2, 20, 'two');");
        await h.DmlAsync("c", 3, "INSERT dbo.t (id, v, s) VALUES (3, 30, 'three');");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        Assert.Empty(h.JournalDiff());
        return h;
    }

    private static void AssertJournal(CaptureHarness h)
    {
        var diffs = h.JournalDiff();
        Assert.True(diffs.Count == 0, string.Join("; ", diffs));
    }

    /// <summary>Live and checkpointed, then the steps run while the miner is down, then it restarts.</summary>
    private static async Task WhileDownAsync(string ddl, params Func<CaptureHarness, Task>[] steps)
    {
        await using var h = await LiveAsync(ddl);
        foreach (var s in steps) await s(h);
        await h.RestartAsync();
        await h.PumpAllAsync();
        AssertJournal(h);
    }

    /// <summary>The same steps with the miner running: caught up after each one.</summary>
    private static async Task WhileLiveAsync(string ddl, params Func<CaptureHarness, Task>[] steps)
    {
        await using var h = await LiveAsync(ddl);
        foreach (var s in steps)
        {
            await s(h);
            await h.PumpAllAsync();
            await h.CheckpointAsync();
        }
        AssertJournal(h);
    }

    private static async Task BothAsync(params Func<CaptureHarness, Task>[] steps)
    {
        await WhileDownAsync(Ddl, steps);
        await WhileLiveAsync(Ddl, steps);
    }

    [Fact] public Task Rebuild_of_an_index() => BothAsync(Dml1, Rebuild, Dml2);
    [Fact] public Task Two_rebuilds() => BothAsync(Dml1, Rebuild, Dml2, Rebuild, Dml3);
    [Fact] public Task Online_rebuild() => BothAsync(Dml1, Sql("ALTER INDEX ALL ON dbo.t REBUILD WITH (ONLINE = ON);"), Dml2);
    [Fact] public Task Alter_column_type() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN v bigint NULL;"), Dml2);
    [Fact] public Task Alter_column_online() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN v bigint NULL WITH (ONLINE = ON);"), Dml2);
    [Fact] public Task Widen_varchar() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN s varchar(200) NULL;"), Dml2);
    [Fact] public Task Varchar_to_max_then_back() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN s varchar(max) NULL;"), Dml2, Sql("ALTER TABLE dbo.t ALTER COLUMN s nvarchar(50) NULL;"), Dml3);
    [Fact] public Task Alter_twice() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN v bigint NULL;"), Dml2, Sql("ALTER TABLE dbo.t ALTER COLUMN v decimal(19,2) NULL;"), Dml3);
    /// <summary>Every change comes under the name its table had when it was made, whether the miner was there or not, and consumers are told that the
    /// rows under the new name are others. (Found by a review: until a restart they came under the old name.)</summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task Renamed_and_swapped_tables(bool live, bool inOneTransaction)
    {
        await using var h = await CaptureHarness.CreateAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL); CREATE TABLE dbo.t_new (id int NOT NULL PRIMARY KEY, v int NULL); CREATE TABLE dbo.other (id int NOT NULL PRIMARY KEY);", ["t", "t_new", "other"]);
        var expected = new List<(int Id, string Table)>();
        async Task InsertAsync(string table, int id)
        {
            await h.SqlAsync($"INSERT {table} (id) VALUES ({id});");
            expected.Add((id, table));
            if (live) await h.PumpAllAsync();
        }
        await h.SqlAsync("CREATE SCHEMA archive;");
        await InsertAsync("dbo.t", 1);
        await InsertAsync("dbo.t_new", 2);
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        await InsertAsync("dbo.t", 3);
        var swap = "EXEC sp_rename 'dbo.t', 't_old_with_a_long_name'; EXEC sp_rename 'dbo.t_new', 't';";
        await h.SqlAsync(inOneTransaction ? $"BEGIN TRAN; {swap} COMMIT;" : swap);
        await InsertAsync("dbo.t", 4);                      // was t_new
        await InsertAsync("dbo.t_old_with_a_long_name", 5); // was t
        await h.SqlAsync("EXEC sp_rename 'dbo.t_old_with_a_long_name', 't_odd_with_a_long_name';"); // one character in the middle
        await InsertAsync("dbo.t_odd_with_a_long_name", 6);
        await h.SqlAsync("BEGIN TRAN; EXEC sp_rename 'dbo.t', 'never'; ROLLBACK;");
        await InsertAsync("dbo.t", 7);
        await h.SqlAsync("ALTER SCHEMA archive TRANSFER dbo.t_odd_with_a_long_name;");
        await InsertAsync("archive.t_odd_with_a_long_name", 8);
        await h.SqlAsync("EXEC sp_rename 'archive.t_odd_with_a_long_name', 'x';");
        await InsertAsync("archive.x", 9);
        await InsertAsync("dbo.other", 10);
        if (!live) await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal(expected, h.Events().Select(e => (e.GetProperty("after").GetProperty("id").GetInt32(), $"{e.GetProperty("source").GetProperty("schema").GetString()}.{e.GetProperty("source").GetProperty("table").GetString()}")).OrderBy(e => e.Item1));
        Assert.All(h.Gaps, g => Assert.Equal(LogMiner.Capture.LogContinuity.Renamed, g.Continuity));
        Assert.Contains("dbo.t", h.Gaps.Select(g => g.Table));
        Assert.Contains("archive.x", h.Gaps.Select(g => g.Table));
        Assert.DoesNotContain("dbo.other", h.Gaps.Select(g => g.Table));
        // and once told, never again
        var told = h.Gaps.Count;
        await h.CheckpointAsync(release: true);
        await h.RestartAsync();
        await InsertAsync("dbo.other", 11);
        await h.PumpAllAsync();
        Assert.Equal(told, h.Gaps.Count);
    }

    /// <summary>An altered column has another hbcolid than null bit; widening it is a change in place, logged by hbcolid.</summary>
    [Fact]
    public Task Alter_a_column_then_widen_it_in_place() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN s varchar(40) COLLATE Cyrillic_General_CI_AS NULL;"), Dml2,
        Sql("ALTER TABLE dbo.t ALTER COLUMN s varchar(60) COLLATE Cyrillic_General_CI_AS NULL;"), Dml3);

    /// <summary>A column altered to a type of the same size keeps its null bit and gets another hbcolid, by which the flags that CDC sets on it are logged.</summary>
    [Fact]
    public Task Alter_to_a_type_of_the_same_size_then_CDC_off_and_on() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN v real NULL;"), Dml2,
        Sql("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N't', @capture_instance = N'all';"),
        Sql("EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL; " +
            "IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'capture') EXEC sys.sp_cdc_drop_job N'capture'; " +
            "IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup') EXEC sys.sp_cdc_drop_job N'cleanup';"),
        Dml3);

    [Fact] public Task Alter_then_rebuild() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN v bigint NULL;"), Dml2, Sql("ALTER TABLE dbo.t REBUILD;"), Dml3);
    [Fact] public Task Collation_change() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ALTER COLUMN s varchar(40) COLLATE Cyrillic_General_CI_AS NULL;"),
        h => h.DmlAsync("c", 8, "INSERT dbo.t (id, v, s) VALUES (8, 80, N'Привет');"), Sql("ALTER TABLE dbo.t ALTER COLUMN s varchar(40) COLLATE Greek_CI_AS NULL;"),
        h => h.DmlAsync("c", 9, "INSERT dbo.t (id, v, s) VALUES (9, 90, N'Γειά');"), Rebuild, Dml1Again);
    [Fact] public Task Add_column_with_default() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ADD extra int NOT NULL CONSTRAINT df DEFAULT 5 WITH VALUES;"), Dml2, Rebuild, Dml3);
    [Fact] public Task Add_columns_of_many_types_with_defaults_then_rebuild() => BothAsync(Dml1,
        Sql("""
            ALTER TABLE dbo.t ADD d1 date NOT NULL DEFAULT '2026-11-24', d2 datetime2(3) NOT NULL DEFAULT '2026-11-24T12:34:56.789', d3 time(0) NOT NULL DEFAULT '12:34:56', d4 datetimeoffset(2) NOT NULL DEFAULT '2026-11-24T12:34:56+01:00',
              d5 decimal(30,7) NOT NULL DEFAULT 1.25, d6 money NOT NULL DEFAULT 1.5, d7 smallmoney NOT NULL DEFAULT 2.5, d8 varchar(20) NOT NULL DEFAULT 'abc', d9 char(6) NOT NULL DEFAULT 'abc', d10 nvarchar(20) NOT NULL DEFAULT N'å–Ж',
              d11 varbinary(9) NOT NULL DEFAULT 0x0102, d12 binary(4) NOT NULL DEFAULT 0x0102, d13 uniqueidentifier NOT NULL DEFAULT '00112233-4455-6677-8899-AABBCCDDEEFF', d14 bit NOT NULL DEFAULT 1, d15 float NOT NULL DEFAULT 2.5,
              d16 real NOT NULL DEFAULT 2.5, d17 smalldatetime NOT NULL DEFAULT '2026-11-24T12:34:00', d18 datetime NOT NULL DEFAULT '2026-11-24T12:34:56', d19 bigint NOT NULL DEFAULT 6, d20 tinyint NOT NULL DEFAULT 7, d21 smallint NOT NULL DEFAULT 8;
            """),
        h => h.DmlAsync("u", 1, "UPDATE dbo.t SET v = 12 WHERE id = 1;"), h => h.DmlAsync("d", 7, "DELETE dbo.t WHERE id = 7;"), Rebuild, h => h.DmlAsync("u", 6, "UPDATE dbo.t SET v = 13 WHERE id = 6;"));
    [Fact] public Task Migration_add_backfill_tighten() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t ADD w varchar(20) NULL;"), h => h.DmlAsync("u", 1, "UPDATE dbo.t SET w = 'one' WHERE id = 1;"),
        h => h.DmlAsync("u", 3, "UPDATE dbo.t SET w = 'three' WHERE id = 3;"), h => h.DmlAsync("u", 6, "UPDATE dbo.t SET w = 'six' WHERE id = 6;"), h => h.DmlAsync("u", 7, "UPDATE dbo.t SET w = '7' WHERE id = 7;"),
        Sql("ALTER TABLE dbo.t ALTER COLUMN w varchar(20) NOT NULL;"), Sql("ALTER TABLE dbo.t DROP COLUMN s;"), h => h.DmlAsync("c", 8, "INSERT dbo.t (id, v, w) VALUES (8, 80, 'eight');"),
        Sql("ALTER TABLE dbo.t ADD s int NULL;"), h => h.DmlAsync("u", 8, "UPDATE dbo.t SET s = 5 WHERE id = 8;"), Sql("ALTER TABLE dbo.t REBUILD;"), h => h.DmlAsync("d", 1, "DELETE dbo.t WHERE id = 1;"));
    [Fact] public Task Drop_column_then_rebuild() => BothAsync(Dml1, Sql("ALTER TABLE dbo.t DROP COLUMN s;"), Rebuild, h => h.DmlAsync("c", 8, "INSERT dbo.t (id, v) VALUES (8, 80);"));
    [Fact] public Task Unpublished_ddl_on_other_tables_does_not_matter() => BothAsync(Dml1, Sql("CREATE TABLE dbo.other (a int PRIMARY KEY, b varchar(10)); ALTER TABLE dbo.other ADD c int; DROP TABLE dbo.other;"), Dml2);
    [Fact] public Task Rolled_back_ddl() => BothAsync(Dml1, Sql("BEGIN TRAN; ALTER TABLE dbo.t ADD ghost int NULL; ALTER INDEX ALL ON dbo.t REBUILD; ROLLBACK;"), Dml2, Rebuild, Dml3);

    private static async Task Dml1Again(CaptureHarness h) => await h.DmlAsync("u", 6, "UPDATE dbo.t SET v = 61 WHERE id = 6;");

    [Fact]
    public Task Cluster_on_another_key() => WhileDownAsync("CREATE TABLE dbo.t (a int NULL, id int NOT NULL, v int NULL, s varchar(40) NULL); CREATE CLUSTERED INDEX cx ON dbo.t (a, id); ALTER TABLE dbo.t ADD CONSTRAINT pk PRIMARY KEY NONCLUSTERED (id);",
        Dml1, Sql("CREATE CLUSTERED INDEX cx ON dbo.t (a DESC) WITH (DROP_EXISTING = ON);"), Dml2, Sql("DROP INDEX cx ON dbo.t;"), Dml3); // non-unique key (uniqueifier), then a heap

    [Fact]
    public async Task Page_splits_in_the_catalog_are_not_mistaken_for_ddl()
    {
        // columns added to an older table push the catalog rows of newer ones to other pages (logged as inserts of a system transaction)
        var wide = string.Join(", ", Enumerable.Range(0, 70).Select(c => $"w_{c}_{new string('y', 60)} int NULL"));
        await using var h = await LiveAsync(Ddl + string.Concat(Enumerable.Range(1, 4).Select(i => $" CREATE TABLE dbo.u{i} (id int NOT NULL PRIMARY KEY, x int NULL, {wide});")), "t", "u1", "u2", "u3", "u4");
        var id = 100;
        for (var i = 0; i < 8; i++)
        {
            await h.DdlAsync($"ALTER TABLE dbo.t ADD {string.Join(", ", Enumerable.Range(0, 20).Select(c => $"col_{i}_{c}_{new string('x', 60)} int NULL"))};");
            await h.DdlAsync($"ALTER TABLE dbo.u{1 + i % 2} ADD {string.Join(", ", Enumerable.Range(0, 15).Select(c => $"col_{i}_{c}_{new string('z', 60)} int NULL"))};");
            foreach (var table in (string[])["t", "u1", "u2", "u3", "u4"])
                await h.DmlAsync("c", ++id, $"INSERT dbo.{table} (id, {(table == "t" ? "v" : "x")}) VALUES ({id}, {i});", table);
        }
        await h.DdlAsync("ALTER INDEX ALL ON dbo.t REBUILD; ALTER INDEX ALL ON dbo.u2 REBUILD;");
        await h.DmlAsync("c", ++id, $"INSERT dbo.u2 (id, x) VALUES ({id}, 1000);", "u2");
        await using (var conn = new SqlConnection(TestServer.ConnectionString(h.Db)))
        {
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("""
                SELECT COUNT(*) FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT' AND b.[Transaction Name] = 'SplitPage'
                WHERE l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.syscolpars.clst') AND l.Operation = 'LOP_INSERT_ROWS' AND l.Context = 'LCX_CLUSTERED'
                """, conn) { CommandTimeout = 120 };
            Assert.True((int)(await cmd.ExecuteScalarAsync())! > 0, "no catalog rows were moved: the test proves nothing");
        }
        await h.RestartAsync();
        await h.PumpAllAsync();
        AssertJournal(h);
    }

    [Fact]
    public async Task Two_tables_rebuilt_at_different_times()
    {
        await using var h = await LiveAsync(Ddl + " CREATE TABLE dbo.u (id int NOT NULL PRIMARY KEY, x varchar(10) NULL);", "t", "u");
        await h.DmlAsync("c", 1, "INSERT dbo.u VALUES (1, 'a');", "u");
        await h.DdlAsync("ALTER INDEX ALL ON dbo.t REBUILD;");
        await h.DmlAsync("u", 1, "UPDATE dbo.u SET x = 'b' WHERE id = 1;", "u");
        await h.DmlAsync("u", 1, "UPDATE dbo.t SET v = 5 WHERE id = 1;");
        await h.DdlAsync("ALTER INDEX ALL ON dbo.u REBUILD;");
        await h.DmlAsync("u", 1, "UPDATE dbo.t SET v = 6 WHERE id = 1;");
        await h.DmlAsync("u", 1, "UPDATE dbo.u SET x = 'c' WHERE id = 1;", "u");
        await h.RestartAsync();
        await h.PumpAllAsync();
        AssertJournal(h);
    }

    /// <summary>DDL and DML in one transaction: each record needs the layout at its own LSN, not at commit.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Ddl_and_dml_in_one_transaction(bool restart)
    {
        await using var h = await LiveAsync();
        await h.SqlAsync("""
            BEGIN TRAN;
            INSERT dbo.t (id, v, s) VALUES (50, 1, 'a');
            ALTER INDEX ALL ON dbo.t REBUILD;
            INSERT dbo.t (id, v, s) VALUES (51, 2, 'b');
            ALTER TABLE dbo.t ALTER COLUMN v decimal(19,2) NULL;
            INSERT dbo.t (id, v, s) VALUES (52, 3, 'c');
            ALTER TABLE dbo.t DROP COLUMN s;
            INSERT dbo.t (id, v) VALUES (53, 4);
            COMMIT;
            """);
        if (restart) await h.RestartAsync();
        await h.PumpAllAsync();
        var got = h.Events().Skip(3).Select(e => e.GetProperty("after").GetRawText()).ToArray();
        Assert.Equal(["""{"id":50,"v":1,"s":"a"}""", """{"id":51,"v":2,"s":"b"}""", """{"id":52,"v":"3.00","s":"c"}""", """{"id":53,"v":"4.00"}"""], got);
    }

    /// <summary>Dropping a column changes neither the rowset nor the row format: only the layout load before decoding tells.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Column_dropped_and_rows_changed_in_one_transaction(bool restart)
    {
        await using var h = await LiveAsync();
        await h.SqlAsync("BEGIN TRAN; INSERT dbo.t (id, v, s) VALUES (60, 1, 'a'); ALTER TABLE dbo.t DROP COLUMN s; EXEC('INSERT dbo.t (id, v) VALUES (61, 2);'); COMMIT;");
        await h.SqlAsync("INSERT dbo.t (id, v) VALUES (62, 3);");
        if (restart) await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal(["""{"id":60,"v":1,"s":"a"}""", """{"id":61,"v":2}""", """{"id":62,"v":3}"""], h.Events().Skip(3).Select(e => e.GetProperty("after").GetRawText()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Ddl_rolled_back_to_a_savepoint(bool restart)
    {
        await using var h = await LiveAsync();
        await h.SqlAsync("""
            BEGIN TRAN;
            INSERT dbo.t (id, v, s) VALUES (53, 1, 'd');
            SAVE TRAN sp;
            ALTER TABLE dbo.t ADD temp1 int NULL;
            INSERT dbo.t (id, v, s) VALUES (54, 1, 'undone');
            ALTER TABLE dbo.t ALTER COLUMN v bigint NULL;
            ROLLBACK TRAN sp;
            INSERT dbo.t (id, v, s) VALUES (55, 1, 'e');
            COMMIT;
            """);
        if (restart) await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Equal(["""{"id":53,"v":1,"s":"d"}""", """{"id":55,"v":1,"s":"e"}"""], h.Events().Skip(3).Select(e => e.GetProperty("after").GetRawText()));
        Assert.Empty(await h.DiffAsync("t"));
    }

    /// <summary>A transaction that is open across the catalog load, over a DDL on another table.</summary>
    [Fact]
    public async Task Open_transaction_across_ddl_and_restart()
    {
        await using var h = await LiveAsync(Ddl + " CREATE TABLE dbo.u (id int NOT NULL PRIMARY KEY, x varchar(10) NULL);", "t", "u");
        await using var conn = new SqlConnection(TestServer.ConnectionString(h.Db));
        await conn.OpenAsync();
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync();
        await using (var cmd = new SqlCommand("INSERT dbo.t (id, v, s) VALUES (60, 1, 'early');", conn, tx)) await cmd.ExecuteNonQueryAsync();
        await h.DmlAsync("c", 1, "INSERT dbo.u VALUES (1, 'a');", "u");
        await h.DdlAsync("ALTER INDEX ALL ON dbo.u REBUILD;");
        await h.DmlAsync("u", 1, "UPDATE dbo.u SET x = 'b' WHERE id = 1;", "u");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        await h.RestartAsync();
        await h.DdlAsync("ALTER TABLE dbo.u ALTER COLUMN x varchar(100) NULL; ALTER INDEX ALL ON dbo.u REBUILD;");
        await h.PumpAllAsync();
        await tx.CommitAsync();
        await h.DdlAsync("ALTER INDEX ALL ON dbo.t REBUILD;");
        await h.PumpAllAsync();
        Assert.Contains(h.Events(), e => e.GetProperty("after").ValueKind == System.Text.Json.JsonValueKind.Object && e.GetProperty("after").GetProperty("id").GetInt32() == 60);
        Assert.Empty(await h.DiffAsync("t"));
        Assert.Empty(await h.DiffAsync("u"));
    }

    /// <summary>The miner pumps, faults and restarts continuously while DDL and row changes commit as fast as they can: catalog reads race
    /// DDL commits all the time.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Ddl_storm(int seed)
    {
        await using var h = await LiveAsync();
        var rng = new Random(seed);
        using var stop = new CancellationTokenSource();
        var faults = new List<string>();
        var pump = Task.Run(async () =>
        {
            for (var i = 0; !stop.IsCancellationRequested; i++)
            {
                try
                {
                    if (i % 50 == 49) h.Capture.SetFault(new IOException("restart")); // rewind: reload everything from the checkpoint
                    await h.PumpOnceAsync();
                    if (i % 10 == 9) await h.CheckpointAsync();
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { faults.Add(ex.GetType().Name + ": " + ex.Message); h.Capture.SetFault(ex); }
            }
        });
        var type = 0;
        string[] types = ["int", "bigint", "decimal(19,2)", "decimal(25,3)"];
        for (var i = 0; i < 70; i++)
        {
            var id = 1000 + i;
            switch (rng.Next(5))
            {
                case 0: await h.DdlAsync("ALTER INDEX ALL ON dbo.t REBUILD;"); break;
                case 1: await h.DdlAsync($"ALTER TABLE dbo.t ADD x{i} int NOT NULL DEFAULT {i};"); break;
                case 2 when type < types.Length - 1: await h.DdlAsync($"ALTER TABLE dbo.t ALTER COLUMN v {types[++type]} NULL;"); break;
            }
            await h.DmlAsync("c", id, $"INSERT dbo.t (id, v, s) VALUES ({id}, {i}, 'storm');");
            await h.DmlAsync("u", 1, $"UPDATE dbo.t SET v = {i} WHERE id = 1;");
        }
        await stop.CancelAsync();
        await pump;
        await h.PumpAllAsync();
        var diffs = h.JournalDiff();
        Assert.True(diffs.Count == 0, string.Join("; ", diffs) + " | faults: " + string.Join(" | ", faults.Distinct().Take(5)));
        Assert.DoesNotContain(faults, f => f.StartsWith("UndecodableChange", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Sink_outage_across_a_rebuild()
    {
        await using var h = await LiveAsync();
        await Dml1(h);
        // the sink is down: the pump fails and rewinds (which also invalidates the cached layouts)
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => h.PumpOnceAsync(new DeadSink()));
        h.Capture.SetFault(ex);
        await Rebuild(h);
        await Dml2(h);
        ex = await Assert.ThrowsAnyAsync<Exception>(() => h.PumpOnceAsync(new DeadSink()));
        h.Capture.SetFault(ex);
        await h.DdlAsync("ALTER TABLE dbo.t ALTER COLUMN v bigint NULL;");
        await Dml3(h);
        await h.PumpAllAsync();
        AssertJournal(h);
    }

    [Theory]
    [InlineData(false)] // clustered primary key
    [InlineData(true)]  // rebuilt before the drop
    public async Task Table_dropped_while_down_still_delivers_its_changes(bool rebuilt)
    {
        await using var h = await LiveAsync();
        await Dml1(h);
        if (rebuilt) { await Rebuild(h); await Dml2(h); }
        await h.DdlAsync("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N't', @capture_instance = N'all'; DROP TABLE dbo.t;");
        await h.RestartAsync();
        await h.PumpAllAsync();
        AssertJournal(h);
        Assert.Equal("""{"id":6}""", h.Events().First(e => e.GetProperty("after").ValueKind == System.Text.Json.JsonValueKind.Object && e.GetProperty("after").GetProperty("id").GetInt32() == 6).GetProperty("key").GetRawText());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cdc_disabled_while_down_still_delivers_the_changes_made_before(bool rebuilt)
    {
        await using var h = await LiveAsync();
        await Dml1(h);
        if (rebuilt) { await Rebuild(h); await Dml2(h); }
        await h.DdlAsync("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N't', @capture_instance = N'all';");
        if (rebuilt) await Rebuild(h);
        await h.SqlAsync("INSERT dbo.t (id, v, s) VALUES (99, 1, 'not captured');");
        await h.RestartAsync();
        await h.PumpAllAsync();
        AssertJournal(h);
    }

    [Fact]
    public async Task Writers_during_an_online_rebuild()
    {
        await using var h = await CaptureHarness.CreateAsync(Ddl, ["t"]);
        await h.SqlAsync("INSERT dbo.t (id, v, s) SELECT value, value, REPLICATE('x', 30) FROM GENERATE_SERIES(1, 60000);");
        await h.PumpAllAsync();
        await h.CheckpointAsync(release: true);
        using var stop = new CancellationTokenSource();
        var rounds = 0;
        var writer = Task.Run(async () =>
        {
            await using var conn = new SqlConnection(TestServer.ConnectionString(h.Db));
            await conn.OpenAsync();
            var n = 0;
            while (!stop.IsCancellationRequested)
            {
                await using var cmd = new SqlCommand("""
                    UPDATE dbo.t SET v = v + 1 WHERE id = @k; DELETE dbo.t WHERE id = @k + 30000; INSERT dbo.t VALUES (100000 + @n, @n, 'new');
                    BEGIN TRAN; INSERT dbo.t VALUES (200000 + @n, @n, 'keep'); SAVE TRAN sp; UPDATE dbo.t SET v = -1 WHERE id = 2 * @k; ROLLBACK TRAN sp; COMMIT;
                    """, conn);
                cmd.Parameters.AddWithValue("@k", 1 + n * 37 % 29000);
                cmd.Parameters.AddWithValue("@n", n++);
                await cmd.ExecuteNonQueryAsync();
                Volatile.Write(ref rounds, n);
            }
        });
        for (var i = 0; i < 3 || (Volatile.Read(ref rounds) < 40 && i < 60); i++) { await h.DdlAsync("ALTER INDEX ALL ON dbo.t REBUILD WITH (ONLINE = ON, MAXDOP = 1);"); if (i == 0) await h.PumpAllAsync(); }
        await stop.CancelAsync();
        await writer;
        Assert.True(rounds >= 40, $"only {rounds} rounds of writes overlapped the rebuilds");
        await h.RestartAsync();
        await h.PumpAllAsync();
        Assert.Empty((await h.DiffAsync("t")).Take(5));
    }

    /// <summary>A table the decoder does not support must stop its stream loudly, also when it became unsupported while the miner was down -
    /// but changes written before that are still delivered.</summary>
    [Fact]
    public async Task Compression_enabled_while_down()
    {
        await using var h = await LiveAsync();
        await Dml1(h);
        await h.DdlAsync("ALTER TABLE dbo.t REBUILD WITH (DATA_COMPRESSION = PAGE);");
        await h.RestartAsync();
        await h.PumpAllAsync();
        AssertJournal(h);
        await h.SqlAsync("INSERT dbo.t (id, v, s) VALUES (70, 1, 'compressed');");
        await Assert.ThrowsAsync<NotSupportedException>(h.PumpAllAsync);
    }

    [Fact]
    public async Task Heap_with_forwarded_rows()
    {
        await using var h = await CaptureHarness.CreateAsync("CREATE TABLE dbo.t (id int NOT NULL, v int NULL, s varchar(4000) NULL);", ["t"]);
        await h.SqlAsync("INSERT dbo.t (id, v, s) SELECT value, value, REPLICATE('x', 100) FROM GENERATE_SERIES(1, 60);");
        await h.SqlAsync("UPDATE dbo.t SET s = REPLICATE('y', 3000) WHERE id % 7 = 0;"); // forwards
        await h.SqlAsync("UPDATE dbo.t SET v = v + 1 WHERE id % 7 = 0;");               // updates forwarded rows in place
        await h.SqlAsync("UPDATE dbo.t SET s = REPLICATE('z', 3900) WHERE id = 14;");   // moves a forwarded row again
        await h.SqlAsync("UPDATE dbo.t SET s = 'small' WHERE id = 21;");                // shrinks (may move back)
        await h.SqlAsync("DELETE dbo.t WHERE id IN (28, 1);");                          // deletes a forwarded and a normal row
        await h.SqlAsync("ALTER TABLE dbo.t REBUILD; UPDATE dbo.t SET v = -1 WHERE id = 35;");
        await h.PumpAllAsync();
        var diffs = await h.DiffAsync("t");
        Assert.True(diffs.Count == 0, string.Join("; ", diffs.Take(8)));
    }

    [Theory]
    [InlineData("UPDATE dbo.t SET doc = REPLICATE(CONVERT(nvarchar(max), N'q'), 15000) WHERE id = 1")]
    [InlineData("UPDATE dbo.t SET doc = NULL WHERE id = 1")]
    [InlineData("UPDATE dbo.t SET doc.WRITE(N'zzzz', 10, 4) WHERE id = 1")]
    [InlineData("UPDATE dbo.t SET doc.WRITE(REPLICATE(CONVERT(nvarchar(max), N'w'), 9000), NULL, 0) WHERE id = 1")]
    [InlineData("UPDATE dbo.t SET v = v + 1 WHERE id = 1")]
    [InlineData("UPDATE dbo.t SET id = 100 WHERE id = 1")]
    [InlineData("DELETE dbo.t WHERE id = 1")]
    public async Task Savepoint_rollback_of_lob_work_keeps_earlier_changes(string undone)
    {
        await using var h = await CaptureHarness.CreateAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL, doc nvarchar(max) NULL);", ["t"]);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 1, REPLICATE(CONVERT(nvarchar(max), N'a'), 20000)), (2, 2, N'small');");
        await h.PumpAllAsync();
        await h.SqlAsync($"BEGIN TRAN; INSERT dbo.t VALUES (3, 3, N'keep'); UPDATE dbo.t SET v = 22 WHERE id = 2; SAVE TRAN sp; {undone}; ROLLBACK TRAN sp; INSERT dbo.t VALUES (4, 4, N'after'); COMMIT;");
        await h.PumpAllAsync();
        var diffs = (await h.DiffAsync("t")).Where(d => !d.Contains(".doc:", StringComparison.Ordinal)).ToList(); // off-row LOB values are "unavailable" by design
        Assert.True(diffs.Count == 0, string.Join("; ", diffs));
    }

    private sealed class DeadSink : ISink
    {
        public string Name => "dead";
        public SinkHealth Health => new(Name, false);
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct) => throw new IOException("sink is down");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
