using System.Diagnostics;
using Hegel;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// The log is read by database id, the catalog and sp_repldone go by name, and ids and names move between databases: renames and swaps,
/// drop and create, detach and attach, restores. Databases made from one backup have the same LSNs and rowset ids, so nothing in the log
/// itself would give a mix-up away. The list of databases is only refreshed every minute (here: never); what happens in between must still
/// be published under the right name, completely.
/// </summary>
#pragma warning disable CA1001 // disposed in IAsyncLifetime.DisposeAsync
public sealed class DatabaseIdentityTests : IAsyncLifetime
#pragma warning restore CA1001
{
    private readonly string _tag = Guid.NewGuid().ToString("N")[..8];
    private string _template = "";
    private readonly List<string> _databases = [];
    private readonly MemorySink _sink = new();
    private readonly MemoryCheckpointStore _store = new();
    private readonly List<CaptureGap> _gaps = [];
    private CaptureEngine? _engine;
    private string _started = "";

    private string Name(string s) => $"lm_it_{_tag}_{s}";
    private string BackupFile => $"lm_it_{_tag}.bak";

    public async Task InitializeAsync()
    {
        _started = await ScalarAsync("SELECT CONVERT(varchar(23), SYSDATETIME(), 121)");
        _template = await TestServer.CreateDatabaseAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, physical varchar(20) NOT NULL);", "t");
        await TestServer.ExecAsync($"BACKUP DATABASE [{_template}] TO DISK = N'{BackupFile}' WITH INIT, COPY_ONLY;");
        foreach (var d in new[] { "a", "b", "c" }) await RestoreAsync(d);
    }

    private async Task RestoreAsync(string d, bool over = false, string? files = null)
    {
        var path = await ScalarAsync("SELECT CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath'))") + Name(files ?? d);
        var (data, log) = (path + ".mdf", path + ".ldf");
        if (over) // into its own files, whatever they are called
        {
            data = await ScalarAsync($"SELECT physical_name FROM sys.master_files WHERE database_id = DB_ID(N'{Name(d)}') AND type = 0");
            log = await ScalarAsync($"SELECT physical_name FROM sys.master_files WHERE database_id = DB_ID(N'{Name(d)}') AND type = 1");
            await TestServer.ExecAsync($"ALTER DATABASE [{Name(d)}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;");
        }
        await TestServer.ExecAsync($"RESTORE DATABASE [{Name(d)}] FROM DISK = N'{BackupFile}' WITH REPLACE, KEEP_CDC, MOVE N'{_template}' TO N'{data}', MOVE N'{_template}_log' TO N'{log}'; ALTER DATABASE [{Name(d)}] SET MULTI_USER;");
        if (!_databases.Contains(Name(d))) _databases.Add(Name(d));
    }

    private static async Task<string> ScalarAsync(string sql)
    {
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        return Convert.ToString(await cmd.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private Task StartAsync(params string[] only) => StartAsync(StartPosition.Auto, only);

    private async Task StartAsync(StartPosition start, params string[] only)
    {
        _engine = new CaptureEngine(new CaptureOptions
        {
            ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n.StartsWith(Name(""), StringComparison.Ordinal) && (only.Length == 0 || only.Any(o => n == Name(o))),
            Workers = 2, CheckpointInterval = TimeSpan.FromMilliseconds(50), ReleaseInterval = TimeSpan.Zero, Settings = new CaptureSettings { StartPosition = start },
            SweepInterval = TimeSpan.FromSeconds(1), HoldCheckInterval = TimeSpan.FromHours(1), DatabaseRefreshInterval = TimeSpan.FromHours(1), Gaps = new Recorder(_gaps),
        }, new DmvPollSignal(TestServer.ConnectionString(), TimeSpan.FromMilliseconds(5)), new(new EncoderOptions { ServerName = "it" }), _sink, _store);
        await _engine.StartAsync(CancellationToken.None);
        for (var i = 0; i < 500 && _engine.Captures.Any(c => !c.Live || c.HoldingForBaseline); i++) await Task.Delay(10);
    }

    private sealed class Recorder(List<CaptureGap> gaps) : IGapLog
    {
        public Task RecordAsync(CaptureGap gap, bool resync, CancellationToken ct) { lock (gaps) gaps.Add(gap); return Task.CompletedTask; }
    }

    /// <summary>A row whose content says which database it was written to (by name, at that time).</summary>
    private Task InsertAsync(string d, int id) => TestServer.ExecAsync($"INSERT dbo.t VALUES ({id}, '{d}')", Name(d));

    private List<(string Db, int Id, string Physical)> Published() => _sink.Snapshot().DistinctBy(m => m.MessageId).Select(m =>
    {
        using var doc = JsonDocument.Parse(m.Payload);
        var e = doc.RootElement;
        var row = e.GetProperty("after");
        return (e.GetProperty("source").GetProperty("db").GetString()!, row.GetProperty("id").GetInt32(), row.GetProperty("physical").GetString()!);
    }).ToList();

    /// <summary>Every row arrives, and only ever under the name of the database it was written to.</summary>
    private async Task ArrivedAsync(params (string Db, int Id)[] expected)
    {
        var sw = Stopwatch.StartNew();
        while (expected.Any(x => !Published().Any(p => p.Db == Name(x.Db) && p.Id == x.Id)))
        {
            var missing = expected.Where(x => !Published().Any(p => p.Db == Name(x.Db) && p.Id == x.Id)).ToList();
            Assert.True(sw.ElapsedMilliseconds < 15_000, $"missing: {string.Join(", ", missing)}; got {string.Join(", ", Published())}; faults: {string.Join("; ", _engine!.Captures.Select(c => c.Fault))}; not opened: {string.Join(", ", _engine.NotOpened)}; not capturable: {string.Join(", ", _engine.NotCapturable)}; engine: {_engine.Failure.Exception}");
            await Task.Delay(20);
        }
        foreach (var (db, id, physical) in Published().Where(p => expected.Any(x => x.Id == p.Id)))
            Assert.True(db == Name(physical), $"row {id} of {Name(physical)} was published as {db}'s");
    }

    private async Task RenameAsync(string from, string to)
    {
        await TestServer.ExecAsync($"ALTER DATABASE [{Name(from)}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [{Name(from)}] MODIFY NAME = [{Name(to)}]; ALTER DATABASE [{Name(to)}] SET MULTI_USER;");
        _databases.Add(Name(to));
    }

    [Fact]
    public async Task Swapped_names()
    {
        await StartAsync("a", "b");
        await InsertAsync("a", 1);
        await InsertAsync("b", 2);
        await ArrivedAsync(("a", 1), ("b", 2));
        await RenameAsync("a", "x");
        await RenameAsync("b", "a");
        await RenameAsync("x", "b");
        await InsertAsync("a", 3);
        await InsertAsync("b", 4);
        await ArrivedAsync(("a", 3), ("b", 4));
        Assert.Contains(_gaps, g => g.Database == Name("a"));
        Assert.Contains(_gaps, g => g.Database == Name("b"));
    }

    [Fact]
    public async Task A_database_id_that_is_taken_by_another_database()
    {
        await StartAsync("a", "c");
        await InsertAsync("a", 1);
        await InsertAsync("c", 2);
        await ArrivedAsync(("a", 1), ("c", 2));
        var id = await ScalarAsync($"SELECT DB_ID('{Name("a")}')");
        var path = await ScalarAsync("SELECT CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath'))");
        // c takes a's place, all in one go
        await TestServer.ExecAsync($"""
            ALTER DATABASE [{Name("a")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [{Name("c")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [{Name("a")}];
            EXEC sp_detach_db N'{Name("c")}', @skipchecks = 'true';
            CREATE DATABASE [{Name("c")}] ON (FILENAME = N'{path}{Name("c")}.mdf'), (FILENAME = N'{path}{Name("c")}.ldf') FOR ATTACH;
            """);
        if (await ScalarAsync($"SELECT DB_ID('{Name("c")}')") != id) return; // another test's database got the id first
        await InsertAsync("c", 3);
        await ArrivedAsync(("c", 3));
    }

    [Fact]
    public async Task A_database_that_was_restored_over_itself()
    {
        await StartAsync("a");
        for (var i = 1; i <= 60; i++) await InsertAsync("a", i);
        await ArrivedAsync(Enumerable.Range(1, 60).Select(i => ("a", i)).ToArray());
        await RestoreAsync("a", over: true);
        for (var i = 101; i <= 160; i++) await InsertAsync("a", i); // the log grows through the same LSNs again
        await ArrivedAsync(Enumerable.Range(101, 60).Select(i => ("a", i)).ToArray());
        Assert.Contains(_gaps, g => g.Database == Name("a"));
    }

    /// <summary>What SQL Server still holds (nothing was released since CDC was enabled): skipped when that is asked for, otherwise the beginning.</summary>
    [Theory]
    [InlineData(StartPosition.Latest, false)]
    [InlineData(StartPosition.Auto, true)]
    [InlineData(StartPosition.Earliest, true)]
    public async Task A_start_without_a_position(StartPosition start, bool history)
    {
        for (var i = 1; i <= 3; i++) await InsertAsync("a", i);
        await StartAsync(start, "a");
        await InsertAsync("a", 10);
        await ArrivedAsync(("a", 10));
        await Task.Delay(300);
        Assert.Equal(history ? [1, 2, 3, 10] : [10], Published().Select(p => p.Id).Order());
    }

    private static async Task<DatabaseRef> RefAsync(string name)
    {
        var (id, incarnation) = await TestServer.IdentityAsync(name);
        return new DatabaseRef(id, name, incarnation);
    }

    /// <summary>Everything the miner does to a database.</summary>
    private static IEnumerable<(string What, Func<Task> Do)> Accesses(SqlConnection conn, DatabaseRef database, LogMiner.Schema.TableLayout table)
    {
        yield return ("log read", async () => { await foreach (var _ in new EngineLogReader(conn).ReadAsync(database, null, 100, CaptureScope.CdcTrackedTables, CancellationToken.None)) { } });
        yield return ("log read without CDC", async () => { await foreach (var _ in new EngineLogReader(conn).ReadAsync(database, null, 100, CaptureScope.AllUserTables, CancellationToken.None)) { } });
        yield return ("catalog changes", () => CatalogLogReader.ReadAsync(conn, database, null, [], CancellationToken.None));
        yield return ("layouts", () => SchemaLoader.LoadAsync(conn, database, CaptureScope.CdcTrackedTables, null, new HashSet<long>(), CancellationToken.None));
        yield return ("oldest unreleased", () => ReplDoneClient.OldestUnreleasedAsync(conn, database, CancellationToken.None));
        yield return ("table state", () => ReplDoneClient.TableStateAsync(conn, database, CancellationToken.None));
        yield return ("release", () => ReplDoneClient.MarkDistributedAsync(conn, database, new Lsn(1, 1, 1), new Lsn(1, 1, 2), CancellationToken.None));
        yield return ("recovery fork", () => DatabaseIdentity.RecoveryForkAsync(conn, database.Id, database.Name, CancellationToken.None));
        yield return ("identity", () => DatabaseIdentity.VerifyAsync(conn, database, CancellationToken.None));
        yield return ("historical load", () => new SqlSnapshotSource(TestServer.ConnectionString()).ReadChunkAsync(table, null, 10, CancellationToken.None));
    }

    private async Task AllRefusedAsync(string state, string back, int error)
    {
        var database = await RefAsync(Name("a"));
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        var table = (await SchemaLoader.LoadAsync(conn, database, CaptureScope.CdcTrackedTables, null, new HashSet<long>(), CancellationToken.None)).Tables.Single();
        await TestServer.ExecAsync(state);
        try
        {
            foreach (var (what, access) in Accesses(conn, database, table))
            {
                var ex = await Assert.ThrowsAsync<SqlException>(access);
                Assert.True(ex.Number == error, $"{what}: {ex.Number} {ex.Message}");
                Assert.Equal(System.Data.ConnectionState.Open, conn.State); // other databases are read on it, too
            }
        }
        finally { await TestServer.ExecAsync(back); }
        foreach (var (what, access) in Accesses(conn, await RefAsync(Name("a")), table).Where(a => a.What is not "release")) await access();
    }

    /// <summary>Its one connection belongs to whoever is restoring, renaming or repairing it - also while that connection is not open.</summary>
    [Fact]
    public Task A_single_user_database_is_left_alone() =>
        AllRefusedAsync($"ALTER DATABASE [{Name("a")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", $"ALTER DATABASE [{Name("a")}] SET MULTI_USER;", DatabaseIdentity.BusyError);

    /// <summary>927 is what SQL Server answers at the door. Anything else would mean that the database is looked at from outside, which at the wrong
    /// moment hangs the instance (experiments/36).</summary>
    [Fact]
    public Task A_database_that_is_being_restored_is_entered_first() =>
        AllRefusedAsync($"ALTER DATABASE [{Name("a")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [{Name("a")}] SET MULTI_USER; BACKUP LOG [{Name("a")}] TO DISK = 'NUL' WITH NORECOVERY;",
            $"RESTORE DATABASE [{Name("a")}] WITH RECOVERY;", 927);

    [Fact]
    public async Task Restores_are_not_refused_while_the_miner_reads()
    {
        await StartAsync("a");
        for (var i = 1; i <= 12; i++)
        {
            for (var j = 1; j <= 5; j++) await InsertAsync("a", j);
            await RestoreAsync("a", over: true); // raises an error when it is refused
        }
        await InsertAsync("a", 1000);
        await ArrivedAsync(("a", 1000));
    }

    /// <summary>One database nobody can enter must not keep the others from being captured.</summary>
    [Fact]
    public async Task A_database_that_cannot_be_opened_does_not_stop_the_others()
    {
        await using var blocker = new SqlConnection(TestServer.ConnectionString() + ";Pooling=false");
        await blocker.OpenAsync();
        await using (var cmd = new SqlCommand($"ALTER DATABASE [{Name("b")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; USE [{Name("b")}];", blocker)) await cmd.ExecuteNonQueryAsync();
        await StartAsync("a", "b", "c");
        await InsertAsync("a", 1);
        await InsertAsync("c", 2);
        await ArrivedAsync(("a", 1), ("c", 2));
        await using (var cmd = new SqlCommand($"INSERT dbo.t VALUES (3, 'b'); ALTER DATABASE [{Name("b")}] SET MULTI_USER; USE master;", blocker)) await cmd.ExecuteNonQueryAsync();
        await InsertAsync("b", 4);
        await ArrivedAsync(("b", 3), ("b", 4));
    }

    /// <summary>A database as a thing: whatever its name, id and files are called at the moment.</summary>
    private sealed class Physical(string token)
    {
        public string Token { get; } = token;
        public string? Name; // null: gone
        /// <summary>Every name it had from each row's commit on.</summary>
        public readonly Dictionary<int, HashSet<string>> Rows = [];
        public void Named(string name)
        {
            Name = name;
            foreach (var names in Rows.Values) names.Add(name);
        }
    }

    [Fact]
    public void Whatever_happens_to_names_and_ids_a_row_is_published_under_a_name_its_database_had_and_none_is_missing() =>
        Property.Test(tc => LogMiner.Tests.Blocking.Run(() => RunAsync(tc)), new Settings { TestCases = TestServer.Cases(8), SuppressHealthCheck = HealthCheck.TooSlow });

    private int _nextRow = 1000;
    private int _nextToken;

    private async Task RunAsync(TestCase tc)
    {
        if (_engine is not null) await _engine.DisposeAsync();
        _sink.Clear();
        var names = new[] { "a", "b", "c" };
        var live = new List<Physical>();
        async Task FreshAsync(string name)
        {
            await TestServer.DropDatabaseAsync(Name(name));
            await RestoreAsync(name, files: $"{name}_{++_nextToken}"); // the file names of a renamed database are still in use
            var p = new Physical($"p{_nextToken}");
            p.Named(name);
            live.Add(p);
        }
        foreach (var n in names.Append("x")) await TestServer.DropDatabaseAsync(Name(n));
        foreach (var n in names) await FreshAsync(n);
        await StartAsync(names);
        var owner = new Dictionary<int, Physical>();

        async Task InsertIntoAsync(Physical p)
        {
            var id = ++_nextRow;
            await TestServer.ExecAsync($"INSERT dbo.t VALUES ({id}, '{p.Token}')", Name(p.Name!));
            p.Rows[id] = [p.Name!];
            owner[id] = p;
        }

        void CheckNames()
        {
            foreach (var (db, id, token) in Published().Where(e => e.Id >= 1000 && owner.ContainsKey(e.Id)))
            {
                var p = owner[id];
                Assert.Equal(p.Token, token);
                Assert.True(p.Rows.TryGetValue(id, out var had) && had.Any(n => Name(n) == db),
                    $"row {id} of {p.Token} (named {string.Join(", ", p.Rows.GetValueOrDefault(id) ?? [])} since it was written) was published as {db}'s");
            }
        }

        async Task ArriveAsync()
        {
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var got = Published().Select(e => e.Id).ToHashSet();
                var missing = live.Where(p => p.Name is not null).SelectMany(p => p.Rows.Keys).Where(id => !got.Contains(id)).ToList();
                if (missing.Count == 0) return;
                Assert.True(sw.ElapsedMilliseconds < 20_000, $"missing: {string.Join(", ", missing.Select(id => $"{id} of {owner[id].Token} ({owner[id].Name})"))}; " +
                    $"captures: {string.Join("; ", _engine!.Captures.Select(c => $"{c.Name} {c.State} {c.Fault}"))}; not opened: {string.Join(", ", _engine.NotOpened)}");
                await Task.Delay(20);
            }
        }

        Physical Pick(string what) => tc.Draw(Generators.SampledFrom(live.Where(p => p.Name is not null).ToArray()), what);

        for (var steps = tc.Draw(Generators.Integers().Min(2).Max(7), "steps"); steps > 0; steps--)
        {
            switch (tc.Draw(Generators.SampledFrom(new[] { "insert", "insert", "swap", "restore", "recreate", "offline", "reattach", "arrive" }), "what"))
            {
                case "insert":
                    for (var n = tc.Draw(Generators.Integers().Min(1).Max(3), "rows"); n > 0; n--) await InsertIntoAsync(Pick("into"));
                    break;
                case "swap":
                {
                    var (p, q) = (Pick("swap"), Pick("with"));
                    if (p == q) break;
                    var (pn, qn) = (p.Name!, q.Name!);
                    await RenameAsync(pn, "x"); await RenameAsync(qn, pn); await RenameAsync("x", qn);
                    p.Named(qn); q.Named(pn);
                    break;
                }
                case "restore": // the log of everything since the backup is gone, and with it what was not read yet
                {
                    var p = Pick("restore");
                    await RestoreAsync(p.Name!, over: true);
                    var n = p.Name!;
                    live.Remove(p); p.Name = null;
                    var q = new Physical($"p{++_nextToken}");
                    q.Named(n);
                    live.Add(q);
                    break;
                }
                case "recreate":
                {
                    var p = Pick("recreate");
                    var n = p.Name!;
                    live.Remove(p); p.Name = null;
                    await FreshAsync(n);
                    break;
                }
                case "offline":
                {
                    var n = Name(Pick("offline").Name!);
                    await TestServer.ExecAsync($"ALTER DATABASE [{n}] SET OFFLINE WITH ROLLBACK IMMEDIATE;");
                    if (tc.Draw(Generators.Booleans(), "for a while")) await Task.Delay(300);
                    await TestServer.ExecAsync($"ALTER DATABASE [{n}] SET ONLINE;");
                    break;
                }
                case "reattach": // possibly two at once: they come back with each other's id
                {
                    var ps = live.Where(p => p.Name is not null && tc.Draw(Generators.Booleans(), "detach")).ToList();
                    if (tc.Draw(Generators.Booleans(), "in reverse")) ps.Reverse();
                    var attach = new List<string>();
                    foreach (var p in ps)
                    {
                        var n = Name(p.Name!);
                        var files = await ScalarAsync($"SELECT STRING_AGG(N'(FILENAME = N''' + physical_name + N''')', N', ') WITHIN GROUP (ORDER BY file_id) FROM sys.master_files WHERE database_id = DB_ID(N'{n}')");
                        attach.Add($"CREATE DATABASE [{n}] ON {files} FOR ATTACH;");
                        await TestServer.ExecAsync($"ALTER DATABASE [{n}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; EXEC sp_detach_db N'{n}', @skipchecks = 'true';");
                    }
                    if (tc.Draw(Generators.Booleans(), "attach in reverse")) attach.Reverse();
                    foreach (var a in attach) await TestServer.RetryOnDeadlockAsync(() => TestServer.ExecAsync(a));
                    break;
                }
                case "arrive": await ArriveAsync(); break;
            }
            CheckNames();
        }
        foreach (var p in live.Where(p => p.Name is not null).ToList()) await InsertIntoAsync(p);
        await ArriveAsync();
        CheckNames();
    }

    public async Task DisposeAsync()
    {
        if (_engine is not null) await _engine.DisposeAsync();
        foreach (var d in _databases.Append(_template)) await TestServer.DropDatabaseAsync(d);
        Assert.Equal("0", await ScalarAsync($"CREATE TABLE #l (d datetime, p nvarchar(60), t nvarchar(max)); INSERT #l EXEC sys.xp_readerrorlog 0, 1, N'Assertion'; SELECT COUNT(*) FROM #l WHERE d >= '{_started}'"));
        await TestServer.ExecAsync($"DECLARE @p nvarchar(500) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultBackupPath')) + N'\\{BackupFile}'; EXEC master.sys.xp_delete_files @p;");
    }
}
