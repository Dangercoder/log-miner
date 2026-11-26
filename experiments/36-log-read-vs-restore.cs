#:property RunAnalyzers=false
#:property ManagePackageVersionsCentrally=false
#:property PublishAot=false
#:package Microsoft.Data.SqlClient@7.1.2
#:project ../src/LogMiner.SqlServer/LogMiner.SqlServer.csproj
// Can reading a database's log while it is being restored (or taken offline, detached) hang the instance, and which way of reading can?
// ONLY for a throwaway instance: experiments/35 hung one.
//   dotnet run 36-log-read-vs-restore.cs -- --server tcp:127.0.0.1,14333 --secret <file with sa's password> [--seconds 60] [--readers 4] [--pace <ms between reads>] [--db lm_su] [--op restore|plain|offline|reattach] [--only names] [--diagnose true]
// After every variant the instance is checked (new connection, sys.databases, 10 s). A hang ends the run with exit code 3.
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using LogMiner.Capture;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;

string Opt(string n, string d) { var i = Array.IndexOf(args, n); return i >= 0 ? args[i + 1] : d; }
var server = Opt("--server", "tcp:127.0.0.1,14333");
if (!server.Contains("14333") && !server.Contains("14334")) { Console.WriteLine("this can hang an instance: only the lab's ports 14333/14334"); return 2; }
var seconds = int.Parse(Opt("--seconds", "60"));
var readers = int.Parse(Opt("--readers", "4"));
var only = Opt("--only", "");
var op = Opt("--op", "restore");
var diagnose = Opt("--diagnose", "") == "true";
var cs = $"Server={server};User ID=sa;Password={File.ReadAllText(Opt("--secret", "")).Trim()};TrustServerCertificate=true;Pooling=false;Connect Timeout=10;Application Name=lab36";
var Db = Opt("--db", "lm_su");
var pace = int.Parse(Opt("--pace", "0"));
var gap = int.Parse(Opt("--gap", "0")); // up to this long between two restores: time for a whole cycle of reads
var back = int.Parse(Opt("--backoff", "250")); // after an error, as the miner does

async Task ExecAsync(string sql, string db = "master", int timeout = 60)
{
    await using var c = new SqlConnection(cs + ";Database=" + db);
    await c.OpenAsync();
    await using var k = new SqlCommand(sql, c) { CommandTimeout = timeout };
    await k.ExecuteNonQueryAsync();
}
async Task<string?> ScalarAsync(string sql, int timeout = 10)
{
    await using var c = new SqlConnection(cs + ";Database=master");
    await c.OpenAsync();
    await using var k = new SqlCommand(sql, c) { CommandTimeout = timeout };
    return (await k.ExecuteScalarAsync())?.ToString();
}
async Task<string?> AssertionsAsync()
{
    try { return await ScalarAsync("CREATE TABLE #l (d datetime, p nvarchar(60), t nvarchar(max)); INSERT #l EXEC sys.xp_readerrorlog 0, 1, N'Assertion'; SELECT CONVERT(varchar(10), COUNT(*)) + ISNULL(' ' + MAX(LEFT(t, 260)), '') FROM #l WHERE t LIKE '%Failed Assertion%'", 20); }
    catch (Exception ex) { return "errorlog not readable: " + ex.Message; }
}

const string Full = "SELECT @n = COUNT(*) FROM sys.fn_full_dblog(NULL, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);";
const string MinerMaster = """
    DECLARE @min nvarchar(46), @end nvarchar(46), @n int;
    IF NOT EXISTS (SELECT 1 FROM sys.database_recovery_status WHERE database_id = @d) RETURN;
    SELECT @min = log_min_lsn, @end = log_end_lsn FROM sys.dm_db_log_stats(@d);
    IF @min IS NULL RETURN;
    SELECT @n = COUNT(*) FROM sys.fn_full_dblog('0x' + @min, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) WHERE [Current LSN] < @end OPTION (MAXDOP 1);
    IF NOT EXISTS (SELECT 1 FROM sys.database_recovery_status WHERE database_id = @d) RETURN;
    """;
const string MinerInside = """
    DECLARE @min nvarchar(46), @end nvarchar(46), @n int, @d int = DB_ID();
    SELECT @min = log_min_lsn, @end = log_end_lsn FROM sys.dm_db_log_stats(@d);
    SELECT @n = COUNT(*) FROM sys.fn_full_dblog('0x' + @min, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) WHERE [Current LSN] < @end OPTION (MAXDOP 1);
    """;
// what never takes a lock on the database (measured): stay out unless it is plainly open
var Guard = $"IF DATABASEPROPERTYEX(N'{Db}', 'UserAccess') IS NULL OR DATABASEPROPERTYEX(N'{Db}', 'UserAccess') <> 'MULTI_USER' OR DATABASEPROPERTYEX(N'{Db}', 'Status') <> 'ONLINE' RETURN; ";
// the database stays open (its shared lock held) by the outer procedure call, while the read itself runs in master with one plan for all databases
var Held = $"EXEC [{Db}].sys.sp_executesql N'EXEC master.sys.sp_executesql @q, N''@d int'', @d;', N'@q nvarchar(max), @d int', N'{MinerMaster.Replace("IF NOT EXISTS (SELECT 1 FROM sys.database_recovery_status WHERE database_id = @d) RETURN;", "").Replace("'", "''")}', @d;";
var GuardDb = $"IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{Db}' AND user_access = 1) RETURN; "; // waits for the metadata lock: starts the read just when the database appears
// the procedure's name in a variable: the database is opened when the statement runs, not when the batch is compiled (before any guard)
string ByName(string sql) => $"DECLARE @p nvarchar(300) = QUOTENAME(DB_NAME(@d)) + N'.sys.sp_executesql'; " + sql.Replace($"EXEC [{Db}].sys.sp_executesql", "EXEC @p");
string Inside(string sql) => $"EXEC [{Db}].sys.sp_executesql N'{sql.Replace("'", "''")}';";
var variants = new (string Name, string? Sql)[]
{
    ("nothing", null),
    ("log_stats", "DECLARE @x nvarchar(46); SELECT @x = log_end_lsn FROM sys.dm_db_log_stats(@d);"),
    ("recovery_status", "DECLARE @x uniqueidentifier; SELECT @x = recovery_fork_guid FROM sys.database_recovery_status WHERE database_id = @d;"),
    ("sys.databases", "DECLARE @x int; SELECT @x = user_access FROM sys.databases WHERE database_id = @d;"),
    ("property", "DECLARE @x sql_variant = DATABASEPROPERTYEX(DB_NAME(@d), 'UserAccess');"),
    ("inside", Inside("DECLARE @n int; SELECT @n = COUNT(*) FROM dbo.t")),
    ("miner-inside", Inside(MinerInside)),
    ("fn_dblog-inside", Inside("DECLARE @n int; SELECT @n = COUNT(*) FROM sys.fn_dblog(NULL, NULL)")),
    ("guard+miner-inside", Guard + Inside(MinerInside)),
    ("held", Held), ("guard+held", Guard + Held), ("guard+held-var", Guard + ByName(Held)), ("held-var", ByName(Held)), ("db+held-var", GuardDb + ByName(Held)),
    ("guard+fn_dblog-inside", Guard + Inside("DECLARE @n int; SELECT @n = COUNT(*) FROM sys.fn_dblog(NULL, NULL)")),
    ("guard+miner-master", Guard + MinerMaster),
    ("shipped", "shipped"), ("shipped-read", "shipped-read"),
    ("miner-master", MinerMaster),
    ("db+miner-inside", GuardDb + Inside(MinerInside)), ("db+held", GuardDb + Held), ("db+miner-master", GuardDb + MinerMaster), ("db+fixed-inside", GuardDb + Inside("DECLARE @n int, @d int = DB_ID(); SELECT @n = COUNT(*) FROM sys.fn_full_dblog('0x$LSN$', NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);")),
    ("db+log_stats", GuardDb + "DECLARE @x nvarchar(46), @y nvarchar(46); SELECT @x = log_end_lsn, @y = log_min_lsn FROM sys.dm_db_log_stats(@d);"), ("db+file_stats", GuardDb + "DECLARE @x bigint; SELECT @x = SUM(num_of_bytes_written) FROM sys.dm_io_virtual_file_stats(NULL, 2);"),
    ("explicit-start", GuardDb + "DECLARE @n int; SELECT @n = COUNT(*) FROM sys.fn_full_dblog('0x$LSN$', NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"),
    ("null-start", GuardDb + "DECLARE @n int; SELECT @n = COUNT(*) FROM sys.fn_full_dblog(NULL, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"),
    ("fn_full_dblog-master", "DECLARE @n int; " + Full),
    ("guard+fn_full_dblog-master", $"IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{Db}' AND user_access = 1) RETURN; DECLARE @n int; " + Full),
};

await ExecAsync($"IF DB_ID('{Db}') IS NOT NULL BEGIN ALTER DATABASE {Db} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {Db}; END; CREATE DATABASE {Db}; ALTER DATABASE {Db} SET RECOVERY FULL;");
await ExecAsync("CREATE TABLE dbo.t (id int PRIMARY KEY); INSERT dbo.t VALUES (1);", Db);
await ExecAsync($"BACKUP DATABASE {Db} TO DISK = N'{Db}.bak' WITH INIT;");
await ExecAsync($"RESTORE DATABASE {Db} FROM DISK = N'{Db}.bak' WITH REPLACE;");
var fixedLsn = await ScalarAsync($"SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID(N'{Db}'))"); // every restore has the same log
Console.WriteLine($"{await ScalarAsync("SELECT CONVERT(varchar(40), SERVERPROPERTY('ProductVersion'))")}, {op}, {seconds} s per variant, {readers} readers; assertions so far: {await AssertionsAsync()}");

var sql = op switch
{
    "restore" => $"ALTER DATABASE {Db} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE {Db} FROM DISK = N'{Db}.bak' WITH REPLACE; ALTER DATABASE {Db} SET MULTI_USER;",
    "plain" => $"RESTORE DATABASE {Db} FROM DISK = N'{Db}.bak' WITH REPLACE;", // nobody is asked to leave first
    "offline" => $"ALTER DATABASE {Db} SET OFFLINE WITH ROLLBACK IMMEDIATE; ALTER DATABASE {Db} SET ONLINE;",
    "reattach" => $"DECLARE @f nvarchar(max) = (SELECT STRING_AGG(N'(FILENAME = N''' + physical_name + N''')', N', ') FROM sys.master_files WHERE database_id = DB_ID(N'{Db}')); ALTER DATABASE {Db} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; EXEC sp_detach_db N'{Db}', @skipchecks = 'true'; SET @f = N'CREATE DATABASE {Db} ON ' + @f + N' FOR ATTACH'; EXEC (@f);",
    _ => throw new ArgumentException(op),
};
foreach (var (name, read) in variants)
{
    if (only.Length > 0 && !only.Split(',').Contains(name)) continue;
    using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
    var errors = new ConcurrentDictionary<int, int>();
    long statements = 0;
    var loops = read is null ? [] : Enumerable.Range(0, readers).Select(_ => Task.Run(async () =>
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await using var c = new SqlConnection(cs + ";Database=master");
                await c.OpenAsync();
                while (!stop.IsCancellationRequested && read.StartsWith("shipped", StringComparison.Ordinal))
                {
                    // the code itself: the log read, and what comes before and after it when layouts are loaded or the log is released
                    try
                    {
                        int id;
                        await using (var k = new SqlCommand("SELECT DB_ID(@n)", c) { Parameters = { new SqlParameter("@n", Db) } }) id = await k.ExecuteScalarAsync() is short v ? v : -1;
                        var database = new DatabaseRef(id, Db, "");
                        await foreach (var _ in new EngineLogReader(c).ReadAsync(database, null, 5000, CaptureScope.CdcTrackedTables, CancellationToken.None)) { }
                        if (read == "shipped")
                        {
                            await SchemaLoader.LoadAsync(c, database, CaptureScope.AllUserTables, null, new HashSet<long>(), CancellationToken.None);
                            await ReplDoneClient.OldestUnreleasedAsync(c, database, CancellationToken.None);
                            await DatabaseIdentity.RecoveryForkAsync(c, id, Db, CancellationToken.None);
                        }
                        Interlocked.Increment(ref statements);
                        if (pace > 0) await Task.Delay(pace);
                    }
                    catch (Exception ex)
                    {
                        errors.AddOrUpdate(ex is SqlException q ? q.Number : ex is DatabaseChangedException ? 50003 : 1, 1, (_, v) => v + 1);
                        if (ex is not SqlException and not DatabaseChangedException) Console.WriteLine($"  reader: {ex.GetType().Name} {ex.Message.Split((char)10)[0]}");
                        if (c.State != ConnectionState.Open) break;
                        await Task.Delay(back);
                    }
                }
                while (!stop.IsCancellationRequested)
                {
                    await using var k = new SqlCommand("DECLARE @d int = DB_ID(N'" + Db + "'); " + read.Replace("$LSN$", fixedLsn), c) { CommandTimeout = 30 };
                    try { await k.ExecuteNonQueryAsync(); Interlocked.Increment(ref statements); if (pace > 0) await Task.Delay(pace); }
                    catch (SqlException ex) when (c.State == ConnectionState.Open) { errors.AddOrUpdate(ex.Number, 1, (_, v) => v + 1); if (ex.Number == -2) break; await Task.Delay(back); }
                }
            }
            catch (SqlException ex) { errors.AddOrUpdate(ex.Number, 1, (_, v) => v + 1); await Task.Delay(Math.Max(20, back)); }
            catch (InvalidOperationException) { await Task.Delay(20); }
        }
    })).ToArray();
    var (ok, refused, slowest) = (0, new SortedDictionary<int, int>(), 0.0);
    while (!stop.IsCancellationRequested)
    {
        var sw = Stopwatch.StartNew();
        try { await ExecAsync(sql, timeout: 40); ok++; slowest = Math.Max(slowest, sw.Elapsed.TotalSeconds); if (gap > 0) await Task.Delay(Random.Shared.Next(gap)); }
        catch (SqlException ex)
        {
            if (diagnose)
                try
                {
                    Console.WriteLine($"  refused {ex.Number} after {sw.ElapsedMilliseconds} ms: " + await ScalarAsync($"""
                        SELECT CONVERT(nvarchar(max), CONCAT('guard sees ', CONVERT(varchar(20), DATABASEPROPERTYEX(N'{Db}', 'UserAccess')), '/', CONVERT(varchar(20), DATABASEPROPERTYEX(N'{Db}', 'Status')), '; catalog ',
                            (SELECT (user_access_desc + '/' + state_desc) COLLATE DATABASE_DEFAULT FROM sys.databases WHERE name = N'{Db}'), '; holders: ',
                            (SELECT STRING_AGG(CONCAT(l.request_session_id, ' ', l.request_mode COLLATE DATABASE_DEFAULT, '/', l.request_status COLLATE DATABASE_DEFAULT, ' ', l.request_owner_type COLLATE DATABASE_DEFAULT, ' [', s.status COLLATE DATABASE_DEFAULT, ' ', r.command COLLATE DATABASE_DEFAULT, ' ', r.wait_type COLLATE DATABASE_DEFAULT, ' blk ', r.blocking_session_id, '] ', LEFT(REPLACE(REPLACE(ISNULL(t.text, ib.event_info) COLLATE DATABASE_DEFAULT, CHAR(10), ' '), CHAR(13), ' '), 70)), ' | ')
                             FROM sys.dm_tran_locks l LEFT JOIN sys.dm_exec_sessions s ON s.session_id = l.request_session_id LEFT JOIN sys.dm_exec_requests r ON r.session_id = l.request_session_id
                             OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t OUTER APPLY sys.dm_exec_input_buffer(l.request_session_id, NULL) ib
                             WHERE l.resource_type = 'DATABASE' AND l.resource_database_id = DB_ID(N'{Db}'))))
                        """));
                }
                catch (Exception d) { Console.WriteLine("  refused; diagnosis failed: " + d.Message.Split((char)10)[0]); }
            foreach (SqlError e in ex.Errors) { refused[e.Number] = refused.GetValueOrDefault(e.Number) + 1; break; }
            for (var i = 0; i < 100; i++) { try { await ExecAsync(op == "reattach" ? $"IF DB_ID(N'{Db}') IS NOT NULL ALTER DATABASE {Db} SET MULTI_USER WITH ROLLBACK IMMEDIATE ELSE THROW 50000, 'detached', 1;" : op == "offline" ? $"ALTER DATABASE {Db} SET ONLINE;" : $"ALTER DATABASE {Db} SET MULTI_USER WITH ROLLBACK IMMEDIATE;", timeout: 20); break; } catch (SqlException e) { if (diagnose && i % 25 == 0) Console.WriteLine($"  cannot set MULTI_USER ({i}): {e.Number} {e.Message.Split((char)10)[0]}"); await Task.Delay(20); } }
        }
    }
    await Task.WhenAny(Task.WhenAll(loops), Task.Delay(45_000));
    var alive = "alive";
    for (var i = 0; i < 5; i++)
        try { await ScalarAsync("SELECT COUNT(*) FROM sys.databases"); alive = "alive"; break; } catch (SqlException ex) when (ex.Number == 1205) { alive = "deadlock victim 5 times"; } catch (Exception ex) { alive = "HUNG: " + ex.Message.Split('\n')[0]; }
    Console.WriteLine($"{name,-28} {op} ok {ok,4}, refused {string.Join(' ', refused.Select(e => $"{e.Key}x{e.Value}")),-14} slowest {slowest:F1} s; reads {statements,8}, errors {string.Join(' ', errors.OrderBy(e => e.Key).Select(e => $"{e.Key}x{e.Value}"))}; {alive}; assertions {await AssertionsAsync()}");
    if (alive != "alive") return 3;
}
return 0;
