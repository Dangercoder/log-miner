using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// Forcing one event session's dispatch dispatches all of them (experiments/24). What does that do to somebody else's busy session with a
/// file target?   lab xeneighbours [--seconds 8] [--rate 2000] [--force-ms 3]
/// And: is dm_xe_sessions.total_bytes_generated up to date the moment an event fires (and does reading it dispatch anything)?
/// </summary>
internal static class XeNeighbours
{
    private const string Mine = "logminer_lab_mine", Other = "logminer_lab_trace";

    public static async Task<int> Run(string[] args)
    {
        var seconds = double.Parse(Program.Opt(args, "--seconds") ?? "8", CultureInfo.InvariantCulture);
        var rate = int.Parse(Program.Opt(args, "--rate") ?? "2000", CultureInfo.InvariantCulture);
        var forceMs = int.Parse(Program.Opt(args, "--force-ms") ?? "3", CultureInfo.InvariantCulture);
        await using var admin = new SqlConnection(Program.ConnectionString());
        await admin.OpenAsync();
        async Task Sql(string sql) { await using var cmd = new SqlCommand(sql, admin); await cmd.ExecuteNonQueryAsync(); }
        async Task<object> Scalar(string sql) { await using var cmd = new SqlCommand(sql, admin); return (await cmd.ExecuteScalarAsync())!; }
        await Sql($"IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '{Mine}') DROP EVENT SESSION {Mine} ON SERVER; IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '{Other}') DROP EVENT SESSION {Other} ON SERVER;");
        await Sql($"CREATE EVENT SESSION {Mine} ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4) WITH (MAX_MEMORY = 1024 KB, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE); ALTER EVENT SESSION {Mine} ON SERVER STATE = START;");
        var mine = (byte[])await Scalar($"SELECT address FROM sys.dm_xe_sessions WHERE name = '{Mine}'");

        // 1. which instance-wide counters move with the commit? (reading them dispatches nothing)
        await using (var writer = new SqlConnection(Program.ConnectionString("tenant_0007")))
        {
            await writer.OpenAsync();
            await using var idle = new SqlConnection(Program.ConnectionString());
            await idle.OpenAsync();
            foreach (var (name, sql) in new[]
            {
                ("@@TOTAL_WRITE", "SELECT CONVERT(bigint, @@TOTAL_WRITE)"),
                ("total_bytes_generated", "SELECT total_bytes_generated FROM sys.dm_xe_sessions WHERE address = @a"),
                ("wait_stats WRITELOG", "SELECT waiting_tasks_count FROM sys.dm_os_wait_stats WHERE wait_type = N'WRITELOG'"),
                ("perf Log Flushes/sec _Total", "SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE N'%:Databases%' AND counter_name = N'Log Flushes/sec' AND instance_name = N'_Total'"),
                ("perf Page writes", "SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = N'Transactions/sec' AND instance_name = N'_Total'"),
            })
            {
                await using var read = new SqlCommand(sql, admin);
                read.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = mine;
                int seen = 0, idleMoves = 0, rounds = 60;
                var late = new List<double>();
                for (var i = 0; i < rounds; i++)
                {
                    var before = Convert.ToInt64(await read.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
                    await Task.Delay(5);
                    var after = Convert.ToInt64(await read.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
                    if (after != before) idleMoves++;
                    await using var write = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", writer);
                    await write.ExecuteNonQueryAsync();
                    var t = Stopwatch.GetTimestamp();
                    var first = true;
                    while (Convert.ToInt64(await read.ExecuteScalarAsync(), CultureInfo.InvariantCulture) == after && Stopwatch.GetElapsedTime(t).TotalMilliseconds < 300) first = false;
                    if (first) seen++; else late.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                }
                await using var many = new SqlCommand($"DECLARE @i int = 0, @b bigint; WHILE @i < 2000 BEGIN SELECT @b = ({sql}); SET @i += 1; END", admin) { CommandTimeout = 60 };
                many.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = mine;
                var sw = Stopwatch.StartNew();
                await many.ExecuteNonQueryAsync();
                Console.WriteLine(FormattableString.Invariant($"{name}: moved by the first read after the commit in {seen} of {rounds}, later in {late.Count} (max {(late.Count > 0 ? late.Max() : 0):F1} ms); moved on its own in {idleMoves} of {rounds} idle 5 ms; {sw.Elapsed.TotalMilliseconds / 2:F1} us per read"));
            }
        }

        // 2. the neighbour
        var dir = (string)await Scalar("SELECT CONVERT(nvarchar(260), SERVERPROPERTY('ErrorLogFileName'))");
        dir = Path.GetDirectoryName(dir)!;
        foreach (var force in new[] { false, true, false, true })
        {
            await Sql($"CREATE EVENT SESSION {Other} ON SERVER ADD EVENT sqlserver.sql_batch_completed (WHERE sqlserver.client_app_name = 'lm-neighbour') ADD TARGET package0.event_file (SET filename = N'{Path.Combine(dir, Other)}.xel', max_file_size = 50, max_rollover_files = 2) WITH (MAX_MEMORY = 4096 KB, MAX_DISPATCH_LATENCY = 30 SECONDS); ALTER EVENT SESSION {Other} ON SERVER STATE = START;");
            var cpu0 = Convert.ToInt64(await Scalar("SELECT SUM(total_cpu_usage_ms) FROM sys.dm_os_schedulers"), CultureInfo.InvariantCulture);
            using var stop = new CancellationTokenSource();
            var forces = 0;
            var forcing = !force ? Task.CompletedTask : Task.Run(async () =>
            {
                await using var conn = new SqlConnection(Program.ConnectionString());
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = @a", conn);
                cmd.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = mine;
                while (!stop.IsCancellationRequested) { await cmd.ExecuteScalarAsync(); forces++; await Task.Delay(forceMs); }
            });
            var done = 0;
            var clock = Stopwatch.StartNew();
            await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                await using var conn = new SqlConnection(Program.ConnectionString().Replace("logminer-lab", "lm-neighbour", StringComparison.Ordinal));
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("SELECT 1", conn);
                var n = 0;
                while (clock.Elapsed.TotalSeconds < seconds)
                {
                    await cmd.ExecuteScalarAsync();
                    Interlocked.Increment(ref done);
                    var ahead = ++n * 8.0 / rate - clock.Elapsed.TotalSeconds;
                    if (ahead > 0) await Task.Delay(TimeSpan.FromSeconds(ahead));
                }
            })));
            await stop.CancelAsync();
            await forcing;
            var elapsed = clock.Elapsed.TotalSeconds;
            var cpu = Convert.ToInt64(await Scalar("SELECT SUM(total_cpu_usage_ms) FROM sys.dm_os_schedulers"), CultureInfo.InvariantCulture) - cpu0;
            var stats = await Scalar($"SELECT CONCAT(t.execution_count, ' dispatches, ', t.execution_duration_ms, ' ms in the target; ', s.total_bytes_generated, ' bytes of events, dropped ', s.dropped_event_count) FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_targets t ON t.event_session_address = s.address WHERE s.name = '{Other}'");
            await Sql($"DROP EVENT SESSION {Other} ON SERVER;");
            var bytes = await Scalar($"SELECT SUM(size_in_bytes) FROM sys.dm_os_enumerate_filesystem(N'{dir}', N'{Other}*.xel')");
            foreach (var f in await FilesAsync(admin, dir)) await Sql($"EXEC sys.xp_delete_files N'{f}';");
            Console.WriteLine(FormattableString.Invariant($"{(force ? $"forcing every {forceMs} ms ({forces} calls)" : "not forcing")}: {done / elapsed:F0} events/s; {stats}; files {bytes} bytes; SQL Server CPU {cpu / elapsed / 10:F1}% of one core"));
        }
        await Sql($"DROP EVENT SESSION {Mine} ON SERVER;");
        return 0;
    }

    private static async Task<List<string>> FilesAsync(SqlConnection conn, string dir)
    {
        var files = new List<string>();
        await using var cmd = new SqlCommand($"SELECT full_filesystem_path FROM sys.dm_os_enumerate_filesystem(N'{dir}', N'{Other}*.xel')", conn);
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) files.Add(r.GetString(0));
        return files;
    }
}
