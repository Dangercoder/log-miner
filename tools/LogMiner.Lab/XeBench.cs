using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// Can Extended Events ring the doorbell? Attaches the live event stream (sys.fn_MSxe_read_event_stream, what SSMS "watch live data" uses)
/// to a session on log_flush_complete and measures commit → buffer arrival, with different ways of forcing dispatch.
///   lab xe [--force none|counter|sessions|stop] [--every-ms 2] [--commits 200] [--gap-ms 50]
/// </summary>
internal static class XeBench
{
    private const string Session = "logminer_lab_bell";

    public static async Task<int> Run(string[] args)
    {
        var force = Program.Opt(args, "--force") ?? "counter";
        var everyMs = int.Parse(Program.Opt(args, "--every-ms") ?? "2", CultureInfo.InvariantCulture);
        var commits = int.Parse(Program.Opt(args, "--commits") ?? "100", CultureInfo.InvariantCulture);
        var gapMs = int.Parse(Program.Opt(args, "--gap-ms") ?? "50", CultureInfo.InvariantCulture);
        var options = Program.Opt(args, "--with") ?? "MAX_MEMORY = 1024 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE";
        var target = (Program.Opt(args, "--target") ?? "none") switch
        {
            "none" => "",
            "counter" => "ADD TARGET package0.event_counter",
            "ring" => "ADD TARGET package0.ring_buffer (SET max_events_limit = 1, max_memory = 64)",
            "histogram" => "ADD TARGET package0.histogram (SET filtering_event_name = N'sqlserver.log_flush_complete', source = N'database_id', source_type = 0, slots = 2)",
            _ => throw new ArgumentException("--target"),
        };
        var dump = args.Contains("--dump");

        await using var admin = new SqlConnection(Program.ConnectionString());
        await admin.OpenAsync();
        async Task Sql(string sql) { await using var cmd = new SqlCommand(sql, admin); await cmd.ExecuteNonQueryAsync(); }
        await Sql($"IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '{Session}') DROP EVENT SESSION {Session} ON SERVER;");
        await Sql($"CREATE EVENT SESSION {Session} ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4) {target} WITH ({options});");
        await Sql($"ALTER EVENT SESSION {Session} ON SERVER STATE = START;");
        try
        {
            using var cts = new CancellationTokenSource();
            var arrivals = new ConcurrentQueue<(long At, int Type, int Bytes)>();
            var arrived = new SemaphoreSlim(0);
            var ready = new TaskCompletionSource();
            var stream = Task.Run(async () =>
            {
                await using var conn = new SqlConnection(Program.ConnectionString());
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("SELECT type, data FROM sys.fn_MSxe_read_event_stream(@source, 0)", conn) { CommandTimeout = 0 };
                cmd.Parameters.Add("@source", System.Data.SqlDbType.NVarChar, 256).Value = Session;
                await using var r = await cmd.ExecuteReaderAsync(cts.Token);
                Console.WriteLine($"columns: {r.GetName(0)} {r.GetDataTypeName(0)}, {r.GetName(1)} {r.GetDataTypeName(1)}");
                ready.TrySetResult();
                while (await r.ReadAsync(cts.Token))
                {
                    var type = r.GetInt32(0);
                    var data = r.GetSqlBinary(1).Value;
                    arrivals.Enqueue((Stopwatch.GetTimestamp(), type, data.Length));
                    if (dump) Console.WriteLine($"type {type} {data.Length} bytes {Convert.ToHexString(data, 0, Math.Min(data.Length, 400))}");
                    arrived.Release();
                }
            });
            await Task.WhenAny(ready.Task, stream, Task.Delay(5000));
            if (stream.IsFaulted) { Console.WriteLine(stream.Exception!.InnerException!.Message); return 1; }

            // what forces a dispatch?
            var forceSql = force switch
            {
                "none" => null,
                "counter" => $"SELECT t.target_data FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_targets t ON t.event_session_address = s.address WHERE s.name = '{Session}' AND t.target_name = 'event_counter' /*lm-xe-force*/",
                "noblob" => $"SELECT t.execution_count FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_targets t ON t.event_session_address = s.address WHERE s.name = '{Session}' AND t.target_name = 'event_counter' /*lm-xe-force*/",
                "address" => $"SELECT t.execution_count FROM sys.dm_xe_session_targets t WHERE t.event_session_address = @a /*lm-xe-force*/",
                "addrtarget" => $"SELECT t.execution_count FROM sys.dm_xe_session_targets t WHERE t.event_session_address = @a AND t.target_name = 'event_counter' /*lm-xe-force*/",
                "top1" => $"SELECT TOP (1) t.execution_count FROM sys.dm_xe_session_targets t WHERE t.event_session_address = @a /*lm-xe-force*/",
                "addrdata" => $"SELECT t.target_data FROM sys.dm_xe_session_targets t WHERE t.event_session_address = @a /*lm-xe-force*/",
                "events" => $"SELECT COUNT(*) FROM sys.dm_xe_session_events t WHERE t.event_session_address = @a /*lm-xe-force*/",
                "columns" => $"SELECT COUNT(*) FROM sys.dm_xe_session_object_columns t WHERE t.event_session_address = @a /*lm-xe-force*/",
                "sessions" => $"SELECT s.dropped_event_count FROM sys.dm_xe_sessions s WHERE s.name = '{Session}' /*lm-xe-force*/",
                _ => throw new ArgumentException(force),
            };
                var tag = $"lm-xe-force-{Environment.ProcessId}";
            forceSql = forceSql?.Replace("lm-xe-force", tag, StringComparison.Ordinal);
            var polls = 0L;
            var pollMs = new List<double>();
            var poller = forceSql is null ? Task.CompletedTask : Task.Run(async () =>
            {
                await using var conn = new SqlConnection(Program.ConnectionString());
                await conn.OpenAsync();
                await using var cmd = new SqlCommand(forceSql, conn);
                await using (var find = new SqlCommand($"SELECT address FROM sys.dm_xe_sessions WHERE name = '{Session}'", conn))
                    cmd.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = await find.ExecuteScalarAsync();
                while (!cts.IsCancellationRequested)
                {
                    var t = Stopwatch.GetTimestamp();
                    var v = await cmd.ExecuteScalarAsync();
                    if (polls++ == 0) Console.WriteLine($"force returns: {v}");
                    pollMs.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                    await Task.Delay(everyMs);
                }
            });
            _ = timeBeginPeriod(1);
            await Task.Delay(500);
            arrivals.Clear();

            await using var writer = new SqlConnection(Program.ConnectionString("tenant_0007"));
            await writer.OpenAsync();
            await using var write = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", writer);
            var lat = new List<double>();
            var lost = 0;
            for (var i = 0; i < commits; i++)
            {
                while (arrived.CurrentCount > 0) arrived.Wait();
                arrivals.Clear();
                var start = Stopwatch.GetTimestamp();
                await write.ExecuteNonQueryAsync();
                var committed = Stopwatch.GetTimestamp();
                if (!await arrived.WaitAsync(4000)) { lost++; continue; }
                arrivals.TryPeek(out var a);
                lat.Add((a.At - committed) * 1000.0 / Stopwatch.Frequency);
                await Task.Delay(gapMs);
            }
            await cts.CancelAsync();
            lat.Sort();
            pollMs.Sort();
            string P(List<double> l, double p) => l.Count == 0 ? "-" : l[Math.Min(l.Count - 1, (int)(l.Count * p))].ToString("F2", CultureInfo.InvariantCulture);
            Console.WriteLine($"force={force} every={everyMs} ms: {lat.Count} arrived, {lost} timed out; commit returned -> buffer ms: min {P(lat, 0)} p50 {P(lat, 0.5)} p90 {P(lat, 0.9)} p99 {P(lat, 0.99)} max {P(lat, 1)}");
            Console.WriteLine($"{polls} forcing calls, ms p50 {P(pollMs, 0.5)} p99 {P(pollMs, 0.99)}");
            await using var cost = new SqlCommand($"SELECT TOP (1) CONCAT(qs.total_worker_time / qs.execution_count, ' cpu, ', qs.total_elapsed_time / qs.execution_count, ' elapsed') FROM sys.dm_exec_query_stats qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) t WHERE t.text LIKE '%{tag}%' AND t.text NOT LIKE '%dm_exec_query_stats%' ORDER BY qs.last_execution_time DESC", admin);
            Console.WriteLine($"server CPU per forcing call: {await cost.ExecuteScalarAsync()} us");
        }
        finally
        {
            await Sql($"DROP EVENT SESSION {Session} ON SERVER;");
        }
        return 0;
    }

    [System.Runtime.InteropServices.DllImport("winmm.dll")] private static extern uint timeBeginPeriod(uint ms);
}
