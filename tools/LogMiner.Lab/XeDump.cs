using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// Records raw live-stream packets of a concurrent workload over several databases (fixtures for the parser), and the growth of
/// num_of_bytes_written over the same time.   lab xedump out-dir [--writers 12] [--seconds 3] [--every-ms 100]
/// </summary>
internal static class XeDump
{
    private const string Session = "logminer_lab_dump";

    public static async Task<int> Run(string[] args)
    {
        var dir = args[1];
        Directory.CreateDirectory(dir);
        var writers = int.Parse(Program.Opt(args, "--writers") ?? "12", CultureInfo.InvariantCulture);
        var seconds = int.Parse(Program.Opt(args, "--seconds") ?? "3", CultureInfo.InvariantCulture);
        var everyMs = int.Parse(Program.Opt(args, "--every-ms") ?? "100", CultureInfo.InvariantCulture);
        var paceMs = int.Parse(Program.Opt(args, "--pace-ms") ?? "0", CultureInfo.InvariantCulture);
        await using var admin = new SqlConnection(Program.ConnectionString());
        await admin.OpenAsync();
        async Task Sql(string sql) { await using var cmd = new SqlCommand(sql, admin); await cmd.ExecuteNonQueryAsync(); }
        async Task<Dictionary<int, long>> Bytes()
        {
            var d = new Dictionary<int, long>();
            await using var cmd = new SqlCommand("SELECT database_id, num_of_bytes_written FROM sys.dm_io_virtual_file_stats(NULL, 2) WHERE database_id > 4", admin);
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) d[r.GetInt16(0)] = r.GetInt64(1);
            return d;
        }
        async Task<string> Others()
        {
            await using var cmd = new SqlCommand("SELECT CONCAT(s.name, ' ', t.target_name, ': ', t.execution_count, ' executions, ', s.total_bytes_generated, ' bytes generated; ') FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_targets t ON t.event_session_address = s.address WHERE s.name <> @s", admin);
            cmd.Parameters.AddWithValue("@s", Session);
            var sb = new System.Text.StringBuilder();
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) sb.Append(r.GetString(0));
            return sb.ToString();
        }
        await Sql($"IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '{Session}') DROP EVENT SESSION {Session} ON SERVER;");
        await Sql($"CREATE EVENT SESSION {Session} ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4) WITH (MAX_MEMORY = 1024 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE);");
        await Sql($"ALTER EVENT SESSION {Session} ON SERVER STATE = START;");
        var packets = new ConcurrentQueue<(int Type, byte[] Data)>();
        using var cts = new CancellationTokenSource();
        var ready = new TaskCompletionSource();
        var stream = Task.Run(async () =>
        {
            await using var conn = new SqlConnection(Program.ConnectionString());
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("SELECT type, data FROM sys.fn_MSxe_read_event_stream(@source, 0)", conn) { CommandTimeout = 0 };
            cmd.Parameters.Add("@source", System.Data.SqlDbType.NVarChar, 256).Value = Session;
            await using var r = await cmd.ExecuteReaderAsync();
            ready.TrySetResult();
            while (await r.ReadAsync()) packets.Enqueue((r.GetInt32(0), r.GetSqlBinary(1).Value));
            return "stream ended without an error";
        });
        await Task.WhenAny(ready.Task, stream);
        byte[] address;
        await using (var find = new SqlCommand($"SELECT address FROM sys.dm_xe_sessions WHERE name = '{Session}'", admin)) address = (byte[])(await find.ExecuteScalarAsync())!;
        Console.WriteLine("other sessions before: " + await Others());
        var forces = 0;
        var force = Task.Run(async () =>
        {
            await using var conn = new SqlConnection(Program.ConnectionString());
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = @a", conn);
            cmd.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = address;
            while (!cts.IsCancellationRequested) { await cmd.ExecuteScalarAsync(); forces++; await Task.Delay(everyMs); }
        });
        await Task.Delay(1500);
        var before = await Bytes();
        var expected = 0;
        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            await using var conn = new SqlConnection(Program.ConnectionString($"tenant_{20 + w:0000}"));
            await conn.OpenAsync();
            var until = Stopwatch.StartNew();
            var rnd = new Random(w);
            while (until.Elapsed.TotalSeconds < seconds)
            {
                await using var cmd = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) SELECT TOP (@n) 1, 9.99, 1, SYSUTCDATETIME() FROM sys.all_objects", conn);
                cmd.Parameters.AddWithValue("@n", rnd.Next(10) == 0 ? 800 : 1);
                await cmd.ExecuteNonQueryAsync();
                Interlocked.Increment(ref expected);
                if (paceMs > 0) await Task.Delay(rnd.Next(paceMs)); else if (w % 3 == 0) await Task.Delay(rnd.Next(5));
            }
        })));
        await Task.Delay(2500);
        var after = await Bytes();
        await cts.CancelAsync();
        await force;
        var all = packets.ToArray();
        Console.WriteLine($"other sessions after {forces} forcing calls: " + await Others());
        for (var i = 0; i < all.Length; i++) await File.WriteAllBytesAsync(Path.Combine(dir, $"{i:000}-type{all[i].Type}.bin"), all[i].Data);
        Console.WriteLine($"{all.Length} packets, {all.Sum(p => p.Data.Length)} bytes for {expected} transactions; types {string.Join(", ", all.GroupBy(p => p.Type).Select(g => $"{g.Key}: {g.Count()} ({g.Min(p => p.Data.Length)}..{g.Max(p => p.Data.Length)} bytes)"))}");
        await File.WriteAllLinesAsync(Path.Combine(dir, "bytes-written.txt"), after.Where(a => a.Value != before.GetValueOrDefault(a.Key)).OrderBy(a => a.Key).Select(a => $"{a.Key} {a.Value - before.GetValueOrDefault(a.Key)}"));
        await using (var dropped = new SqlCommand($"SELECT CONCAT(dropped_event_count, ' events, ', dropped_buffer_count, ' buffers dropped, ', total_bytes_generated, ' bytes; buffers ', total_regular_buffers, ' x ', regular_buffer_size) FROM sys.dm_xe_sessions WHERE name = '{Session}'", admin)) Console.WriteLine(await dropped.ExecuteScalarAsync());
        // how does the stream end when the session is stopped from outside?
        var sw = Stopwatch.StartNew();
        await Sql($"DROP EVENT SESSION {Session} ON SERVER;");
        var end = await Task.WhenAny(stream, Task.Delay(5000)) == stream ? stream.IsFaulted ? "faulted: " + stream.Exception!.InnerException!.Message : await stream : "still open after 5 s";
        Console.WriteLine($"DROP EVENT SESSION -> {end} ({sw.ElapsedMilliseconds} ms); last packet type {packets.Last().Type}, {packets.Last().Data.Length} bytes");
        return 0;
    }
}
