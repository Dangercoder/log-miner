using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using LogMiner.Log;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// Is a log block always visible to fn_full_dblog when its log_flush_complete event arrives? Reads each pushed block immediately, under
/// concurrent commits, and reports how often the read did not reach it (and how long it took until it did).
///   lab xerace [--commits 2000] [--noise 8]
/// </summary>
internal static class XeRace
{
    private const string Session = "logminer_lab_race";

    public static async Task<int> Run(string[] args)
    {
        var commits = int.Parse(Program.Opt(args, "--commits") ?? "2000", CultureInfo.InvariantCulture);
        var noise = int.Parse(Program.Opt(args, "--noise") ?? "8", CultureInfo.InvariantCulture);
        await using var admin = new SqlConnection(Program.ConnectionString());
        await admin.OpenAsync();
        async Task Sql(string sql) { await using var cmd = new SqlCommand(sql, admin); await cmd.ExecuteNonQueryAsync(); }
        await Sql($"IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '{Session}') DROP EVENT SESSION {Session} ON SERVER;");
        await Sql($"CREATE EVENT SESSION {Session} ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4) WITH (MAX_MEMORY = 1024 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE);");
        await Sql($"ALTER EVENT SESSION {Session} ON SERVER STATE = START;");
        try
        {
            var columns = new List<(string, string)>();
            await using (var cmd = new SqlCommand("SELECT name, type_name FROM sys.dm_xe_object_columns WHERE object_name = 'log_flush_complete' AND column_type = 'data' ORDER BY column_id", admin))
            await using (var r = await cmd.ExecuteReaderAsync())
                while (await r.ReadAsync()) columns.Add((r.GetString(0), r.GetString(1)));
            var decoder = new LogFlushDecoder(columns);
            byte[] address;
            await using (var find = new SqlCommand($"SELECT address FROM sys.dm_xe_sessions WHERE name = '{Session}'", admin)) address = (byte[])(await find.ExecuteScalarAsync())!;
            int dbId;
            await using (var id = new SqlCommand("SELECT DB_ID('tenant_0007')", admin)) dbId = Convert.ToInt32(await id.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
            using var cts = new CancellationTokenSource();
            var bells = new BlockingCollection<(long At, LogFlush Flush)>();
            var ready = new TaskCompletionSource();
            _ = Task.Run(async () =>
            {
                await using var conn = new SqlConnection(Program.ConnectionString());
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("SELECT type, data FROM sys.fn_MSxe_read_event_stream(@source, 0)", conn) { CommandTimeout = 0 };
                cmd.Parameters.Add("@source", System.Data.SqlDbType.NVarChar, 256).Value = Session;
                await using var r = await cmd.ExecuteReaderAsync();
                var list = new List<LogFlush>();
                while (await r.ReadAsync())
                {
                    list.Clear();
                    decoder.Decode(r.GetInt32(0), r.GetSqlBinary(1).Value, list);
                    if (decoder.Ready) ready.TrySetResult();
                    var now = Stopwatch.GetTimestamp();
                    foreach (var f in list) if (f.DatabaseId == dbId) bells.Add((now, f));
                }
            });
            await ready.Task;
            _ = Task.Run(async () =>
            {
                await using var conn = new SqlConnection(Program.ConnectionString());
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = @a", conn);
                cmd.Parameters.Add("@a", System.Data.SqlDbType.VarBinary, 8).Value = address;
                while (!cts.IsCancellationRequested) await cmd.ExecuteScalarAsync(); // as fast as possible: the tightest race
            });
            foreach (var w in Enumerable.Range(0, noise))
                _ = Task.Run(async () =>
                {
                    await using var conn = new SqlConnection(Program.ConnectionString($"tenant_{20 + w:0000}"));
                    await conn.OpenAsync();
                    while (!cts.IsCancellationRequested)
                    {
                        await using var cmd = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", conn);
                        await cmd.ExecuteNonQueryAsync();
                    }
                });
            await using var writer = new SqlConnection(Program.ConnectionString("tenant_0007"));
            await writer.OpenAsync();
            await using var read = new SqlConnection(Program.ConnectionString());
            await read.OpenAsync();
            await using var max = new SqlCommand("""
                SELECT MAX([Current LSN]) FROM sys.fn_full_dblog('0x' + @start, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) OPTION (MAXDOP 1)
                """, read);
            max.Parameters.Add("@start", System.Data.SqlDbType.NVarChar, 46);
            max.Parameters.Add("@dbid", System.Data.SqlDbType.Int).Value = dbId;
            string start;
            await using (var s = new SqlCommand("SELECT MAX([Current LSN]) FROM sys.fn_full_dblog(NULL, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", read))
            {
                s.Parameters.AddWithValue("@dbid", dbId);
                start = (string)(await s.ExecuteScalarAsync())!;
            }
            int late = 0, beforeCommitReturned = 0;
            var lateMs = new List<double>();
            var readMs = new List<double>();
            for (var i = 0; i < commits; i++)
            {
                await using var write = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", writer);
                var task = write.ExecuteNonQueryAsync();
                var bell = bells.Take();
                if (!task.IsCompleted) beforeCommitReturned++;
                var t = Stopwatch.GetTimestamp();
                var attempts = 0;
                Lsn seen;
                do
                {
                    max.Parameters["@start"].Value = start;
                    seen = Lsn.Parse((string)(await max.ExecuteScalarAsync())!);
                    if (attempts++ == 0) readMs.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                } while (seen < bell.Flush.Block && attempts < 1000);
                if (attempts > 1) { late++; lateMs.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds); }
                start = seen.ToString();
                await task;
                while (bells.TryTake(out _)) { }
            }
            await cts.CancelAsync();
            readMs.Sort();
            lateMs.Sort();
            Console.WriteLine(FormattableString.Invariant($"{commits} pushed blocks read at once ({noise} concurrent writers): {late} not visible in the first read{(late > 0 ? $" (visible after {lateMs[lateMs.Count / 2]:F2} ms, worst {lateMs[^1]:F2} ms)" : "")}; bell before the commit returned to the client: {beforeCommitReturned}; first read p50 {readMs[readMs.Count / 2]:F2} p99 {readMs[(int)(readMs.Count * 0.99)]:F2} ms"));
            await Task.Delay(200);
        }
        finally { await Sql($"DROP EVENT SESSION {Session} ON SERVER;"); }
        return 0;
    }
}
