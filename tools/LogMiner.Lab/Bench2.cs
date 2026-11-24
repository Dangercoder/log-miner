using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>Candidate doorbells: how cheap is each, and how soon after a commit does it move?</summary>
internal static class Bench2
{
    private static readonly (string Name, string Sql)[] Candidates =
    [
        ("perf: Log Flushes/sec", "SELECT instance_name, cntr_value FROM sys.dm_os_performance_counters WHERE object_name = 'SQLServer:Databases' AND counter_name = 'Log Flushes/sec' /*lm-bell-a*/"),
        ("perf: Repl. Pending Xacts", "SELECT instance_name, cntr_value FROM sys.dm_os_performance_counters WHERE object_name = 'SQLServer:Databases' AND counter_name = 'Repl. Pending Xacts' /*lm-bell-b*/"),
        ("perf: Repl. Trans. Rate", "SELECT instance_name, cntr_value FROM sys.dm_os_performance_counters WHERE object_name = 'SQLServer:Databases' AND counter_name = 'Repl. Trans. Rate' /*lm-bell-c*/"),
        ("dm_db_log_stats one db", "SELECT log_end_lsn FROM sys.dm_db_log_stats(@dbid) /*lm-bell-d*/"),
        ("tran_database_transactions", "SELECT database_id, COUNT(*) FROM sys.dm_tran_database_transactions GROUP BY database_id /*lm-bell-e*/"),
        ("sp_replcounters", "EXEC sp_replcounters /*lm-bell-f*/"),
        ("log_stats CROSS APPLY all", "SELECT d.database_id, s.log_end_lsn FROM sys.databases d CROSS APPLY sys.dm_db_log_stats(d.database_id) s WHERE d.database_id > 4 /*lm-bell-g*/"),
        ("log_stats CROSS APPLY 50", "SELECT d.database_id, s.log_end_lsn FROM sys.databases d CROSS APPLY sys.dm_db_log_stats(d.database_id) s WHERE d.database_id BETWEEN 5 AND 54 /*lm-bell-h*/"),
        ("wait_stats WRITELOG", "SELECT waiting_tasks_count FROM sys.dm_os_wait_stats WHERE wait_type = N'WRITELOG' /*lm-bell-i*/"),
        ("file_stats one db", "SELECT num_of_bytes_written FROM sys.dm_io_virtual_file_stats(@dbid, 2) /*lm-bell-j*/"),
    ];

    public static async Task<int> Bells(string[] args)
    {
        var n = int.Parse(Program.Opt(args, "--n") ?? "300", CultureInfo.InvariantCulture);
        await using var conn = new SqlConnection(Program.ConnectionString());
        await conn.OpenAsync();
        foreach (var (name, sql) in Candidates)
        {
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@dbid", 6);
            var lat = new double[n];
            var rows = 0;
            for (var i = 0; i < n; i++)
            {
                var sw = Stopwatch.StartNew();
                await using (var r = await cmd.ExecuteReaderAsync()) while (await r.ReadAsync()) rows++;
                lat[i] = sw.Elapsed.TotalMilliseconds;
            }
            Array.Sort(lat);
            Console.WriteLine(FormattableString.Invariant($"{name,-30} rows/call={rows / n,5} client ms p50={lat[n / 2]:F3} p99={lat[(int)(n * 0.99)]:F3}"));
        }
        return 0;
    }

    /// <summary>Commit in tenant_0007 and spin on a counter until it moves: is the counter live?</summary>
    public static async Task<int> Liveness(string[] args)
    {
        var counter = Program.Opt(args, "--counter") ?? "Repl. Pending Xacts";
        var db = Program.Opt(args, "--db") ?? "tenant_0007";
        await using var writer = new SqlConnection(Program.ConnectionString(db));
        await using var reader = new SqlConnection(Program.ConnectionString());
        await writer.OpenAsync();
        await reader.OpenAsync();
        await using var read = new SqlCommand("SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name = 'SQLServer:Databases' AND counter_name = @c AND instance_name = @db", reader);
        read.Parameters.AddWithValue("@c", counter);
        read.Parameters.AddWithValue("@db", db);
        await using var write = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", writer);
        for (var round = 0; round < 5; round++)
        {
            var before = Convert.ToInt64(await read.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
            var sw = Stopwatch.StartNew();
            await write.ExecuteNonQueryAsync();
            var committedAt = sw.Elapsed.TotalMilliseconds;
            long now;
            var polls = 0;
            do { now = Convert.ToInt64(await read.ExecuteScalarAsync(), CultureInfo.InvariantCulture); polls++; }
            while (now == before && sw.ElapsedMilliseconds < 3000);
            Console.WriteLine(FormattableString.Invariant($"{counter}: {before} -> {now} commit returned at {committedAt:F2} ms, counter moved at {sw.Elapsed.TotalMilliseconds:F2} ms after {polls} polls"));
        }
        return 0;
    }
}
