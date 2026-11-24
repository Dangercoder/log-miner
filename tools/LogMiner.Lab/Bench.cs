using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>Micro-benchmarks for the design decisions (doorbell cost, empty log read cost).</summary>
internal static class Bench
{
    public const string DoorbellSql =
        "SELECT database_id, num_of_bytes_written FROM sys.dm_io_virtual_file_stats(NULL, NULL) WHERE file_id = 2 /*logminer-doorbell*/";

    public const string ReadSql = """
        SELECT [Current LSN], Operation, Context, [Transaction ID], CONVERT(int, [Flag Bits]) AS flags, AllocUnitId, PartitionId,
               [Offset in Row], [Modify Size], [Log Record Length], [RowLog Contents 0], [RowLog Contents 1], [Transaction Begin], [End Time]
        FROM sys.fn_full_dblog(@start, @end, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        WHERE (CONVERT(int, [Flag Bits]) & 5) <> 0 OR Operation = N'LOP_ABORT_XACT' /*logminer-read*/
        """;

    public static async Task<int> Doorbell(string[] args)
    {
        var n = int.Parse(Program.Opt(args, "--n") ?? "2000", CultureInfo.InvariantCulture);
        await using var conn = new SqlConnection(Program.ConnectionString());
        await conn.OpenAsync();
        await ClearStats(conn, "logminer-doorbell");
        await using var cmd = new SqlCommand(DoorbellSql, conn);
        var lat = new double[n];
        var rows = 0;
        for (var i = 0; i < n; i++)
        {
            var sw = Stopwatch.StartNew();
            await using (var r = await cmd.ExecuteReaderAsync())
                while (await r.ReadAsync()) rows++;
            lat[i] = sw.Elapsed.TotalMilliseconds;
        }
        Report("doorbell", lat, rows / n);
        await ServerCost(conn, "logminer-doorbell");
        return 0;
    }

    public static async Task<int> ReadProbe(string[] args)
    {
        var db = Program.Opt(args, "--db") ?? "tenant_0001";
        var n = int.Parse(Program.Opt(args, "--n") ?? "500", CultureInfo.InvariantCulture);
        await using var conn = new SqlConnection(Program.ConnectionString());
        await conn.OpenAsync();
        await ClearStats(conn, "logminer-read");
        int dbid;
        string endLsn;
        await using (var c = new SqlCommand("SELECT DB_ID(@db), (SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID(@db)))", conn))
        {
            c.Parameters.AddWithValue("@db", db);
            await using var r = await c.ExecuteReaderAsync();
            await r.ReadAsync();
            dbid = r.GetInt16(0);
            endLsn = r.GetString(1);
        }
        Console.WriteLine($"{db}: dbid={dbid} log_end_lsn={endLsn}");

        // Does @end accept log_end_lsn (an LSN that does not exist yet)?
        foreach (var (label, end) in new[] { ("end=NULL", (object)DBNull.Value), ("end=log_end_lsn", "0x" + endLsn) })
        {
            try
            {
                await using var c = new SqlCommand(ReadSql, conn);
                c.Parameters.AddWithValue("@start", "0x" + endLsn);
                c.Parameters.AddWithValue("@end", end);
                c.Parameters.AddWithValue("@dbid", dbid);
                var count = 0;
                await using (var r = await c.ExecuteReaderAsync()) while (await r.ReadAsync()) count++;
                Console.WriteLine($"  {label}: ok, {count} rows from start=log_end_lsn");
            }
            catch (SqlException ex) { Console.WriteLine($"  {label}: error {ex.Number}: {ex.Message}"); }
        }

        // cost of an empty read from the current end of the log
        await using var cmd = new SqlCommand(ReadSql, conn);
        cmd.Parameters.AddWithValue("@start", "0x" + endLsn);
        cmd.Parameters.AddWithValue("@end", DBNull.Value);
        cmd.Parameters.AddWithValue("@dbid", dbid);
        var lat = new double[n];
        for (var i = 0; i < n; i++)
        {
            var sw = Stopwatch.StartNew();
            await using (var r = await cmd.ExecuteReaderAsync()) while (await r.ReadAsync()) { }
            lat[i] = sw.Elapsed.TotalMilliseconds;
        }
        Report("empty read", lat, 0);
        await ServerCost(conn, "logminer-read");
        return 0;
    }

    private static void Report(string what, double[] lat, int rowsPerCall)
    {
        Array.Sort(lat);
        double P(double q) => lat[Math.Min(lat.Length - 1, (int)(q * lat.Length))];
        Console.WriteLine(FormattableString.Invariant($"{what}: n={lat.Length} rows/call={rowsPerCall} client ms p50={P(0.5):F3} p90={P(0.9):F3} p99={P(0.99):F3} max={lat[^1]:F3}"));
    }

    private static async Task ClearStats(SqlConnection conn, string tag)
    {
        await using var c = new SqlCommand($"""
            DECLARE @h varbinary(64);
            DECLARE c CURSOR LOCAL FOR SELECT plan_handle FROM sys.dm_exec_query_stats qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) t WHERE t.text LIKE '%{tag}%' AND t.text NOT LIKE '%dm_exec_query_stats%';
            OPEN c; FETCH NEXT FROM c INTO @h; WHILE @@FETCH_STATUS = 0 BEGIN DBCC FREEPROCCACHE(@h) WITH NO_INFOMSGS; FETCH NEXT FROM c INTO @h; END
            """, conn);
        await c.ExecuteNonQueryAsync();
    }

    private static async Task ServerCost(SqlConnection conn, string tag)
    {
        await using var c = new SqlCommand($"""
            SELECT SUM(qs.execution_count), SUM(qs.total_worker_time) / 1000.0 / SUM(qs.execution_count), SUM(qs.total_elapsed_time) / 1000.0 / SUM(qs.execution_count),
                   SUM(qs.total_logical_reads) * 1.0 / SUM(qs.execution_count)
            FROM sys.dm_exec_query_stats qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) t
            WHERE t.text LIKE '%{tag}%' AND t.text NOT LIKE '%dm_exec_query_stats%'
            """, conn);
        await using var r = await c.ExecuteReaderAsync();
        if (await r.ReadAsync() && !r.IsDBNull(0))
            Console.WriteLine(FormattableString.Invariant($"  server: executions={r.GetInt64(0)} cpu ms/call={r.GetDecimal(1):F4} elapsed ms/call={r.GetDecimal(2):F4} logical reads/call={r.GetDecimal(3):F1}"));
    }
}
