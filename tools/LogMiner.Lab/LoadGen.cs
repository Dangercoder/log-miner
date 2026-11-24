using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// The application, in its own process: inserts into random tenant databases over warm connections (one per database, opened before the
/// start), stamping client_ts = when the application issued the insert. Prints READY, starts on a line from stdin, and writes every
/// "client_ts tenant" to a file.   lab load --out file [--dbs 1200] [--tps 200] [--seconds 40] [--writers 16]
/// </summary>
internal static class LoadGen
{
    [DllImport("winmm.dll")] private static extern uint timeBeginPeriod(uint ms);

    public static async Task<int> Run(string[] args)
    {
        var dbs = int.Parse(Program.Opt(args, "--dbs") ?? "1200", CultureInfo.InvariantCulture);
        var tps = int.Parse(Program.Opt(args, "--tps") ?? "200", CultureInfo.InvariantCulture);
        var seconds = int.Parse(Program.Opt(args, "--seconds") ?? "40", CultureInfo.InvariantCulture);
        var writers = int.Parse(Program.Opt(args, "--writers") ?? "16", CultureInfo.InvariantCulture);
        var outFile = Program.Opt(args, "--out") ?? throw new ArgumentException("--out");
        _ = timeBeginPeriod(1);
        var conns = new SqlConnection[dbs + 1];
        await Parallel.ForEachAsync(Enumerable.Range(1, dbs), new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (db, ct) =>
        {
            conns[db] = new SqlConnection(Program.ConnectionString($"tenant_{db:D4}").Replace("logminer-lab", "logminer-lab-load", StringComparison.Ordinal) + ";Pooling=false");
            await conns[db].OpenAsync(ct);
            // warm the plan and the connection without writing anything
            await using var cmd = Insert(conns[db], 0, 0, 0);
            cmd.CommandText = cmd.CommandText.Replace("VALUES (@c, @a, 1, SYSUTCDATETIME(), @n, @t)", "SELECT @c, @a, 1, SYSUTCDATETIME(), @n, @t WHERE 1 = 0", StringComparison.Ordinal);
            await cmd.ExecuteNonQueryAsync(ct);
        });
        var busy = Enumerable.Range(0, dbs + 1).Select(_ => new SemaphoreSlim(1)).ToArray();
        var sent = new ConcurrentDictionary<long, int>();
        Console.WriteLine("READY");
        Console.ReadLine();
        var start = Stopwatch.StartNew();
        var every = (double)writers / Math.Max(1, tps);
        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            var rng = new Random(w * 7919 + Environment.ProcessId);
            var next = every * w / writers; // the writers do not all start in the same millisecond
            while (true)
            {
                var wait = next - start.Elapsed.TotalSeconds;
                if (wait > 0) await Task.Delay(TimeSpan.FromSeconds(wait));
                if (start.Elapsed.TotalSeconds >= seconds) return;
                next += every;
                var db = rng.Next(1, dbs + 1);
                await busy[db].WaitAsync();
                try
                {
                    var stamp = E2EBench.NowMicros();
                    while (!sent.TryAdd(stamp, db)) stamp++;
                    await using var cmd = Insert(conns[db], rng.Next(1, 1000), rng.Next(1, 100000), stamp);
                    await cmd.ExecuteNonQueryAsync();
                }
                finally { busy[db].Release(); }
            }
        })));
        await File.WriteAllLinesAsync(outFile, sent.Select(s => $"{s.Key} {s.Value}"));
        Console.WriteLine($"SENT {sent.Count}");
        foreach (var c in conns) if (c is not null) await c.DisposeAsync();
        return 0;
    }

    private static SqlCommand Insert(SqlConnection conn, int customer, int cents, long stamp)
    {
        var cmd = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at, note, client_ts) VALUES (@c, @a, 1, SYSUTCDATETIME(), @n, @t);", conn);
        cmd.Parameters.Add("@c", System.Data.SqlDbType.Int).Value = customer;
        cmd.Parameters.Add(new SqlParameter("@a", System.Data.SqlDbType.Decimal) { Precision = 18, Scale = 2, Value = cents / 100m });
        cmd.Parameters.Add("@n", System.Data.SqlDbType.NVarChar, 50).Value = "bench";
        cmd.Parameters.Add("@t", System.Data.SqlDbType.BigInt).Value = stamp;
        return cmd;
    }
}
