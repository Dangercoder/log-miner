using System.Diagnostics;
using System.Globalization;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// What does the doorbell cost the server and the writers? Alternates rounds without any doorbell, with the push and with polling only (no
/// capture at all), under the same load, and compares throughput, commit latency and the CPU of the whole SQL Server process.
///   lab xecost [--writers 16] [--tps 0 (= as fast as possible)] [--seconds 4] [--rounds 5] [--dispatch-ms 4]
/// </summary>
internal static class XeCost
{
    public static async Task<int> Run(string[] args)
    {
        var writers = int.Parse(Program.Opt(args, "--writers") ?? "16", CultureInfo.InvariantCulture);
        var tps = int.Parse(Program.Opt(args, "--tps") ?? "0", CultureInfo.InvariantCulture);
        var seconds = double.Parse(Program.Opt(args, "--seconds") ?? "4", CultureInfo.InvariantCulture);
        var rounds = int.Parse(Program.Opt(args, "--rounds") ?? "5", CultureInfo.InvariantCulture);
        var dispatch = int.Parse(Program.Opt(args, "--dispatch-ms") ?? "4", CultureInfo.InvariantCulture);
        await using var admin = new SqlConnection(Program.ConnectionString());
        await admin.OpenAsync();
        async Task<long> CpuAsync()
        {
            await using var cmd = new SqlCommand("SELECT SUM(total_cpu_usage_ms) FROM sys.dm_os_schedulers", admin);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        }
        async Task<(double Tps, double P50, double P99, double CpuUsPerTx, double CpuPct)> RoundAsync()
        {
            var latencies = new List<double>[writers];
            var cpu = await CpuAsync();
            var sw = Stopwatch.StartNew();
            await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
            {
                var l = latencies[w] = [];
                await using var conn = new SqlConnection(Program.ConnectionString($"tenant_{100 + w:0000}"));
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", conn);
                var every = tps > 0 ? (double)writers / tps : 0;
                var next = w * every / writers;
                while (sw.Elapsed.TotalSeconds < seconds)
                {
                    if (every > 0)
                    {
                        next += every;
                        var wait = next - sw.Elapsed.TotalSeconds;
                        if (wait > 0) await Task.Delay(TimeSpan.FromSeconds(wait));
                    }
                    var t = Stopwatch.GetTimestamp();
                    await cmd.ExecuteNonQueryAsync();
                    l.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                }
            })));
            var elapsed = sw.Elapsed.TotalSeconds;
            cpu = await CpuAsync() - cpu;
            var all = latencies.SelectMany(x => x).Order().ToArray();
            return (all.Length / elapsed, all[all.Length / 2], all[(int)(all.Length * 0.99)], cpu * 1000.0 / all.Length, cpu / elapsed / 10);
        }
        async Task<(double, double, double, double, double)> WithAsync(string mode)
        {
            if (mode == "nothing") return await RoundAsync();
            using var stop = new CancellationTokenSource();
            var signal = new DoorbellSignal(new DoorbellOptions { ConnectionString = Program.ConnectionString(), Push = mode == "push", DispatchInterval = TimeSpan.FromMilliseconds(dispatch), SessionName = "logminer_lab_cost" });
            var watch = Task.Run(async () => { try { await foreach (var _ in signal.WatchAsync(stop.Token)) { } } catch (OperationCanceledException) { } });
            for (var i = 0; i < 500 && mode == "push" && !signal.Pushing; i++) await Task.Delay(10);
            await Task.Delay(300);
            var r = await RoundAsync();
            if (mode == "push" && !signal.Pushing) Console.WriteLine("push was lost during the round: " + signal.PushProblem);
            await stop.CancelAsync();
            await watch;
            return r;
        }
        var modes = new[] { "nothing", "push", "poll" };
        var results = modes.ToDictionary(m => m, _ => new List<(double Tps, double P50, double P99, double CpuUsPerTx, double CpuPct)>());
        await RoundAsync(); // warm up
        for (var r = 0; r < rounds; r++)
            foreach (var m in r % 2 == 0 ? modes : modes.Reverse())
            {
                results[m].Add(await WithAsync(m));
                await Task.Delay(500);
            }
        static double Median(IEnumerable<double> v) { var a = v.Order().ToArray(); return a[a.Length / 2]; }
        foreach (var m in modes)
        {
            var l = results[m];
            Console.WriteLine(FormattableString.Invariant($"{m,-8} median of {l.Count}: {Median(l.Select(x => x.Tps)):F0} tx/s, commit p50 {Median(l.Select(x => x.P50)):F3} p99 {Median(l.Select(x => x.P99)):F3} ms, SQL Server CPU {Median(l.Select(x => x.CpuPct)):F1}% of one core = {Median(l.Select(x => x.CpuUsPerTx)):F1} us/tx   (tx/s {l.Min(x => x.Tps):F0}..{l.Max(x => x.Tps):F0}, us/tx {l.Min(x => x.CpuUsPerTx):F1}..{l.Max(x => x.CpuUsPerTx):F1})"));
        }
        return 0;
    }
}
