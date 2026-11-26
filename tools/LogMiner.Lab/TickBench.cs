using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>How punctual is a loop "wait 3 ms, run a tiny query" - and is it the wait or the query that is late?   lab tick [--seconds 20] [--ms 3]</summary>
internal static class TickBench
{
    [DllImport("winmm.dll")] private static extern uint timeBeginPeriod(uint ms);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] private static extern IntPtr CreateWaitableTimerExW(IntPtr attributes, string? name, uint flags, uint access);
    [DllImport("kernel32.dll", SetLastError = true)] private static extern bool SetWaitableTimer(IntPtr timer, in long due, int period, IntPtr routine, IntPtr arg, bool resume);
    [DllImport("kernel32.dll")] private static extern uint WaitForSingleObject(IntPtr handle, uint ms);

    [StructLayout(LayoutKind.Sequential)] private struct PowerThrottling { public uint Version, ControlMask, StateMask; }
    [DllImport("kernel32.dll", SetLastError = true)] private static extern bool SetProcessInformation(IntPtr process, int cls, in PowerThrottling info, uint size);

    public static async Task<int> Run(string[] args)
    {
        var seconds = int.Parse(Program.Opt(args, "--seconds") ?? "20", CultureInfo.InvariantCulture);
        var ms = int.Parse(Program.Opt(args, "--ms") ?? "3", CultureInfo.InvariantCulture);
        _ = timeBeginPeriod(1);
        if (args.Contains("--no-throttling"))
        {
            // PROCESS_POWER_THROTTLING_EXECUTION_SPEED (1) | PROCESS_POWER_THROTTLING_IGNORE_TIMER_RESOLUTION (4): both off
            var ok = SetProcessInformation(Process.GetCurrentProcess().Handle, 4, new PowerThrottling { Version = 1, ControlMask = 5, StateMask = 0 }, 12);
            Console.WriteLine($"power throttling off: {ok}");
        }
        await using var conn = new SqlConnection(Program.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = 0x01", conn);

        var (waits, queries) = (new List<double>(), new List<double>());
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed.TotalSeconds < seconds)
        {
            var t = Stopwatch.GetTimestamp();
            await Task.Delay(ms);
            waits.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
            t = Stopwatch.GetTimestamp();
            await cmd.ExecuteScalarAsync();
            queries.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
        }
        Report("Task.Delay, async query", waits, queries);

        void OnThread(string name, Action wait)
        {
            var (w, q) = (new List<double>(), new List<double>());
            var thread = new Thread(() =>
            {
                var until = Stopwatch.StartNew();
                while (until.Elapsed.TotalSeconds < seconds)
                {
                    var t = Stopwatch.GetTimestamp();
                    wait();
                    w.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                    t = Stopwatch.GetTimestamp();
                    cmd.ExecuteScalar();
                    q.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                }
            });
            thread.Start();
            thread.Join();
            Report(name, w, q);
        }
        OnThread("own thread, Thread.Sleep, sync query", () => Thread.Sleep(ms));
        var timer = CreateWaitableTimerExW(IntPtr.Zero, null, 2 /* high resolution */, 0x1F0003);
        OnThread("own thread, high-resolution timer", () =>
        {
            SetWaitableTimer(timer, -ms * 10_000L, 0, IntPtr.Zero, IntPtr.Zero, false);
            _ = WaitForSingleObject(timer, 1000);
        });
        return 0;
    }

    private static void Report(string name, List<double> waits, List<double> queries)
    {
        static string S(List<double> l)
        {
            var a = l.Order().ToArray();
            return FormattableString.Invariant($"p50 {a[a.Length / 2]:F2} p99 {a[(int)(a.Length * 0.99)]:F2} p99.9 {a[(int)(a.Length * 0.999)]:F2} max {a[^1]:F1}, >10 ms: {a.Count(x => x > 10)}");
        }
        Console.WriteLine($"{name}: {waits.Count} rounds; wait ms {S(waits)}; query ms {S(queries)}");
    }
}
