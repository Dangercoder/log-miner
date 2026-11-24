using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>Reads SQL Server database counters through PDH (shared memory, no SQL Server CPU).</summary>
internal static partial class PdhBench
{
    private const uint PdhMoreData = 0x800007D2;

    [DllImport("pdh.dll", CharSet = CharSet.Unicode)] private static extern uint PdhOpenQueryW(string? source, IntPtr user, out IntPtr query);
    [DllImport("pdh.dll", CharSet = CharSet.Unicode)] private static extern uint PdhAddEnglishCounterW(IntPtr query, string path, IntPtr user, out IntPtr counter);
    [DllImport("pdh.dll")] private static extern uint PdhCollectQueryData(IntPtr query);
    [DllImport("pdh.dll", CharSet = CharSet.Unicode)] private static extern uint PdhGetRawCounterArrayW(IntPtr counter, ref uint bufferSize, out uint itemCount, IntPtr buffer);
    [DllImport("pdh.dll")] private static extern uint PdhCloseQuery(IntPtr query);

    // PDH_RAW_COUNTER_ITEM_W: LPWSTR szName (8) + PDH_RAW_COUNTER { DWORD CStatus; FILETIME TimeStamp; LONGLONG FirstValue; LONGLONG SecondValue; DWORD MultiCount; } (40)
    private const int ItemSize = 48, FirstValueOffset = 8 + 16;

    public static async Task<int> Run(string[] args)
    {
        var counter = Program.Opt(args, "--counter") ?? "Repl. Pending Xacts";
        var n = int.Parse(Program.Opt(args, "--n") ?? "2000", CultureInfo.InvariantCulture);
        var path = $@"\SQLServer:Databases(*)\{counter}";
        Check(PdhOpenQueryW(null, IntPtr.Zero, out var query), "open");
        Check(PdhAddEnglishCounterW(query, path, IntPtr.Zero, out var handle), "add " + path);
        var buffer = IntPtr.Zero;
        uint size = 0;
        Dictionary<string, long> Read()
        {
            Check(PdhCollectQueryData(query), "collect");
            uint count;
            var st = PdhGetRawCounterArrayW(handle, ref size, out count, buffer);
            if (st == PdhMoreData)
            {
                if (buffer != IntPtr.Zero) Marshal.FreeHGlobal(buffer);
                buffer = Marshal.AllocHGlobal((int)size);
                st = PdhGetRawCounterArrayW(handle, ref size, out count, buffer);
            }
            Check(st, "raw array");
            var result = new Dictionary<string, long>((int)count);
            for (var i = 0; i < count; i++)
            {
                var item = buffer + i * ItemSize;
                var name = Marshal.PtrToStringUni(Marshal.ReadIntPtr(item)) ?? "";
                result[name] = Marshal.ReadInt64(item + FirstValueOffset);
            }
            return result;
        }

        var first = Read();
        Console.WriteLine($"PDH {path}: {first.Count} instances; tenant_0007={first.GetValueOrDefault("tenant_0007")}");
        var lat = new double[n];
        var cpu0 = Process.GetCurrentProcess().TotalProcessorTime;
        var wall = Stopwatch.StartNew();
        for (var i = 0; i < n; i++)
        {
            var sw = Stopwatch.StartNew();
            Read();
            lat[i] = sw.Elapsed.TotalMilliseconds;
        }
        var cpu = (Process.GetCurrentProcess().TotalProcessorTime - cpu0).TotalMilliseconds;
        Array.Sort(lat);
        Console.WriteLine(FormattableString.Invariant($"PDH collect+read: n={n} ms p50={lat[n / 2]:F3} p99={lat[(int)(n * 0.99)]:F3} max={lat[^1]:F3}; our CPU {cpu / n:F3} ms/poll over {wall.Elapsed.TotalSeconds:F1}s"));

        // liveness: commit, then poll PDH until the counter for that database moves
        await using var writer = new SqlConnection(Program.ConnectionString("tenant_0007"));
        await writer.OpenAsync();
        await using var write = new SqlCommand("INSERT dbo.orders (customer_id, amount, status, created_at) VALUES (1, 9.99, 1, SYSUTCDATETIME())", writer);
        for (var round = 0; round < 5; round++)
        {
            var before = Read().GetValueOrDefault("tenant_0007");
            var sw = Stopwatch.StartNew();
            await write.ExecuteNonQueryAsync();
            var committed = sw.Elapsed.TotalMilliseconds;
            long now; var polls = 0;
            do { now = Read().GetValueOrDefault("tenant_0007"); polls++; } while (now == before && sw.ElapsedMilliseconds < 3000);
            Console.WriteLine(FormattableString.Invariant($"  {before} -> {now}: commit returned {committed:F2} ms, PDH saw it at {sw.Elapsed.TotalMilliseconds:F2} ms ({polls} polls)"));
        }
        _ = PdhCloseQuery(query);
        if (buffer != IntPtr.Zero) Marshal.FreeHGlobal(buffer);
        return 0;
    }

    private static void Check(uint status, string what)
    {
        if (status != 0) throw new InvalidOperationException($"PDH {what} failed: 0x{status:X8}");
    }
}
