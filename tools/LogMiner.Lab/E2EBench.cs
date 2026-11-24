using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Nats;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace LogMiner.Lab;

/// <summary>
/// End-to-end latency: a load generator inserts into random tenant databases (stamping client_ts = commit-return time in
/// microseconds), the miner captures and publishes to JetStream, and a JetStream consumer reads the events back from the
/// stream. Latency = consume time - client commit time, on one machine and one clock.
///   lab e2e [--dbs 1200] [--tps 200] [--seconds 60] [--writers 16] [--poll-ms 20] [--workers 8]
/// </summary>
internal static class E2EBench
{
    public static async Task<int> Run(string[] args)
    {
        var dbs = int.Parse(Program.Opt(args, "--dbs") ?? "1200", CultureInfo.InvariantCulture);
        var tps = int.Parse(Program.Opt(args, "--tps") ?? "200", CultureInfo.InvariantCulture);
        var seconds = int.Parse(Program.Opt(args, "--seconds") ?? "60", CultureInfo.InvariantCulture);
        var writers = int.Parse(Program.Opt(args, "--writers") ?? "16", CultureInfo.InvariantCulture);
        var pollMs = int.Parse(Program.Opt(args, "--poll-ms") ?? "20", CultureInfo.InvariantCulture);
        var workers = int.Parse(Program.Opt(args, "--workers") ?? "8", CultureInfo.InvariantCulture);
        var natsUrl = Program.Opt(args, "--nats") ?? "nats://127.0.0.1:4222";
        var sweepSeconds = int.Parse(Program.Opt(args, "--sweep-s") ?? "30", CultureInfo.InvariantCulture);
        var releaseSeconds = int.Parse(Program.Opt(args, "--release-s") ?? "5", CultureInfo.InvariantCulture);
        var cs = Program.ConnectionString().Replace("Application Name=logminer-lab", "Application Name=logminer-miner", StringComparison.Ordinal);
        var stream = "LMBENCH";

        // 1. JetStream consumer of the stream (fresh stream each run)
        await using var nats = new NatsConnection(new NatsOpts { Url = natsUrl, Name = "lab-e2e-consumer" });
        await nats.ConnectAsync();
        var js = new NatsJSContext(nats);
        try { await js.DeleteStreamAsync(stream); } catch (NatsJSApiException) { }
        var sinkOpts = new JetStreamSinkOptions { Url = natsUrl, StreamName = stream, StreamSubjects = ["bench.>"], DuplicateWindow = TimeSpan.FromMinutes(2) };
        var latencies = new ConcurrentBag<double>();
        var commitPart = new ConcurrentBag<double>();   // issue -> commit record time
        var capturePart = new ConcurrentBag<double>();  // commit -> event encoded (signal + queue + read + decode)
        var deliveryPart = new ConcurrentBag<double>(); // encoded -> consumed (publish, JetStream, consumer)
        var slowest = new ConcurrentBag<(double Ms, long IssuedUs, string Db, double Issue, double Capture, double Delivery)>();
        var received = 0L;
        var duplicateIds = new ConcurrentDictionary<string, byte>();
        var receivedTs = new ConcurrentDictionary<long, byte>();
        var sentTs = new ConcurrentDictionary<long, int>(); // client_ts -> tenant number
        var dupes = 0L;
        using var stop = new CancellationTokenSource();

        // 2. the miner
        ISink sink = Program.Opt(args, "--sink") == "null" ? new MemorySink() : new JetStreamSink(sinkOpts);
        var encoder = new EventEncoder(new EncoderOptions { ServerName = "bench", DestinationTemplate = "bench.{server}.{db}.{schema}.{table}" });
        var checkpoints = new MemoryCheckpointStore();
        var hotMs = int.Parse(Program.Opt(args, "--hot-ms") ?? "5", CultureInfo.InvariantCulture);
        var pushMs = int.Parse(Program.Opt(args, "--push-ms") ?? "0", CultureInfo.InvariantCulture); // 0 = poll only
        var signal = new DoorbellSignal(new DoorbellOptions
        {
            ConnectionString = cs, Push = pushMs > 0, DispatchInterval = TimeSpan.FromMilliseconds(Math.Max(1, pushMs)), SessionName = "logminer_lab_e2e",
            FullPollInterval = TimeSpan.FromMilliseconds(pollMs), HotPollInterval = TimeSpan.FromMilliseconds(hotMs),
        });
        var engine = new CaptureEngine(new CaptureOptions
        {
            ConnectionString = cs,
            Workers = workers,
            Settings = new CaptureSettings { StartPosition = StartPosition.Latest },
            DatabaseFilter = name => name.StartsWith("tenant_", StringComparison.Ordinal) && int.Parse(name[7..], CultureInfo.InvariantCulture) <= dbs,
            ReleaseInterval = TimeSpan.FromSeconds(Math.Max(1, releaseSeconds)),
            ReleaseLog = releaseSeconds > 0,
            SweepInterval = TimeSpan.FromSeconds(sweepSeconds),
            HoldCheckInterval = TimeSpan.FromSeconds(int.Parse(Program.Opt(args, "--hold-s") ?? "60", CultureInfo.InvariantCulture)),
        }, signal, encoder, sink, checkpoints);
        Console.WriteLine($"starting miner over {dbs} databases (poll {pollMs} ms, push {pushMs} ms, {workers} workers)...");
        var sw = Stopwatch.StartNew();
        await engine.StartAsync(stop.Token);
        // let the initial "Latest" catch-up finish
        while (engine.Captures.Count < dbs || engine.Captures.Any(c => (!c.Live || c.HoldingForBaseline) && c.State != CaptureState.Faulted))
        {
            if (sw.Elapsed > TimeSpan.FromMinutes(3)) break;
            await Task.Delay(500);
        }
        Console.WriteLine($"miner ready after {sw.Elapsed.TotalSeconds:F1}s: {engine.Captures.Count} captures, {engine.Captures.Count(c => c.State == CaptureState.Faulted)} faulted");

        var consumer = await js.CreateOrderedConsumerAsync(stream, new NatsJSOrderedConsumerOpts { FilterSubjects = ["bench.>"], DeliverPolicy = ConsumerConfigDeliverPolicy.New });
        var consumeTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: stop.Token))
                {
                    var now = NowMicros();
                    var id = msg.Headers?["Nats-Msg-Id"].ToString() ?? "";
                    if (!duplicateIds.TryAdd(id, 0)) { Interlocked.Increment(ref dupes); continue; }
                    using var doc = JsonDocument.Parse(msg.Data);
                    if (doc.RootElement.GetProperty("after") is { ValueKind: JsonValueKind.Object } after
                        && after.TryGetProperty("client_ts", out var ts) && ts.ValueKind == JsonValueKind.Number)
                    {
                        receivedTs.TryAdd(ts.GetInt64(), 0);
                        latencies.Add((now - ts.GetInt64()) / 1000.0);
                        Interlocked.Increment(ref received);
                        var src = doc.RootElement.GetProperty("source");
                        if (src.TryGetProperty("commit_time", out var ct) && DateTime.TryParse(ct.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var commit))
                        {
                            var commitUs = (commit.Ticks - DateTime.UnixEpoch.Ticks) / 10;
                            var encodedUs = doc.RootElement.GetProperty("ts_ms").GetInt64() * 1000;
                            commitPart.Add((commitUs - ts.GetInt64()) / 1000.0);
                            capturePart.Add((encodedUs - commitUs) / 1000.0);
                            deliveryPart.Add((now - encodedUs) / 1000.0);
                            if (now - ts.GetInt64() > 15_000)
                                slowest.Add(((now - ts.GetInt64()) / 1000.0, ts.GetInt64(), src.GetProperty("db").GetString()!, (commitUs - ts.GetInt64()) / 1000.0, (encodedUs - commitUs) / 1000.0, (now - encodedUs) / 1000.0));
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
        });

        // 4. load, in its own process (the application's threads and connections are not the miner's)
        var sentFile = Path.GetTempFileName();
        using var loader = Process.Start(new ProcessStartInfo(Environment.ProcessPath!, [.. LoadArgs(), "--out", sentFile]) { RedirectStandardInput = true, RedirectStandardOutput = true, UseShellExecute = false })!;
        string[] LoadArgs() => [.. (Environment.ProcessPath!.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase) ? new[] { typeof(E2EBench).Assembly.Location } : []), "load", "--dbs", dbs.ToString(CultureInfo.InvariantCulture),
            "--tps", tps.ToString(CultureInfo.InvariantCulture), "--seconds", seconds.ToString(CultureInfo.InvariantCulture), "--writers", writers.ToString(CultureInfo.InvariantCulture)];
        while (await loader.StandardOutput.ReadLineAsync() is { } line && line != "READY") Console.WriteLine(line);
        await Task.Delay(2000);
        while (engine.Samples.TryDequeue(out _)) { } // the start-up is over: only the load's pumps are of interest
        var cpuBefore = await StatementCpuAsync(cs);
        var loadStartUs = NowMicros();
        // what else stops this process (GC), its threads (scheduling) or the server for a while?
        var stalls = new ConcurrentBag<(string What, double At, double Ms)>();
        var gcPause = GC.GetTotalPauseDuration();
        var (gen0, gen1, gen2) = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
        void Watch(string what, Action step, double allowedMs)
        {
            var thread = new Thread(() =>
            {
                var t = Stopwatch.GetTimestamp();
                var pause = GC.GetTotalPauseDuration();
                while (!stop.IsCancellationRequested)
                {
                    step();
                    var now = Stopwatch.GetTimestamp();
                    var ms = Stopwatch.GetElapsedTime(t, now).TotalMilliseconds;
                    var p = GC.GetTotalPauseDuration();
                    if (ms > allowedMs) stalls.Add(($"{what} (GC paused {(p - pause).TotalMilliseconds:F1} ms of it)", (NowMicros() - loadStartUs) / 1e6, ms));
                    (t, pause) = (now, p);
                }
            }) { IsBackground = true };
            thread.Start();
        }
        if (args.Contains("--probes"))
        {
            Watch("thread", () => Thread.Sleep(1), 8);
            Watch("thread pool", () => Task.Run(() => { }).Wait(), 8);
            var probe = new SqlConnection(Program.ConnectionString());
            await probe.OpenAsync();
            var probeCmd = new SqlCommand("SELECT 1", probe);
            Watch("SQL Server round trip", () => { probeCmd.ExecuteScalar(); Thread.Sleep(1); }, 8);
        }
        while (engine.Samples.TryDequeue(out _)) { } // the start-up is over: only the load's pumps are of interest
        var minerCpu = Process.GetCurrentProcess().TotalProcessorTime;
        Console.WriteLine($"load: {tps} inserts/s for {seconds}s from {writers} writers into {dbs} databases");
        await loader.StandardInput.WriteLineAsync();
        while (await loader.StandardOutput.ReadLineAsync() is { } line && !line.StartsWith("SENT", StringComparison.Ordinal)) Console.WriteLine(line);
        minerCpu = Process.GetCurrentProcess().TotalProcessorTime - minerCpu;
        await loader.WaitForExitAsync();
        foreach (var line in await File.ReadAllLinesAsync(sentFile))
        {
            var parts = line.Split(' ');
            sentTs[long.Parse(parts[0], CultureInfo.InvariantCulture)] = int.Parse(parts[1], CultureInfo.InvariantCulture);
        }
        File.Delete(sentFile);
        long sent = sentTs.Count;
        Console.WriteLine($"load done: {sent} transactions; this process (miner and consumer) used {minerCpu.TotalMilliseconds / seconds / 10:F1}% of one core; draining...");
        var drainUntil = DateTime.UtcNow.AddSeconds(15);
        while (Interlocked.Read(ref received) < sent && DateTime.UtcNow < drainUntil) await Task.Delay(200);
        var cpuAfter = await StatementCpuAsync(cs);
        var faulted = engine.Captures.Count(c => c.State == CaptureState.Faulted);
        var missing = sentTs.Where(kv => !receivedTs.ContainsKey(kv.Key)).OrderBy(kv => kv.Key).ToList();
        if (missing.Count > 0)
        {
            Console.WriteLine($"MISSING {missing.Count} events:");
            foreach (var (ts, db) in missing.Take(10))
            {
                var cap = engine.Captures.First(c => c.Name == $"tenant_{db:D4}");
                await using var mc = new SqlConnection(Program.ConnectionString($"tenant_{db:D4}"));
                await mc.OpenAsync();
                await using var q = new SqlCommand("SELECT COUNT(*) FROM dbo.orders WHERE client_ts = @t", mc);
                q.Parameters.AddWithValue("@t", ts);
                var exists = (int)(await q.ExecuteScalarAsync())!;
                var issued = DateTime.UnixEpoch.AddTicks(ts * 10);
                Console.WriteLine($"  tenant_{db:D4} client_ts={ts} issued={issued:HH:mm:ss.fff} rowInTable={exists} capture: live={cap.Live} events={cap.EventsPublished} state={cap.State} fault={cap.Fault}");
            }
        }

        var l = latencies.ToArray();
        Array.Sort(l);
        double P(double q) => l.Length == 0 ? double.NaN : l[Math.Min(l.Length - 1, (int)(q * l.Length))];
        Console.WriteLine(FormattableString.Invariant($"events received: {received} (expected {sent}), duplicates dropped by consumer: {dupes}"));
        Console.WriteLine(FormattableString.Invariant($"issue -> consumed latency ms: p50={P(0.50):F1} p90={P(0.90):F1} p99={P(0.99):F1} p99.9={P(0.999):F1} max={(l.Length > 0 ? l[^1] : double.NaN):F1}"));
        foreach (var (name, bag) in new[] { ("  issue -> commit  ", commitPart), ("  commit -> encoded", capturePart), ("  encoded -> consumed", deliveryPart) })
        {
            var a = bag.ToArray(); Array.Sort(a);
            double Q(double q) => a.Length == 0 ? double.NaN : a[Math.Min(a.Length - 1, (int)(q * a.Length))];
            Console.WriteLine(FormattableString.Invariant($"{name} ms: p50={Q(0.5):F1} p90={Q(0.9):F1} p99={Q(0.99):F1} max={(a.Length > 0 ? a[^1] : double.NaN):F1}  (ms-resolution timestamps)"));
        }
        Console.WriteLine(FormattableString.Invariant($"this process during the load: GC paused {(GC.GetTotalPauseDuration() - gcPause).TotalMilliseconds:F0} ms, {GC.CollectionCount(0) - gen0}/{GC.CollectionCount(1) - gen1}/{GC.CollectionCount(2) - gen2} collections, {Process.GetCurrentProcess().Threads.Count} threads"));
        foreach (var st in stalls.Where(s => s.At >= 0).OrderBy(s => s.At).Take(80)) Console.WriteLine(FormattableString.Invariant($"    stall at +{st.At:F3} s: {st.What} {st.Ms:F1} ms"));
        Console.WriteLine($"slower than 15 ms: {slowest.Count}");
        foreach (var s in slowest.OrderByDescending(s => s.Ms).Take(int.Parse(Program.Opt(args, "--slow") ?? "25", CultureInfo.InvariantCulture)))
            Console.WriteLine(FormattableString.Invariant($"    {s.Ms:F1} ms at +{(s.IssuedUs - loadStartUs) / 1e6:F3} s in {s.Db}: issue->commit {s.Issue:F1}, commit->encoded {s.Capture:F1}, encoded->consumed {s.Delivery:F1}"));
        foreach (var (k, before) in cpuBefore)
        {
            var after = cpuAfter.GetValueOrDefault(k);
            Console.WriteLine(FormattableString.Invariant($"SQL Server CPU for {k}: {(after.Cpu - before.Cpu) / 1000.0:F0} ms over {seconds}s of load = {(after.Cpu - before.Cpu) / 1000.0 / seconds / 10:F1}% of one core ({after.Count} sessions)"));
        }
        var samples = engine.Samples.ToArray();
        foreach (var (name, values) in new[] { ("queue wait", samples.Select(x => x.QueueMs).ToArray()), ("pump duration", samples.Select(x => x.PumpMs).ToArray()) })
        {
            Array.Sort(values);
            double R(double q) => values.Length == 0 ? double.NaN : values[Math.Min(values.Length - 1, (int)(q * values.Length))];
            Console.WriteLine(FormattableString.Invariant($"  {name} ms over {values.Length} pumps: p50={R(0.5):F2} p90={R(0.9):F2} p99={R(0.99):F2} max={(values.Length > 0 ? values[^1] : 0):F1}"));
        }
        var slow = samples.Where(x => x.PumpMs > 30).ToArray();
        Console.WriteLine(FormattableString.Invariant($"  slow pumps (>30 ms): {slow.Length}; with schema load: {slow.Count(x => x.SchemaLoads > 0)}; rows p50={(slow.Length > 0 ? slow.Select(x => x.Rows).Order().ElementAt(slow.Length / 2) : 0)}; distinct dbs: {slow.Select(x => x.Db).Distinct().Count()}"));
        foreach (var x in slow.Take(5)) Console.WriteLine(FormattableString.Invariant($"    {x.Db}: {x.PumpMs:F0} ms, {x.Rows} rows, schema loads {x.SchemaLoads}"));
        Console.WriteLine($"faulted captures: {faulted}; full polls: {signal.FullPolls}, hot polls: {signal.HotPolls}, signal failures: {engine.SignalFailures}; pushing: {signal.Pushing}, {signal.Pushed} flushes pushed, {signal.FoundByAudit} found by audit, {signal.PushFailures} push failures ({signal.PushProblem}), {engine.BellRetries} bell retries");
        await stop.CancelAsync();
        try { await engine.DisposeAsync(); await sink.DisposeAsync(); await consumeTask; }
        catch (Exception ex) { Console.WriteLine($"shutdown: {ex.GetType().Name}: {ex.Message}"); }
        return 0;
    }

    internal static long NowMicros() => (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) / 10;

    private static async Task<Dictionary<string, (long Cpu, long Count)>> StatementCpuAsync(string cs)
    {
        // CPU of the miner's own sessions (reads, signal, schema loads, sp_repldone); microseconds to match the report below
        var result = new Dictionary<string, (long, long)>();
        await using var conn = new SqlConnection(Program.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("SELECT CAST(SUM(s.cpu_time + ISNULL(r.cpu_time, 0)) AS bigint) * 1000, COUNT(*) FROM sys.dm_exec_sessions s LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id WHERE s.program_name = 'logminer-miner'", conn);
        await using var r = await cmd.ExecuteReaderAsync();
        if (await r.ReadAsync() && !r.IsDBNull(0)) result["miner sessions"] = (r.GetInt64(0), r.GetInt32(1));
        else result["miner sessions"] = (0, 0);
        return result;
    }
}
