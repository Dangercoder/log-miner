using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace LogMiner.Lab;

/// <summary>
/// The real thing: logminer.exe, the load and the consumer are three processes.
///   lab service [--dbs 1200] [--tps 200] [--seconds 40] [--dispatch-ms 4] [--push true] [--warm-s 45] [--drop-s 10] [--kill-s 20]
/// --drop-s drops the event session that many seconds into the load, --kill-s kills the service and starts it again.
/// </summary>
internal static class ServiceBench
{
    public static async Task<int> Run(string[] args)
    {
        string Arg(string name, string fallback) => Program.Opt(args, name) ?? fallback;
        var (dbs, tps, seconds) = (Arg("--dbs", "1200"), Arg("--tps", "200"), Arg("--seconds", "40"));
        var repo = AppContext.BaseDirectory;
        while (!File.Exists(Path.Combine(repo, "LogMiner.slnx"))) repo = Path.GetDirectoryName(repo.TrimEnd('\\')) ?? throw new DirectoryNotFoundException("LogMiner.slnx");
        var exe = Path.Combine(repo, "src", "LogMiner.Service", "bin", "Release", "net10.0", "logminer.exe");
        var work = Directory.CreateTempSubdirectory("logminer-bench-").FullName;
        await using var nats = new NatsConnection(new NatsOpts { Url = "nats://127.0.0.1:4222", Name = "lab-service-consumer" });
        await nats.ConnectAsync();
        var js = new NatsJSContext(nats);
        try { await js.DeleteStreamAsync("LMBENCH"); } catch (NatsJSApiException) { }

        var start = new ProcessStartInfo(exe) { RedirectStandardOutput = true, UseShellExecute = false, WorkingDirectory = Path.GetDirectoryName(exe)! };
        foreach (var (k, v) in new Dictionary<string, string>
        {
            ["ServerName"] = "bench", ["StartPosition"] = "Latest", ["State__Type"] = "File", ["State__File"] = Path.Combine(work, "checkpoints.lmc"),
            ["Signal__DispatchIntervalMs"] = Arg("--dispatch-ms", "4"), ["Signal__Push"] = Arg("--push", "true"), ["Signal__EventSession"] = "logminer_lab_service",
            ["Routing__DestinationTemplate"] = "bench.{server}.{db}.{schema}.{table}", ["Sink__NatsJetStream__Stream"] = "LMBENCH", ["Sink__NatsJetStream__Subjects__0"] = "bench.>",
            ["HealthLogIntervalSeconds"] = "20",
        }) start.Environment["LogMiner__" + k] = v;
        var service = Process.Start(start)!;
        var sentFile = Path.Combine(work, "sent.txt");
        Process? loader = null;
        try
        {
            var output = new ConcurrentQueue<string>();
            var capturing = new TaskCompletionSource();
            void Read(Process process) => _ = Task.Run(async () =>
            {
                while (await process.StandardOutput.ReadLineAsync() is { } line)
                {
                    output.Enqueue(line);
                    if (line.Contains("health:", StringComparison.Ordinal) || line.Contains("doorbell", StringComparison.Ordinal) || line.Contains("self-test", StringComparison.Ordinal) || line.Contains("fail", StringComparison.OrdinalIgnoreCase)) Console.WriteLine("  service: " + line.Trim());
                    if (line.Contains("capturing ", StringComparison.Ordinal)) capturing.TrySetResult();
                }
            });
            Read(service);
            if (await Task.WhenAny(capturing.Task, Task.Delay(60_000)) != capturing.Task) { Console.WriteLine("service did not start:\n" + string.Join('\n', output)); return 1; }
            await Task.Delay(TimeSpan.FromSeconds(int.Parse(Arg("--warm-s", "45"), CultureInfo.InvariantCulture))); // the Latest catch-up
            loader = Process.Start(new ProcessStartInfo(Environment.ProcessPath!, [.. Environment.ProcessPath!.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase) ? new[] { typeof(ServiceBench).Assembly.Location } : [],
                "load", "--dbs", dbs, "--tps", tps, "--seconds", seconds, "--out", sentFile]) { RedirectStandardInput = true, RedirectStandardOutput = true, UseShellExecute = false })!;
            while (await loader.StandardOutput.ReadLineAsync() is { } line && line != "READY") Console.WriteLine(line);

            using var stop = new CancellationTokenSource();
            var latencies = new ConcurrentDictionary<long, double>();
            var ids = new HashSet<string>();
            var duplicates = 0;
            var consumer = await js.CreateOrderedConsumerAsync("LMBENCH", new NatsJSOrderedConsumerOpts { FilterSubjects = ["bench.>"], DeliverPolicy = ConsumerConfigDeliverPolicy.New });
            var consuming = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: stop.Token))
                    {
                        var now = E2EBench.NowMicros();
                        if (!ids.Add(msg.Headers?["Nats-Msg-Id"].ToString() ?? "")) { duplicates++; continue; }
                        using var doc = JsonDocument.Parse(msg.Data);
                        if (doc.RootElement.GetProperty("after") is { ValueKind: JsonValueKind.Object } after && after.TryGetProperty("client_ts", out var ts) && ts.ValueKind == JsonValueKind.Number)
                            latencies.TryAdd(ts.GetInt64(), (now - ts.GetInt64()) / 1000.0);
                    }
                }
                catch (OperationCanceledException) { }
            });
            await Task.Delay(3000);
            service.Refresh();
            var (cpu, sql) = (service.TotalProcessorTime, await SqlCpuAsync());
            var clock = Stopwatch.StartNew();
            await loader.StandardInput.WriteLineAsync();
            async Task<bool> AtAsync(string option)
            {
                if (Program.Opt(args, option) is not { } at) return false;
                await Task.Delay(TimeSpan.FromSeconds(double.Parse(at, CultureInfo.InvariantCulture)) - clock.Elapsed);
                Console.WriteLine($"  {option} at +{clock.Elapsed.TotalSeconds:F1} s");
                return true;
            }
            var trouble = Task.Run(async () =>
            {
                if (await AtAsync("--drop-s")) await Program.ExecAsync("DROP EVENT SESSION logminer_lab_service ON SERVER;");
                if (!await AtAsync("--kill-s")) return;
                service.Kill();
                await service.WaitForExitAsync();
                service = Process.Start(start)!;
                Read(service);
                cpu = TimeSpan.Zero;
            });
            while (await loader.StandardOutput.ReadLineAsync() is { } line && !line.StartsWith("SENT", StringComparison.Ordinal)) Console.WriteLine(line);
            await trouble;
            service.Refresh();
            var elapsed = clock.Elapsed.TotalSeconds;
            (cpu, sql) = (service.TotalProcessorTime - cpu, await SqlCpuAsync() - sql);
            await loader.WaitForExitAsync();
            var sent = (await File.ReadAllLinesAsync(sentFile)).Select(l => long.Parse(l.Split(' ')[0], CultureInfo.InvariantCulture)).ToHashSet();
            for (var i = 0; i < 75 && sent.Any(s => !latencies.ContainsKey(s)); i++) await Task.Delay(200);
            await stop.CancelAsync();
            await consuming;
            var l = latencies.Where(x => sent.Contains(x.Key)).Select(x => x.Value).Order().ToArray();
            double P(double q) => l.Length == 0 ? double.NaN : l[Math.Min(l.Length - 1, (int)(q * l.Length))];
            Console.WriteLine(FormattableString.Invariant($"dispatch {Arg("--dispatch-ms", "4")} ms, push {Arg("--push", "true")}, {tps} tx/s over {dbs} databases: {l.Length} of {sent.Count} events, {duplicates} duplicates"));
            Console.WriteLine(FormattableString.Invariant($"issue -> consumed ms: p50 {P(0.5):F1} p90 {P(0.9):F1} p99 {P(0.99):F1} p99.9 {P(0.999):F1} max {(l.Length > 0 ? l[^1] : double.NaN):F1}; over 20 ms: {l.Count(x => x > 20)}"));
            Console.WriteLine(FormattableString.Invariant($"logminer.exe: {cpu.TotalSeconds / elapsed * 100:F1}% of one core, {service.WorkingSet64 / 1048576} MB; its SQL sessions: {sql / elapsed / 10:F1}% of one core"));
            return l.Length == sent.Count && duplicates == 0 ? 0 : 1;
        }
        finally
        {
            if (loader is { HasExited: false }) loader.Kill();
            loader?.Dispose();
            if (!service.HasExited) { service.Kill(); await service.WaitForExitAsync(); }
            await Program.ExecAsync("IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'logminer_lab_service') DROP EVENT SESSION logminer_lab_service ON SERVER;");
            try { await js.DeleteStreamAsync("LMBENCH"); } catch (NatsJSApiException) { }
            try { Directory.Delete(work, true); } catch (IOException) { }
        }
    }

    private static async Task<long> SqlCpuAsync()
    {
        await using var conn = new SqlConnection(Program.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("SELECT SUM(CAST(s.cpu_time + ISNULL(r.cpu_time, 0) AS bigint)) FROM sys.dm_exec_sessions s LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id WHERE s.program_name IN ('logminer', 'logminer-state')", conn);
        return await cmd.ExecuteScalarAsync() is long v ? v : 0;
    }
}
