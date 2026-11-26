using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using LogMiner.Tests;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace LogMiner.Verify.Tests;

/// <summary>The real logminer-verify.exe over a stream in a real nats-server. Skipped (passes trivially) when no nats-server is found
/// (LOGMINER_NATS_SERVER, or ~/tools/nats-server).</summary>
#pragma warning disable CA1001
public sealed class ConsoleTests : IDisposable
{
    private static readonly string? Exe = Environment.GetEnvironmentVariable("LOGMINER_NATS_SERVER")
        ?? (Directory.Exists(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server"))
            ? Directory.GetFiles(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server"), "nats-server*.exe", SearchOption.AllDirectories).FirstOrDefault() : null);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lm-verify-" + Guid.NewGuid().ToString("N"));
    private readonly int _port = FreePort();
    private Process? _server;

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    private string Url => $"nats://127.0.0.1:{_port}";

    private async Task StartServerAsync()
    {
        Directory.CreateDirectory(_dir);
        _server = Process.Start(new ProcessStartInfo(Exe!, $"-a 127.0.0.1 -p {_port} -js -sd \"{_dir}\"") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true })!;
        _server.BeginErrorReadLine(); _server.BeginOutputReadLine();
        for (var i = 0; i < 200; i++)
        {
            try { using var c = new TcpClient(); await c.ConnectAsync(IPAddress.Loopback, _port); return; }
            catch (SocketException) { await Task.Delay(25); }
        }
        throw new TimeoutException("nats-server did not start");
    }

    public void Dispose()
    {
        if (_server is not null) { _server.Kill(); _server.WaitForExit(); _server.Dispose(); }
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private static async Task<(int ExitCode, string Output, string Error)> RunAsync(params string[] args)
    {
        var info = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "logminer-verify.exe")) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true, StandardOutputEncoding = Encoding.UTF8 };
        foreach (var a in args) info.ArgumentList.Add(a);
        using var p = Process.Start(info)!;
        var (output, error) = (p.StandardOutput.ReadToEndAsync(), p.StandardError.ReadToEndAsync());
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(60));
        return (p.ExitCode, await output, await error);
    }

    /// <summary>A history of some size, the same every time: tables loaded, changed, loaded again, with everything the generator does.</summary>
    private static (Sim Sim, int Loads) History()
    {
        var random = new Random(20261125);
        var sim = new Sim(new Tape([.. Enumerable.Range(0, 3000).Select(_ => random.Next(1_000_000))]));
        sim.Run();
        sim.Settle();
        return (sim, sim.Settle());
    }

    [Fact]
    public async Task A_clean_stream_is_verified_and_one_with_a_message_deleted_is_not()
    {
        if (Exe is null) return;
        await StartServerAsync();
        var (sim, loads) = History();
        var stream = sim.Out;
        // a change whose loss anybody can see, by the oracle
        var lost = Enumerable.Range(0, loads).Last(i =>
        {
            if (!stream[i].IsChange || !stream[i].Table.Keyed || stream.Count(e => e.Id == stream[i].Id) > 1) return false;
            var oracle = new Oracle();
            for (var j = 0; j < loads; j++) if (j != i) oracle.Add(stream[j]);
            oracle.CompareWithTables(sim.Dbs.SelectMany(d => d.Tables));
            return oracle.Seen is not null;
        });

        var sequences = new ulong[stream.Count];
        await using (var nats = new NatsConnection(new NatsOpts { Url = Url, HeaderEncoding = Encoding.UTF8 }))
        {
            var js = new NatsJSContext(nats);
            // what is delivered twice is stored twice, as when it comes again after the server's duplicate window
            await js.CreateStreamAsync(new StreamConfig("CDC", ["cdc.>", "other.>"]) { DuplicateWindow = TimeSpan.FromMilliseconds(100) });
            await js.PublishAsync("other.thing", "not for the verifier"u8.ToArray());
            var seen = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            for (var i = 0; i < stream.Count; i++)
            {
                if (seen.TryGetValue(stream[i].Id, out var at) && DateTime.UtcNow - at < TimeSpan.FromMilliseconds(400)) await Task.Delay(400);
                var ack = await js.PublishAsync(stream[i].Subject, stream[i].Payload, headers: new NatsHeaders { ["Nats-Msg-Id"] = stream[i].Id, ["lm-op"] = stream[i].Op.ToString() });
                ack.EnsureSuccess();
                Assert.False(ack.Duplicate, $"message {i} was dropped as a duplicate");
                (sequences[i], seen[stream[i].Id]) = (ack.Seq, DateTime.UtcNow);
            }
        }

        var clean = await RunAsync("--url", Url, "--stream", "CDC");
        Assert.True(clean.ExitCode == 0, clean.Output + clean.Error);
        // a table without a key cannot be verified, and the program says so instead of "verified"
        var keyless = stream.Any(e => e.Op != 'l' && !e.Table.Keyed);
        Assert.Contains(keyless ? "RESULT: no difference; not every table is verified" : "RESULT: verified", clean.Output, StringComparison.Ordinal);
        Assert.Equal(keyless, clean.Output.Contains("cannot be verified - ", StringComparison.Ordinal));
        Assert.Contains("baseline: first complete load at stream sequence", clean.Output, StringComparison.Ordinal);
        Assert.Contains($"{stream.Count} messages", clean.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("DIFFERENT;", clean.Output.Replace("0 DIFFERENT;", "", StringComparison.Ordinal), StringComparison.Ordinal);

        var json = await RunAsync("--url", Url, "--stream", "CDC", "--subject", "cdc.>", "--json", "--strict");
        Assert.True(json.ExitCode == (keyless ? 3 : 0), json.Output + json.Error);
        using (var doc = JsonDocument.Parse(json.Output))
        {
            Assert.Equal((true, stream.Count, 0), (doc.RootElement.GetProperty("clean").GetBoolean(), doc.RootElement.GetProperty("messages").GetInt32(), doc.RootElement.GetProperty("differences").GetInt32()));
            Assert.Equal(sim.Dbs.Sum(d => d.Tables.Count), doc.RootElement.GetProperty("tables").GetArrayLength());
        }

        // only one database's subjects: the others are not looked at
        var one = await RunAsync("--url", Url, "--stream", "CDC", "--subject", sim.Out[0].Subject, "--json");
        using (var doc = JsonDocument.Parse(one.Output)) Assert.Equal((0, 1), (one.ExitCode, doc.RootElement.GetProperty("tables").GetArrayLength()));

        // nothing was written: the stream is as it was, and no consumer is left
        await using (var nats = new NatsConnection(new NatsOpts { Url = Url }))
        {
            var js = new NatsJSContext(nats);
            var info = (await js.GetStreamAsync("CDC")).Info;
            Assert.Equal(((long)stream.Count + 1, 0L), (info.State.Messages, info.State.ConsumerCount));
            Assert.True((await js.DeleteMessageAsync("CDC", new StreamMsgDeleteRequest { Seq = sequences[lost] })).Success);
        }

        var damaged = await RunAsync("--url", Url, "--stream", "CDC");
        Assert.True(damaged.ExitCode == 1, $"deleted: {stream[lost]}\n{damaged.Output}{damaged.Error}");
        Assert.Contains("RESULT: DIFFERENT", damaged.Output, StringComparison.Ordinal);
        Assert.Contains("1 message(s) inside the stream are gone", damaged.Output, StringComparison.Ordinal);
        Assert.Contains($"{stream[lost].Table.Db.Server}/{stream[lost].Table.Db.Name}/{stream[lost].Table.Schema}.{stream[lost].Table.Name}: DIFFERENT", damaged.Output, StringComparison.Ordinal);
    }

    /// <summary>How fast the program reads a stream from a server on this machine: the number goes to perf-console.txt beside the tests.</summary>
    [Fact]
    public async Task A_stream_of_a_hundred_thousand_messages_is_read_to_its_end()
    {
        if (Exe is null) return;
        await StartServerAsync();
        const int Rows = 50_000;
        static string Image(int id, int version) => $"{{\"id\":{id},\"customer\":{id % 5021},\"status\":\"{(version == 0 ? "open" : "shipped")}\",\"amount\":\"{id % 1000}.{version:D2}\"}}";
        static string Source(string rest) => $"\"source\":{{\"server\":\"srv\",\"db\":\"db\",\"incarnation\":\"06E4\",\"schema\":\"dbo\",\"table\":\"orders\",\"lsn\":\"0000002A:00000001:0001\"{rest}}}";
        await using (var nats = new NatsConnection(new NatsOpts { Url = Url }))
        {
            var js = new NatsJSContext(nats);
            await js.CreateStreamAsync(new StreamConfig("CDC", ["cdc.>"]));
            var acks = new List<Task>();
            async Task SendAsync(string json)
            {
                acks.Add(js.PublishAsync("cdc.srv.db.dbo.orders", Encoding.UTF8.GetBytes(json)).AsTask());
                if (acks.Count == 500) { await Task.WhenAll(acks); acks.Clear(); }
            }
            // a load, every row changed once, and a second load that is compared with all of it
            await SendAsync($"{{\"op\":\"l\",\"key\":null,\"before\":null,\"after\":null,\"load\":{{\"id\":\"L1\",\"phase\":\"begin\"}},{Source(",\"snapshot\":\"L1\"")}}}");
            for (var i = 0; i < Rows; i++) await SendAsync($"{{\"op\":\"r\",\"key\":{{\"id\":{i}}},\"before\":null,\"after\":{Image(i, 0)},{Source(",\"snapshot\":\"L1\"")}}}");
            await SendAsync($"{{\"op\":\"l\",\"key\":null,\"before\":null,\"after\":null,\"load\":{{\"id\":\"L1\",\"phase\":\"end\",\"rows\":{Rows}}},{Source(",\"snapshot\":\"L1\"")}}}");
            for (var i = 0; i < Rows; i++) await SendAsync($"{{\"op\":\"u\",\"key\":{{\"id\":{i}}},\"before\":{Image(i, 0)},\"after\":{Image(i, 1)},{Source($",\"commit_lsn\":\"0000002A:{i + 16:X8}:0001\",\"seq\":0")}}}");
            await SendAsync($"{{\"op\":\"l\",\"key\":null,\"before\":null,\"after\":null,\"load\":{{\"id\":\"L2\",\"phase\":\"begin\"}},{Source(",\"snapshot\":\"L2\"")}}}");
            for (var i = 0; i < Rows; i++) await SendAsync($"{{\"op\":\"r\",\"key\":{{\"id\":{i}}},\"before\":null,\"after\":{Image(i, 1)},{Source(",\"snapshot\":\"L2\"")}}}");
            await SendAsync($"{{\"op\":\"l\",\"key\":null,\"before\":null,\"after\":null,\"load\":{{\"id\":\"L2\",\"phase\":\"end\",\"rows\":{Rows}}},{Source(",\"snapshot\":\"L2\"")}}}");
            await Task.WhenAll(acks);
        }
        var clock = Stopwatch.StartNew();
        var run = await RunAsync("--url", Url, "--stream", "CDC");
        var seconds = clock.Elapsed.TotalSeconds;
        Assert.True(run.ExitCode == 0, run.Output + run.Error);
        Assert.Contains("RESULT: verified", run.Output, StringComparison.Ordinal);
        Assert.Contains($"{3 * Rows + 4} messages", run.Output, StringComparison.Ordinal);
        Assert.Contains($"rows in state {Rows} ", run.Output, StringComparison.Ordinal);
        File.WriteAllText(Path.Combine(AppContext.BaseDirectory, "perf-console.txt"), FormattableString.Invariant($"{3 * Rows + 4} messages from a local nats-server in {seconds:F1} s, start of the process included: {(3 * Rows + 4) / seconds:F0} messages/s"));
    }

    [Fact]
    public async Task Usage_and_connection_problems_are_exit_code_2()
    {
        Assert.Equal(2, (await RunAsync()).ExitCode);
        Assert.Equal(2, (await RunAsync("--stream", "CDC", "--what")).ExitCode);
        Assert.Equal(2, (await RunAsync("--stream", "CDC", "--creds", Path.Combine(_dir, "none.creds"))).ExitCode);
        Assert.Equal(0, (await RunAsync("--help")).ExitCode);
        if (Exe is null) return;
        // nobody listens on this port
        var nobody = await RunAsync("--url", Url, "--stream", "CDC");
        Assert.True(nobody.ExitCode == 2, nobody.Output + nobody.Error);
        await StartServerAsync();
        var none = await RunAsync("--url", Url, "--stream", "CDC");
        Assert.True(none.ExitCode == 2 && none.Error.Contains("cannot read stream CDC", StringComparison.Ordinal), none.Output + none.Error);
        // a stream with nothing in it: nothing to verify
        await using (var nats = new NatsConnection(new NatsOpts { Url = Url }))
            await new NatsJSContext(nats).CreateStreamAsync(new StreamConfig("CDC", ["cdc.>"]));
        var empty = await RunAsync("--url", Url, "--stream", "CDC", "--strict");
        Assert.True(empty.ExitCode == 0 && empty.Output.Contains("RESULT: nothing to verify", StringComparison.Ordinal), empty.Output + empty.Error);
    }
}
