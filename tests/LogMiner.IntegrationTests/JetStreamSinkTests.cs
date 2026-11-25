using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Hegel;
using Hegel.Xunit;
using LogMiner.Nats;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using LogMiner.Tests;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>The JetStream sink against a real server that is stopped, restarted, purged and lied about. Skipped (passes trivially) when no
/// nats-server is found (LOGMINER_NATS_SERVER, or ~/tools/nats-server).</summary>
#pragma warning disable CA1001
public sealed class JetStreamSinkTests : IDisposable
{
    private static readonly string? Exe = Environment.GetEnvironmentVariable("LOGMINER_NATS_SERVER")
        ?? (Directory.Exists(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server"))
            ? Directory.GetFiles(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server"), "nats-server*.exe", SearchOption.AllDirectories).FirstOrDefault() : null);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lm-nats-" + Guid.NewGuid().ToString("N"));
    private readonly int _port = FreePort();
    private readonly int _monitor = FreePort();
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
        _server = Process.Start(new ProcessStartInfo(Exe!, $"-a 127.0.0.1 -p {_port} -m {_monitor} -js -sd \"{_dir}\"") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true })!;
        _server.BeginErrorReadLine(); _server.BeginOutputReadLine();
        for (var i = 0; i < 200; i++)
        {
            try { using var c = new TcpClient(); await c.ConnectAsync(IPAddress.Loopback, _port); return; }
            catch (SocketException) { await Task.Delay(25); }
        }
        throw new TimeoutException("nats-server did not start");
    }

    private void StopServer()
    {
        _server!.Kill();
        _server.WaitForExit();
        _server.Dispose();
        _server = null;
    }

    public void Dispose()
    {
        if (_server is not null) StopServer();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private JetStreamSink Sink(double ackTimeout = 2, double duplicateWindow = 120) => new(new JetStreamSinkOptions { Url = Url, StreamName = "T", AckTimeout = TimeSpan.FromSeconds(ackTimeout), DuplicateWindow = TimeSpan.FromSeconds(duplicateWindow) });

    private static SinkMessage Msg(string id, string key = "db", string? payload = null) => new() { Database = key, Destination = "cdc.s." + new string(key.Where(char.IsAsciiLetterOrDigit).ToArray()), Key = key, MessageId = id, Payload = Encoding.UTF8.GetBytes(payload ?? id) };

    private static async Task SendAsync(JetStreamSink sink, params SinkMessage[] messages) => await (await sink.SendAsync(messages, CancellationToken.None)).WaitAsync(TimeSpan.FromSeconds(20));

    /// <summary>Until it works, as the pipeline does: whatever failed is sent again, in order.</summary>
    private static async Task SendUntilAsync(JetStreamSink sink, params SinkMessage[] messages)
    {
        for (var i = 0; ; i++)
        {
            try { await SendAsync(sink, messages); return; }
            catch (Exception) when (i < 200) { await Task.Delay(50); }
        }
    }

    /// <summary>Connections of sinks the server has now.</summary>
    private async Task<int> SinkConnectionsAsync()
    {
        using var http = new HttpClient();
        using var doc = System.Text.Json.JsonDocument.Parse(await http.GetStringAsync(new Uri($"http://127.0.0.1:{_monitor}/connz")));
        return doc.RootElement.GetProperty("connections").EnumerateArray().Count(c => c.TryGetProperty("name", out var n) && n.GetString()!.StartsWith("logminer@", StringComparison.Ordinal));
    }

    private async Task<List<string>> HeadersAsync(string name)
    {
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        var stream = await new NatsJSContext(nats).GetStreamAsync("T");
        var list = new List<string>();
        for (var seq = stream.Info.State.FirstSeq; seq <= stream.Info.State.LastSeq; seq++)
        {
            var headers = Encoding.UTF8.GetString(Convert.FromBase64String((await stream.GetAsync(new StreamMsgGetRequest { Seq = seq })).Message.Hdrs!));
            list.Add(headers.Split("\r\n").First(h => h.StartsWith(name + ": ", StringComparison.Ordinal))[(name.Length + 2)..]);
        }
        return list;
    }

    private async Task<List<(string Id, string Data)>> StoredAsync()
    {
        await using var nats = new NatsConnection(new NatsOpts { Url = Url, HeaderEncoding = Encoding.UTF8 });
        var stream = await new NatsJSContext(nats).GetStreamAsync("T");
        var list = new List<(string, string)>();
        for (var seq = stream.Info.State.FirstSeq; seq <= stream.Info.State.LastSeq && stream.Info.State.Messages > 0; seq++)
        {
            var m = (await stream.GetAsync(new StreamMsgGetRequest { Seq = seq })).Message;
            var headers = Encoding.UTF8.GetString(Convert.FromBase64String(m.Hdrs!));
            list.Add((headers.Split("\r\n").First(h => h.StartsWith("Nats-Msg-Id: ", StringComparison.Ordinal))["Nats-Msg-Id: ".Length..], Encoding.UTF8.GetString(m.Data.ToArray())));
        }
        return list;
    }

    [Fact]
    public async Task Stores_in_order_once_and_audits()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink();
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, [.. Enumerable.Range(0, 50).Select(i => Msg($"m{i}", "db" + i % 3))]);
        await SendAsync(sink, [.. Enumerable.Range(40, 20).Select(i => Msg($"m{i}", "db" + i % 3))]);
        Assert.Equal(10, sink.Duplicates);
        var stored = await StoredAsync();
        for (var k = 0; k < 3; k++) Assert.Equal(Enumerable.Range(0, 60).Where(i => i % 3 == k).Select(i => "m" + i), stored.Where(s => int.Parse(s.Id[1..], System.Globalization.CultureInfo.InvariantCulture) % 3 == k).Select(s => s.Id));
        Assert.Empty(await sink.AuditAsync(["db0", "db1", "db2", "never seen"], CancellationToken.None));
    }

    /// <summary>The server goes away between two messages of a call. What was on its way must not turn up after it is back, behind newer
    /// messages and beyond the duplicate window.</summary>
    [Fact]
    public async Task What_was_not_acknowledged_in_time_is_not_sent_later()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink(ackTimeout: 0.5, duplicateWindow: 1);
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, Msg("a"));
        StopServer();
        await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(sink, Msg("b", payload: "stale"), Msg("c")));
        for (var i = 0; i < 5; i++) await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(sink, Msg("b", payload: "stale")));
        await Task.Delay(1500); // the duplicate window passes
        await StartServerAsync();
        await SendUntilAsync(sink, Msg("b", payload: "fresh"), Msg("c", payload: "fresh"), Msg("d"));
        await Task.Delay(500);
        Assert.Equal([("a", "a"), ("b", "fresh"), ("c", "fresh"), ("d", "d")], await StoredAsync());
        Assert.True(sink.Health.Healthy);
        Assert.True(sink.Connections >= 2);
        await Task.Delay(3000); // a connection that was only left alone would have come back by itself, with whatever it still had to send
        Assert.Equal(1, await SinkConnectionsAsync());
    }

    /// <summary>Anything may answer on the reply subject: only the stream's own acknowledgement counts, and nothing else does harm.</summary>
    [Theory]
    [InlineData("{}")]
    [InlineData("{\"ok\":true}")]
    [InlineData("[]")]
    [InlineData("1")]
    [InlineData("\"x\"")]
    [InlineData("")]
    [InlineData("+ACK")]
    [InlineData("null")]
    [InlineData("{\"stream\":\"OTHER\",\"seq\":7}")]
    [InlineData("{\"stream\":\"T\",\"seq\":0}")]
    [InlineData("{\"stream\":\"T\"}")]
    [InlineData("{\"stream\":\"T\",\"seq\":\"7\",\"duplicate\":\"yes\"}")]
    public async Task Only_the_streams_acknowledgement_counts(string answer)
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink(ackTimeout: 0.7);
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, Msg("first"));
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        await new NatsJSContext(nats).DeleteStreamAsync("T");
        using var stop = new CancellationTokenSource();
        var responder = Task.Run(async () =>
        {
            try { await foreach (var m in nats.SubscribeAsync<byte[]>("cdc.>", cancellationToken: stop.Token)) await m.ReplyAsync(Encoding.UTF8.GetBytes(answer)); }
            catch (OperationCanceledException) { }
        });
        await Task.Delay(200);
        await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(sink, Msg("second")));
        await stop.CancelAsync();
        await responder;
        Assert.NotNull(await AuditOrErrorAsync(sink));
        try { await new NatsJSContext(nats).CreateStreamAsync(new StreamConfig("T", ["cdc.>"])); }
        catch (NatsJSApiException) { } // the sink was faster
        // another stream: this sink is done with, and the next one is told by the receipt
        for (var i = 0; i < 200 && !sink.Health.Failed; i++) await Task.Delay(50);
        Assert.Contains("deleted and created again", await AuditOrErrorAsync(sink), StringComparison.Ordinal);
        await using var next = Sink();
        await next.StartAsync(CancellationToken.None);
        Assert.Equal(ReceiptState.OtherDestination, (await next.VerifyAsync(new Dictionary<string, string> { ["db"] = sink.ReceiptOf("db")! }, CancellationToken.None))["db"]);
        await SendUntilAsync(next, Msg("second"), Msg("third"));
        Assert.Equal(["second", "third"], (await StoredAsync()).Select(s => s.Id));
    }

    /// <summary>A subscriber that answers, too (perhaps first), does not keep the stream's acknowledgement from counting.</summary>
    [Fact]
    public async Task Somebody_else_answering_does_no_harm()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink(ackTimeout: 3);
        await sink.StartAsync(CancellationToken.None);
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        using var stop = new CancellationTokenSource();
        var responder = Task.Run(async () =>
        {
            try { await foreach (var m in nats.SubscribeAsync<byte[]>("cdc.>", cancellationToken: stop.Token)) await m.ReplyAsync("+ACK"u8.ToArray()); }
            catch (OperationCanceledException) { }
        });
        await Task.Delay(200);
        await SendAsync(sink, [.. Enumerable.Range(0, 30).Select(i => Msg("m" + i))]);
        await stop.CancelAsync();
        await responder;
        Assert.Equal(Enumerable.Range(0, 30).Select(i => "m" + i), (await StoredAsync()).Select(s => s.Id));
        Assert.Equal(1, sink.Connections);
    }

    private static async Task<string?> AuditOrErrorAsync(JetStreamSink sink)
    {
        try { return (await sink.AuditAsync(["db"], CancellationToken.None)).Values.FirstOrDefault(); }
        catch (NatsException ex) { return ex.Message; }
    }

    [Fact]
    public async Task An_audit_notices_what_the_stream_no_longer_has()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink();
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, Msg("a"), Msg("b"));
        Assert.Empty(await sink.AuditAsync(["db"], CancellationToken.None));
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        await (await new NatsJSContext(nats).GetStreamAsync("T")).PurgeAsync(new StreamPurgeRequest());
        Assert.Contains("does not hold", (await sink.AuditAsync(["db", "other"], CancellationToken.None))["db"], StringComparison.Ordinal);
        await SendAsync(sink, Msg("c"));
        Assert.Empty(await sink.AuditAsync(["db"], CancellationToken.None));
    }

    /// <summary>What has expired is not missing, and what is wrong with one database does not concern the others.</summary>
    [Fact]
    public async Task An_audit_does_not_miss_what_has_expired()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = new JetStreamSink(new JetStreamSinkOptions { Url = Url, StreamName = "T", MaxAge = TimeSpan.FromSeconds(2), DuplicateWindow = TimeSpan.FromSeconds(1) });
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, Msg("quiet", "quiet"));
        await Task.Delay(1500);
        await SendAsync(sink, Msg("busy", "busy"));
        Assert.Empty(await sink.AuditAsync(["quiet", "busy"], CancellationToken.None));
        await Task.Delay(1000);
        Assert.Empty(await sink.AuditAsync(["quiet", "busy"], CancellationToken.None)); // expired
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        await (await new NatsJSContext(nats).GetStreamAsync("T")).PurgeAsync(new StreamPurgeRequest());
        Assert.Equal(["busy"], (await sink.AuditAsync(["quiet", "busy"], CancellationToken.None)).Keys);
    }

    /// <summary>The receipt of the newest message, shown to the stream by another process later.</summary>
    [Fact]
    public async Task A_receipt_says_whether_the_stream_went_back()
    {
        if (Exe is null) return;
        await StartServerAsync();
        string a, b;
        await using (var sink = Sink())
        {
            await sink.StartAsync(CancellationToken.None);
            Assert.Null(sink.ReceiptOf("db"));
            await SendAsync(sink, Msg("a"));
            a = sink.ReceiptOf("db")!;
        }
        StopServer();
        var copy = _dir + "-copy";
        CopyDirectory(_dir, copy);
        await StartServerAsync();
        async Task<ReceiptState> StateAsync(string receipt, string stream = "T")
        {
            await using var sink = new JetStreamSink(new JetStreamSinkOptions { Url = Url, StreamName = stream, StreamSubjects = [stream == "T" ? "cdc.>" : "other.>"] });
            await sink.StartAsync(CancellationToken.None);
            return (await sink.VerifyAsync(new Dictionary<string, string> { ["db"] = receipt }, CancellationToken.None))["db"];
        }
        await using (var sink = Sink())
        {
            await sink.StartAsync(CancellationToken.None);
            await SendAsync(sink, Msg("b|with a separator and a %"));
            b = sink.ReceiptOf("db")!;
        }
        Assert.Equal((ReceiptState.Present, ReceiptState.Present), (await StateAsync(a), await StateAsync(b)));
        Assert.Equal(ReceiptState.OtherDestination, await StateAsync(a, "OTHER"));
        Assert.Equal(ReceiptState.Unknown, await StateAsync("something else"));
        StopServer();
        Directory.Delete(_dir, recursive: true);
        Directory.Move(copy, _dir);
        await StartServerAsync();
        Assert.Equal((ReceiptState.Present, ReceiptState.Lost), (await StateAsync(a), await StateAsync(b)));
        await using (var sink = Sink())
        {
            await sink.StartAsync(CancellationToken.None);
            await SendAsync(sink, Msg("c")); // takes b's sequence number
        }
        Assert.Equal((ReceiptState.Present, ReceiptState.Lost), (await StateAsync(a), await StateAsync(b)));
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        var js = new NatsJSContext(nats);
        await (await js.GetStreamAsync("T")).PurgeAsync(new StreamPurgeRequest());
        Assert.Equal(ReceiptState.Unknown, await StateAsync(a));
        await js.DeleteStreamAsync("T");
        Assert.Equal(ReceiptState.OtherDestination, await StateAsync(a));
    }

    /// <summary>A receipt of an earlier process that was verified: from then on every new connection is checked against it, too.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_stream_that_goes_back_behind_a_verified_receipt_is_noticed(bool goesBack)
    {
        if (Exe is null) return;
        await StartServerAsync();
        async Task<string> SendAndCloseAsync(string id)
        {
            await using var s = Sink();
            await s.StartAsync(CancellationToken.None);
            await SendAsync(s, Msg(id));
            return s.ReceiptOf("db")!;
        }
        await SendAndCloseAsync("a");
        StopServer();
        var before = _dir + "-before";
        CopyDirectory(_dir, before);
        await StartServerAsync();
        var receipt = await SendAndCloseAsync("b");
        await using var sink = Sink();
        await sink.StartAsync(CancellationToken.None);
        Assert.Equal(ReceiptState.Present, (await sink.VerifyAsync(new Dictionary<string, string> { ["db"] = receipt }, CancellationToken.None))["db"]);
        StopServer();
        if (goesBack)
        {
            Directory.Delete(_dir, recursive: true);
            Directory.Move(before, _dir);
        }
        await StartServerAsync();
        for (var i = 0; i < 200 && !sink.Health.Failed && !sink.Health.Healthy; i++) await Task.Delay(50);
        Assert.Equal(goesBack, sink.Health.Failed);
        if (!goesBack) await SendUntilAsync(sink, Msg("c"));
        Assert.Equal(goesBack ? ["a"] : ["a", "b", "c"], (await StoredAsync()).Select(m => m.Id));
    }

    /// <summary>The power fails on the NATS host and unsynced messages are gone.</summary>
    [Fact]
    public async Task An_audit_notices_a_stream_that_went_back()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink();
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, Msg("a"));
        StopServer();
        var copy = _dir + "-copy";
        CopyDirectory(_dir, copy);
        await StartServerAsync();
        await SendUntilAsync(sink, Msg("b"), Msg("c"));
        StopServer();
        Directory.Delete(_dir, recursive: true);
        Directory.Move(copy, _dir);
        await StartServerAsync();
        // nothing is sent over the new connection: it would take the place of what was lost
        for (var i = 0; i < 200 && !sink.Health.Failed; i++)
        {
            await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(sink, Msg("d")));
            await Task.Delay(50);
        }
        Assert.True(sink.Health.Failed);
        await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(sink, Msg("d")));
        Assert.Contains("lost what it had acknowledged", (await sink.AuditAsync(["db"], CancellationToken.None))["db"], StringComparison.Ordinal);
        Assert.Equal([("a", "a")], await StoredAsync());
    }

    /// <summary>SQL Server, NATS (one replica) and the miner on one host; the power fails between two syncs of NATS. The position (durable at once) is
    /// behind rows 4 and 5, the stream (durable at the next sync) is not. Found by a review: they were lost, and nothing said so.</summary>
    [Fact]
    public async Task What_the_stream_lost_in_a_power_failure_is_published_again()
    {
        if (Exe is null) return;
        await StartServerAsync();
        var db = await TestServer.CreateDatabaseAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NOT NULL);", "t");
        var copy = _dir + "-copy";
        try
        {
            var store = new MemoryCheckpointStore();
            async Task<(CaptureEngine Engine, JetStreamSink Sink)> RunAsync()
            {
                var sink = Sink();
                var engine = new CaptureEngine(new CaptureOptions
                {
                    ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n == db, Workers = 1, CheckpointInterval = TimeSpan.FromMilliseconds(50), ReleaseInterval = TimeSpan.FromSeconds(2),
                    Settings = new CaptureSettings { StartPosition = StartPosition.Latest }, SweepInterval = TimeSpan.FromSeconds(1), HoldCheckInterval = TimeSpan.FromHours(1),
                }, new DmvPollSignal(TestServer.ConnectionString(), TimeSpan.FromMilliseconds(5)), new EventEncoder(new EncoderOptions { ServerName = "it" }), sink, store);
                await engine.StartAsync(CancellationToken.None);
                for (var i = 0; i < 400 && engine.Captures.Any(c => !c.Live || c.HoldingForBaseline); i++) await Task.Delay(20);
                return (engine, sink);
            }
            async Task<List<int>> IdsAsync() => (await StoredAsync()).DistinctBy(m => m.Id).Select(m => System.Text.Json.JsonDocument.Parse(m.Data).RootElement.GetProperty("after").GetProperty("id").GetInt32()).ToList();
            async Task InsertAsync(int id)
            {
                await TestServer.ExecAsync($"INSERT dbo.t VALUES ({id}, {id});", db);
                for (var i = 0; i < 400 && !(await IdsAsync()).Contains(id); i++) await Task.Delay(20);
            }
            var (engine, sink) = await RunAsync();
            for (var id = 1; id <= 3; id++) { await InsertAsync(id); await Task.Delay(2500); } // several looks: there is a safe position, and log was released behind it
            var safe = (await store.LoadAsync(db, CancellationToken.None))!.Safe;
            Assert.NotNull(safe);
            Assert.True(engine.Captures.Single().ReleasedThrough < safe.CommitLsn && !engine.Captures.Single().ReleasedThrough.IsZero);
            StopServer(); CopyDirectory(_dir, copy); await StartServerAsync(); // what NATS has on disk at its last sync
            await InsertAsync(4);
            await InsertAsync(5);
            for (var i = 0; i < 200 && (await store.LoadAsync(db, CancellationToken.None))!.LastCommitLsn < engine.Captures.Single().AcknowledgedCommit; i++) await Task.Delay(20);
            Assert.Equal([1, 2, 3, 4, 5], await IdsAsync());
            var durable = (await store.LoadAsync(db, CancellationToken.None))!; // at this moment the power fails
            await engine.DisposeAsync();
            await sink.DisposeAsync();
            store = new MemoryCheckpointStore();
            await store.SaveAsync([durable], CancellationToken.None);
            StopServer(); Directory.Delete(_dir, recursive: true); Directory.Move(copy, _dir); await StartServerAsync();
            Assert.Equal([1, 2, 3], await IdsAsync());
            (engine, sink) = await RunAsync();
            await InsertAsync(6);
            Assert.Equal([1, 2, 3, 4, 5, 6], await IdsAsync());
            await Task.Delay(5000); // and the log behind it is released again
            await InsertAsync(7);
            Assert.Equal([1, 2, 3, 4, 5, 6, 7], await IdsAsync());
            Assert.Null(engine.AuditProblem);
            Assert.Empty(engine.ForeignReleases);
            await engine.DisposeAsync();
            await sink.DisposeAsync();
        }
        finally { await TestServer.DropDatabaseAsync(db); }
    }

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var d in Directory.GetDirectories(from, "*", SearchOption.AllDirectories)) Directory.CreateDirectory(d.Replace(from, to, StringComparison.Ordinal));
        foreach (var f in Directory.GetFiles(from, "*", SearchOption.AllDirectories)) File.Copy(f, f.Replace(from, to, StringComparison.Ordinal), overwrite: true);
    }

    [Fact]
    public async Task A_stream_that_exists_is_not_changed()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var nats = new NatsConnection(new NatsOpts { Url = Url });
        var js = new NatsJSContext(nats);
        await js.CreateStreamAsync(new StreamConfig("T", ["cdc.>", "other.>"]) { MaxAge = TimeSpan.FromDays(365), DuplicateWindow = TimeSpan.FromMinutes(1) });
        await using var sink = Sink(duplicateWindow: 600);
        await sink.StartAsync(CancellationToken.None);
        await SendAsync(sink, Msg("a"));
        var config = (await js.GetStreamAsync("T")).Info.Config;
        Assert.Equal((TimeSpan.FromDays(365), TimeSpan.FromMinutes(1), 2), (config.MaxAge, config.DuplicateWindow, config.Subjects!.Count));
        Assert.Contains("de-duplicates over", sink.Health.LastError, StringComparison.Ordinal);
    }

    [Fact]
    public async Task One_message_that_cannot_be_sent_does_not_hold_up_other_databases()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink(ackTimeout: 5);
        await sink.StartAsync(CancellationToken.None);
        Assert.InRange(sink.MaxPayloadBytes, 1024, 1024 * 1024); // the server's default is 1 MB
        var big = new SinkMessage { Database = "big", Destination = "cdc.s.big", Key = "big", MessageId = "big", Payload = new byte[3_000_000] };
        var bad = new SinkMessage { Database = "bad", Destination = "cdc.s.bad", Key = "bad", MessageId = "bad", Payload = new byte[1], Headers = [new("h", "line\r\nbreak")] };
        var failed = await sink.SendAsync([big, bad, Msg("ok", "good"), Msg("ok2", "good")], CancellationToken.None);
        await Assert.ThrowsAnyAsync<Exception>(() => failed.WaitAsync(TimeSpan.FromSeconds(20)));
        await SendUntilAsync(sink, Msg("ok", "good"), Msg("ok2", "good"), Msg("ok3", "good"));
        Assert.Equal(["ok", "ok2", "ok3"], (await StoredAsync()).Select(s => s.Id).Distinct());
    }

    [Fact]
    public async Task Names_and_ids_arrive_as_they_are()
    {
        if (Exe is null) return;
        await StartServerAsync();
        await using var sink = Sink();
        await sink.StartAsync(CancellationToken.None);
        var encoder = new EventEncoder(new EncoderOptions { ServerName = "sørver" });
        foreach (var db in new[] { "kund_å", "kund_ä", " kund", "kund", "kund\nx" })
        {
            var layout = new LogMiner.Schema.TableLayout(5, db, 1, "dbo", "t", 1, LogMiner.Schema.DataCompression.None, []);
            await SendAsync(sink, encoder.Encode(new LogMiner.Events.ChangeEvent { Table = layout, Op = LogMiner.Events.ChangeOp.Create, Lsn = new Lsn(1, 1, 1), CommitLsn = new Lsn(1, 1, 1), Sequence = 0 }));
        }
        Assert.Equal(5, (await StoredAsync()).Count);
        Assert.Equal(0, sink.Duplicates);
        Assert.Equal(["kund_å", "kund_ä", "%20kund", "kund", "kund%0Ax"], await HeadersAsync("lm-db"));
    }

    [Fact]
    public async Task Sending_fails_at_once_while_there_is_no_connection_and_disposing_does_not_wait()
    {
        if (Exe is null) return;
        await StartServerAsync();
        var sink = Sink(ackTimeout: 30);
        await sink.StartAsync(CancellationToken.None);
        StopServer();
        var sw = Stopwatch.StartNew();
        for (var i = 0; i < 20; i++) await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(sink, Msg("x" + i)));
        Assert.False(sink.Health.Healthy);
        await sink.DisposeAsync();
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(15), $"{sw.Elapsed}");
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await sink.SendAsync([Msg("y")], CancellationToken.None));
    }

    /// <summary>Outages at any moment, sending again as the pipeline does: what is stored, taking the last copy of each message, is what was
    /// sent, in order - with a duplicate window so short that it does not help.</summary>
    [Fact]
    public void Whatever_the_server_does_the_last_copies_are_in_order() => Property.Test(tc => Blocking.Run(() => OutagesAsync(tc)), new Settings { TestCases = TestServer.Cases(6), SuppressHealthCheck = HealthCheck.TooSlow });

    private async Task OutagesAsync(TestCase tc)
    {
        if (Exe is null) return;
        if (_server is not null) StopServer();
        try { Directory.Delete(_dir, recursive: true); } catch (DirectoryNotFoundException) { }
        await StartServerAsync();
        await using var sink = Sink(ackTimeout: 0.4, duplicateWindow: 0.2);
        await sink.StartAsync(CancellationToken.None);
        var sent = 0;
        for (var steps = tc.Draw(Generators.Integers().Min(2).Max(6), "steps"); steps > 0; steps--)
        {
            var batch = Enumerable.Range(sent, tc.Draw(Generators.Integers().Min(1).Max(40), "messages")).Select(i => Msg("m" + i)).ToArray();
            sent += batch.Length;
            var kill = tc.Draw(Generators.Integers().Min(0).Max(40), "killAfterMs");
            var down = tc.Draw(Generators.Integers().Min(0).Max(900), "downMs");
            var sending = SendUntilAsync(sink, batch);
            if (tc.Draw(Generators.Booleans(), "outage"))
            {
                await Task.Delay(kill);
                StopServer();
                await Task.Delay(down);
                await StartServerAsync();
            }
            await sending;
        }
        await Task.Delay(600); // anything late would come now
        var stored = (await StoredAsync()).Select(s => int.Parse(s.Id[1..], System.Globalization.CultureInfo.InvariantCulture)).ToList();
        var lastCopies = stored.Select((id, at) => (id, at)).GroupBy(x => x.id).Select(g => (g.Key, At: g.Max(x => x.at))).OrderBy(x => x.At).Select(x => x.Key).ToList();
        Assert.Equal(Enumerable.Range(0, sent), lastCopies);
    }
}
