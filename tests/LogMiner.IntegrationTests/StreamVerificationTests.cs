using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Nats;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using LogMiner.Verify;
using Microsoft.Data.SqlClient;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;
using Xunit.Abstractions;

namespace LogMiner.IntegrationTests;

/// <summary>
/// Tables against the stream, end to end: SQL Server, the engine, a NATS server, and the verifier (logminer-verify) over what is in the stream.
/// It must find nothing in what was published, and it must find every single event that is taken away. Skipped (passes trivially) when no
/// nats-server is found.
/// </summary>
public sealed class StreamVerificationTests(ITestOutputHelper output) : IDisposable
{
    private static readonly string? Exe = Executables.NatsServer;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lm-nats-" + Guid.NewGuid().ToString("N"));
    private Process? _server;

    private sealed record Message(ulong Seq, string Subject, string Id, byte[] Data);

    private const string Ddl = """
        CREATE TABLE dbo.orders (id bigint NOT NULL PRIMARY KEY, customer nvarchar(40) NOT NULL, amount decimal(18, 4) NOT NULL, at datetime2(7) NOT NULL, note varchar(100) NULL, f float NULL, b varbinary(40) NULL, flag bit NULL);
        CREATE TABLE dbo.lines (order_id bigint NOT NULL, line int NOT NULL, sku varchar(30) COLLATE Latin1_General_CI_AS NOT NULL, qty int NOT NULL, PRIMARY KEY (order_id, line));
        CREATE TABLE dbo.codes (code nvarchar(30) NOT NULL PRIMARY KEY, v int NULL, changed datetimeoffset(3) NULL);
        """;

    private const string Fill = """
        INSERT dbo.orders SELECT TOP (300) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), N'kund ö ' + CONVERT(nvarchar(10), ABS(CHECKSUM(NEWID())) % 50), ABS(CHECKSUM(NEWID())) % 100000 / 7.0, SYSUTCDATETIME(), NULL, RAND(CHECKSUM(NEWID())), CONVERT(varbinary(40), NEWID()), 1 FROM sys.all_objects;
        INSERT dbo.lines SELECT id, 1, 'sku-' + CONVERT(varchar(20), id), id % 9 FROM dbo.orders;
        INSERT dbo.codes SELECT TOP (100) N'code ' + CONVERT(nvarchar(10), ROW_NUMBER() OVER (ORDER BY (SELECT 1))), 1, SYSDATETIMEOFFSET() FROM sys.all_objects;
        """;

    private static string Statement(Random r, int writer)
    {
        var id = r.Next(1, 420);
        return r.Next(12) switch
        {
            0 => $"IF NOT EXISTS (SELECT 1 FROM dbo.orders WHERE id = {id}) INSERT dbo.orders (id, customer, amount, at) VALUES ({id}, N'ny {writer}', {r.Next(1, 99999)} / 3.0, SYSUTCDATETIME());",
            1 => $"DELETE dbo.orders WHERE id = {id};",
            2 => $"BEGIN TRAN; DELETE dbo.codes WHERE code = N'code {id}'; INSERT dbo.codes VALUES (N'code {id}', {r.Next(9)}, SYSDATETIMEOFFSET()); COMMIT;",
            3 => $"BEGIN TRAN; UPDATE dbo.orders SET amount += 1, note = REPLICATE('n', {r.Next(100)}) WHERE id % 25 = {id % 25}; ROLLBACK;",
            4 => $"UPDATE dbo.orders SET amount += 1, note = REPLICATE('n', {r.Next(100)}), f = {r.Next(1000)} / 7.0 WHERE id % 25 = {id % 25};",
            5 => $"UPDATE dbo.codes SET code = N'code {id}' WHERE code = N'code {r.Next(1, 420)}' AND NOT EXISTS (SELECT 1 FROM dbo.codes x WHERE x.code = N'code {id}');",
            6 => $"UPDATE dbo.lines SET line += 1 WHERE order_id = {id};",
            7 => $"BEGIN TRAN; IF NOT EXISTS (SELECT 1 FROM dbo.lines WHERE order_id = {id} AND line = 77) INSERT dbo.lines VALUES ({id}, 77, 'x', 1); SAVE TRAN s; DELETE dbo.lines WHERE order_id = {id}; ROLLBACK TRAN s; UPDATE dbo.lines SET qty += 1 WHERE order_id = {id}; COMMIT;",
            8 => $"DELETE dbo.lines WHERE order_id = {id} AND line > {r.Next(3)};",
            _ => $"UPDATE dbo.orders SET amount += 1, flag = ~ISNULL(flag, 0) WHERE id = {id}; UPDATE dbo.codes SET v = ISNULL(v, 0) + 1 WHERE code = N'code {id % 130}';",
        };
    }

    private async Task<string> StartServerAsync()
    {
        int port;
        using (var l = new TcpListener(IPAddress.Loopback, 0)) { l.Start(); port = ((IPEndPoint)l.LocalEndpoint).Port; }
        Directory.CreateDirectory(_dir);
        _server = Process.Start(new ProcessStartInfo(Exe!, $"-a 127.0.0.1 -p {port} -js -sd \"{_dir}\"") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true })!;
        _server.BeginErrorReadLine(); _server.BeginOutputReadLine();
        for (var i = 0; i < 200; i++)
        {
            try { using var c = new TcpClient(); await c.ConnectAsync(IPAddress.Loopback, port); return $"nats://127.0.0.1:{port}"; }
            catch (SocketException) { await Task.Delay(25); }
        }
        throw new TimeoutException("nats-server did not start");
    }

    public void Dispose()
    {
        if (_server is not null) { _server.Kill(); _server.WaitForExit(); _server.Dispose(); }
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private static async Task<List<Message>> MessagesAsync(string url)
    {
        await using var nats = new NatsConnection(new NatsOpts { Url = url, HeaderEncoding = Encoding.UTF8 });
        var stream = await new NatsJSContext(nats).GetStreamAsync("V");
        var list = new List<Message>();
        for (var seq = stream.Info.State.FirstSeq; seq <= stream.Info.State.LastSeq; seq++)
        {
            var m = (await stream.GetAsync(new StreamMsgGetRequest { Seq = seq })).Message;
            list.Add(new Message(seq, m.Subject, Encoding.UTF8.GetString(Convert.FromBase64String(m.Hdrs!)).Split("\r\n").First(h => h.StartsWith("Nats-Msg-Id: ", StringComparison.Ordinal))[13..], m.Data.ToArray()));
        }
        return list;
    }

    private static VerificationReport Verify(IReadOnlyList<Message> messages, int without = -1)
    {
        var v = new StreamVerifier();
        for (var i = 0; i < messages.Count; i++)
            if (i != without) v.Add(messages[i].Subject, messages[i].Data, messages[i].Id, messages[i].Seq);
        return v.Report();
    }

    private static async Task LoadAsync(string state, string db, int chunkSize)
    {
        await TestServer.ExecAsync($"INSERT dbo.snapshot_requests (server_name, database_name, table_name, chunk_size) VALUES ('srv', '{db}', NULL, {chunkSize});", state);
        await using var conn = new SqlConnection(TestServer.ConnectionString(state));
        await conn.OpenAsync();
        for (var i = 0; i < 600; i++)
        {
            await using var cmd = new SqlCommand("SELECT TOP (1) status FROM dbo.snapshot_requests ORDER BY id DESC", conn);
            if ((string)(await cmd.ExecuteScalarAsync())! is var status and not ("requested" or "running")) { Assert.Equal("done", status); return; }
            await Task.Delay(200);
        }
        Assert.Fail("the historical load did not finish");
    }

    [Fact]
    public async Task The_stream_matches_the_tables_and_every_event_that_is_taken_away_is_noticed()
    {
        if (Exe is null) return;
        var url = await StartServerAsync();
        var state = "lm_state_" + Guid.NewGuid().ToString("N")[..10];
        await TestServer.ExecAsync($"CREATE DATABASE [{state}];");
        var db = await TestServer.CreateDatabaseAsync(Ddl + Fill, "orders", "lines", "codes"); // there are rows from before the capture
        try
        {
            async Task<(CaptureEngine Engine, SqlStateStore Store, JetStreamSink Sink)> StartAsync()
            {
                var store = new SqlStateStore(TestServer.ConnectionString(state), "srv");
                await store.EnsureSchemaAsync(CancellationToken.None);
                await store.AcquireLeaseAsync(null, CancellationToken.None);
                var requests = new SnapshotRequests(TestServer.ConnectionString(state), "srv");
                await requests.EnsureSchemaAsync(CancellationToken.None);
                var sink = new JetStreamSink(new JetStreamSinkOptions { Url = url, StreamName = "V", AckTimeout = TimeSpan.FromSeconds(5) });
                var engine = new CaptureEngine(new CaptureOptions
                {
                    ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n == db, Workers = 2, CheckpointInterval = TimeSpan.FromMilliseconds(100), ReleaseInterval = TimeSpan.FromSeconds(1),
                    Settings = new CaptureSettings { StartPosition = StartPosition.Latest, MaxBatchEvents = 64 }, SnapshotRequests = requests,
                }, new DmvPollSignal(TestServer.ConnectionString(), TimeSpan.FromMilliseconds(10)), new EventEncoder(new EncoderOptions { ServerName = "it" }), sink, store);
                await engine.StartAsync(CancellationToken.None);
                for (var i = 0; i < 400 && engine.Captures.Any(c => !c.Live || c.HoldingForBaseline); i++) await Task.Delay(20);
                return (engine, store, sink);
            }

            var (engine, store, sink) = await StartAsync();
            using var stop = new CancellationTokenSource();
            var writers = Task.WhenAll(Enumerable.Range(0, 3).Select(w => Task.Run(async () =>
            {
                var r = new Random(w * 7919 + 1);
                await using var conn = new SqlConnection(TestServer.ConnectionString(db));
                await conn.OpenAsync();
                while (!stop.IsCancellationRequested)
                {
                    await using var cmd = new SqlCommand(Statement(r, w), conn);
                    try { await cmd.ExecuteNonQueryAsync(); }
                    catch (SqlException ex) when (ex.Number is 2627 or 2601 or 1205 or 3960 or 266 or 3902) { } // a conflict with another writer
                    await Task.Delay(r.Next(12));
                }
            })));
            await Task.Delay(1500);
            await LoadAsync(state, db, 40);
            await TestServer.RetryOnDeadlockAsync(() => TestServer.ExecAsync("ALTER TABLE dbo.orders ADD extra int NULL;", db));
            await Task.Delay(1500);
            await engine.DisposeAsync(); await store.DisposeAsync(); await sink.DisposeAsync(); // and what is written meanwhile is read afterwards
            await Task.Delay(1500);
            (engine, store, sink) = await StartAsync();
            await Task.Delay(1500);
            await LoadAsync(state, db, 25);
            await Task.Delay(1000);
            await stop.CancelAsync();
            await writers;
            await LoadAsync(state, db, 100);
            await engine.DisposeAsync(); await store.DisposeAsync(); await sink.DisposeAsync();

            var messages = await MessagesAsync(url);
            var report = Verify(messages);
            output.WriteLine(report.ToText());
            Assert.True(report.Clean, report.ToText());
            Assert.Equal(3, report.Count(Verdict.Verified));
            Assert.Equal(3, report.Tables.Count);

            // one event is missing. Before a table's first load has ended nobody knows what it held, and what changes nothing is nothing.
            var ended = new HashSet<string>();
            var candidates = new List<(int Index, string Op)>();
            for (var i = 0; i < messages.Count; i++)
            {
                using var doc = JsonDocument.Parse(messages[i].Data);
                var e = doc.RootElement;
                var op = e.GetProperty("op").GetString()!;
                var table = e.GetProperty("source").GetProperty("table").GetString()!;
                if (op == "l" && e.GetProperty("load").GetProperty("phase").GetString() == "end") ended.Add(table);
                if (op is not ("c" or "u" or "d" or "r") || !ended.Contains(table)) continue;
                if (op == "u" && e.GetProperty("before").GetRawText() == e.GetProperty("after").GetRawText()) continue;
                candidates.Add((i, op));
            }
            Assert.True(candidates.Count > 1000, $"only {candidates.Count} candidates in {messages.Count} messages");
            var rng = new Random(1);
            var tried = candidates.OrderBy(_ => rng.Next()).Take(600).ToList();
            Assert.Equal(["c", "d", "r", "u"], tried.Select(t => t.Op).Distinct().Order());
            var unnoticed = tried.AsParallel().Where(t => Verify(messages, t.Index).Clean).ToList();
            output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"{messages.Count} messages; {tried.Count} of {candidates.Count} taken away one at a time, {unnoticed.Count} unnoticed"));
            Assert.True(unnoticed.Count == 0, $"{unnoticed.Count} lost events are not noticed: " + string.Join("\n", unnoticed.Take(5).Select(u => $"{messages[u.Index].Seq} {messages[u.Index].Subject}: {Encoding.UTF8.GetString(messages[u.Index].Data)}")));
        }
        finally
        {
            await TestServer.DropDatabaseAsync(db);
            await TestServer.DropDatabaseAsync(state);
        }
    }
}
