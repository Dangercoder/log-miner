using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>The real logminer.exe, its databases and its NATS server: whatever is taken away, it comes back by itself and nothing is missing.</summary>
public sealed class ServiceTests : IAsyncLifetime, IDisposable
{
    private static readonly string? Nats = Directory.Exists(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server"))
        ? Directory.GetFiles(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server"), "nats-server*.exe", SearchOption.AllDirectories).FirstOrDefault() : null;

    private readonly string _tag = Guid.NewGuid().ToString("N")[..8];
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lm-svc-" + Guid.NewGuid().ToString("N"));
    private readonly int _port = FreePort();
    private readonly List<string> _databases = [];
    private readonly System.Text.StringBuilder _output = new();
    private Process? _nats, _service;

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    private string State => $"lm_it_state_{_tag}";

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        await TestServer.ExecAsync($"CREATE DATABASE [{State}];");
        for (var i = 0; i < 3; i++)
        {
            var db = await TestServer.CreateDatabaseAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);", "t");
            await TestServer.ExecAsync($"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [{db}] MODIFY NAME = [lm_it_svc_{_tag}_{i}]; ALTER DATABASE [lm_it_svc_{_tag}_{i}] SET MULTI_USER;");
            _databases.Add($"lm_it_svc_{_tag}_{i}");
        }
    }

    private void StartNats()
    {
        _nats = Process.Start(new ProcessStartInfo(Nats!, $"-a 127.0.0.1 -p {_port} -js -sd \"{_dir}\"") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true })!;
        _nats.BeginErrorReadLine(); _nats.BeginOutputReadLine();
    }

    private void StopNats() { _nats!.Kill(); _nats.WaitForExit(); _nats.Dispose(); _nats = null; }

    private void StartService(params (string Key, string Value)[] more)
    {
        var info = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "logminer.exe")) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true, WorkingDirectory = _dir };
        (string, string)[] settings =
        [
            ("ServerName", "svc-" + _tag), ("ConnectionString", TestServer.ConnectionString()), ("Databases__0", $"lm_it_svc_{_tag}_*"), ("State__Database", State), ("SelfTest", "Off"),
            ("ReleaseIntervalMs", "500"), ("CheckpointIntervalMs", "200"), ("HealthLogIntervalSeconds", "1"), ("StartPosition", "Earliest"),
            ("Sink__NatsJetStream__Url", $"nats://127.0.0.1:{_port}"), ("Sink__NatsJetStream__Stream", "SVC"), ("Sink__NatsJetStream__AckTimeoutSeconds", "2"), .. more,
        ];
        foreach (var (k, v) in settings) info.Environment["LogMiner__" + k] = v;
        _service = Process.Start(info)!;
        _service.OutputDataReceived += (_, e) => { lock (_output) _output.AppendLine(e.Data); };
        _service.ErrorDataReceived += (_, e) => { lock (_output) _output.AppendLine(e.Data); };
        _service.BeginOutputReadLine(); _service.BeginErrorReadLine();
    }

    private string Output { get { lock (_output) return _output.ToString(); } }

    private async Task InsertAsync(int id) => await TestServer.ExecAsync($"INSERT dbo.t VALUES ({id}, {id})", _databases[id % 3]);

    private async Task<List<(string Db, int Id)>> StoredAsync()
    {
        await using var nats = new NatsConnection(new NatsOpts { Url = $"nats://127.0.0.1:{_port}" });
        INatsJSStream stream;
        try { stream = await new NatsJSContext(nats).GetStreamAsync("SVC"); } catch (NatsException) { return []; }
        var list = new List<(string, int)>();
        for (var seq = stream.Info.State.FirstSeq; seq <= stream.Info.State.LastSeq && stream.Info.State.Messages > 0; seq++)
        {
            using var doc = JsonDocument.Parse((await stream.GetAsync(new StreamMsgGetRequest { Seq = seq })).Message.Data);
            list.Add((doc.RootElement.GetProperty("source").GetProperty("db").GetString()!, doc.RootElement.GetProperty("after").GetProperty("id").GetInt32()));
        }
        return list;
    }

    private async Task ArrivedAsync(int count, int seconds = 60)
    {
        var sw = Stopwatch.StartNew();
        List<(string, int)> stored = [];
        while (sw.Elapsed < TimeSpan.FromSeconds(seconds))
        {
            try { stored = await StoredAsync(); } catch (NatsException) { }
            if (stored.Distinct().Count() >= count) return;
            Assert.False(_service!.HasExited, "the service exited\n" + Output);
            await Task.Delay(200);
        }
        var got = stored.Select(s => s.Item2).ToHashSet();
        Assert.Fail($"missing: {string.Join(", ", Enumerable.Range(1, count).Where(i => !got.Contains(i)))}\n{Output[Math.Max(0, Output.Length - 6000)..]}");
    }

    private async Task<int> KillSessionsAsync(string where)
    {
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand($"DECLARE @n int = 0, @id int; DECLARE c CURSOR LOCAL FOR SELECT s.session_id FROM sys.dm_exec_sessions s WHERE {where}; OPEN c; FETCH NEXT FROM c INTO @id; WHILE @@FETCH_STATUS = 0 BEGIN EXEC('KILL ' + @id); SET @n += 1; FETCH NEXT FROM c INTO @id; END SELECT @n;", conn);
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task It_comes_back_by_itself_from_everything()
    {
        if (Nats is null) return;
        await InsertAsync(1);
        StartService();                       // NATS is not there yet
        await Task.Delay(6000);
        Assert.False(_service!.HasExited, Output);
        StartNats();
        await InsertAsync(2);
        await ArrivedAsync(2);
        // the lease: without it another instance may be active, so everything starts over
        Assert.True(await KillSessionsAsync($"s.program_name = N'logminer-state' AND s.database_id = DB_ID(N'{State}') AND EXISTS (SELECT 1 FROM sys.dm_tran_locks l WHERE l.request_session_id = s.session_id AND l.resource_type = N'APPLICATION')") > 0);
        for (var i = 3; i <= 6; i++) await InsertAsync(i);
        await ArrivedAsync(6);
        // the destination, in the middle of things
        StopNats();
        for (var i = 7; i <= 12; i++) await InsertAsync(i);
        await Task.Delay(4000);
        StartNats();
        for (var i = 13; i <= 15; i++) await InsertAsync(i);
        await ArrivedAsync(15);
        // the state database
        await TestServer.ExecAsync($"ALTER DATABASE [{State}] SET OFFLINE WITH ROLLBACK IMMEDIATE;");
        for (var i = 16; i <= 18; i++) await InsertAsync(i);
        await Task.Delay(5000);
        Assert.False(_service.HasExited, Output);
        await TestServer.ExecAsync($"ALTER DATABASE [{State}] SET ONLINE;");
        for (var i = 19; i <= 21; i++) await InsertAsync(i);
        await ArrivedAsync(21);
        // one of the captured databases
        await TestServer.ExecAsync($"ALTER DATABASE [{_databases[0]}] SET OFFLINE WITH ROLLBACK IMMEDIATE;");
        await Task.Delay(2000);
        await TestServer.ExecAsync($"ALTER DATABASE [{_databases[0]}] SET ONLINE;");
        for (var i = 22; i <= 24; i++) await InsertAsync(i);
        await ArrivedAsync(24);
        // every session it has
        Assert.True(await KillSessionsAsync($"s.host_process_id = {_service.Id}") > 0);
        for (var i = 25; i <= 27; i++) await InsertAsync(i);
        await ArrivedAsync(27);
        Assert.False(_service.HasExited, Output);
        var stored = await StoredAsync();
        Assert.All(stored, s => Assert.Equal(_databases[s.Id % 3], s.Db));
        // in order, last copies
        foreach (var db in _databases)
        {
            var ids = stored.Where(s => s.Db == db).Select(s => s.Id).ToList();
            Assert.Equal(ids.Distinct().Order(), ids.Select((id, at) => (id, at)).GroupBy(x => x.id).OrderBy(g => g.Max(x => x.at)).Select(g => g.Key));
        }
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM [{State}].dbo.gaps"));
        // killed outright, and a second start
        _service.Kill();
        await _service.WaitForExitAsync();
        for (var i = 28; i <= 30; i++) await InsertAsync(i);
        StartService();
        await ArrivedAsync(30);
    }

    private static async Task<int> ScalarAsync(string sql)
    {
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task A_standby_takes_over_and_publishing_goes_on_after_a_purge()
    {
        if (Nats is null) return;
        StartNats();
        StartService();
        for (var i = 1; i <= 6; i++) await InsertAsync(i);
        await ArrivedAsync(6);
        var first = _service!;
        StartService(); // the standby waits
        await Task.Delay(4000);
        first.Kill();
        await first.WaitForExitAsync();
        for (var i = 7; i <= 9; i++) await InsertAsync(i);
        await ArrivedAsync(9);
        // somebody purges the stream: what was acknowledged is gone
        await using (var nats = new NatsConnection(new NatsOpts { Url = $"nats://127.0.0.1:{_port}" }))
        {
            var js = new NatsJSContext(nats);
            await InsertAsync(10);
            for (var i = 0; i < 100 && !(await StoredAsync()).Any(s => s.Id == 10); i++) await Task.Delay(20);
            await (await js.GetStreamAsync("SVC")).PurgeAsync(new StreamPurgeRequest());
        }
        for (var i = 11; i <= 13; i++) await InsertAsync(i);
        for (var i = 0; i < 200 && (await StoredAsync()).Count < 3; i++) await Task.Delay(50);
        Assert.Equal([11, 12, 13], (await StoredAsync()).Select(s => s.Id).Order());
    }

    /// <summary>What an engine's loops meet that nobody expected: they go on, or the engine says that it cannot.</summary>
    private sealed class BrokenStore : ICheckpointStore
    {
        private readonly MemoryCheckpointStore _inner = new();
        public int Failures = 4;
        public Task<Checkpoint?> LoadAsync(string database, CancellationToken ct) => _inner.LoadAsync(database, ct);
        public Task SaveAsync(IReadOnlyCollection<Checkpoint> checkpoints, CancellationToken ct) => Interlocked.Decrement(ref Failures) switch
        {
            3 => throw new FormatException("unexpected"), 2 => throw new OperationCanceledException("not a shutdown"), 1 => throw new KeyNotFoundException("unexpected"), 0 => throw new NotSupportedException("unexpected"),
            _ => _inner.SaveAsync(checkpoints, ct),
        };
        public Checkpoint? Saved(string db) => LogMiner.Tests.Blocking.Run(() => _inner.LoadAsync(db, CancellationToken.None));
    }

    [Fact]
    public async Task No_exception_ends_checkpointing()
    {
        var store = new BrokenStore();
        await using var engine = new CaptureEngine(new CaptureOptions
        {
            ConnectionString = TestServer.ConnectionString(), DatabaseFilter = n => n == _databases[0], Workers = 1, CheckpointInterval = TimeSpan.FromMilliseconds(50),
            Settings = new CaptureSettings { StartPosition = StartPosition.Earliest }, SweepInterval = TimeSpan.FromMilliseconds(500),
        }, new DmvPollSignal(TestServer.ConnectionString(), TimeSpan.FromMilliseconds(10)), new(new EncoderOptions { ServerName = "it" }), new MemorySink(), store);
        await engine.StartAsync(CancellationToken.None);
        await InsertAsync(3);
        for (var i = 0; i < 300 && store.Saved(_databases[0]) is null; i++) await Task.Delay(20);
        Assert.NotNull(store.Saved(_databases[0]));
        Assert.False(engine.Failure.IsCompleted);
    }

    public async Task DisposeAsync()
    {
        Dispose();
        foreach (var d in _databases.Append(State)) await TestServer.DropDatabaseAsync(d);
        await TestServer.ExecAsync($"DECLARE @s sysname = (SELECT name FROM sys.server_event_sessions WHERE name LIKE N'logminer_doorbell_svc%{_tag}'); IF @s IS NOT NULL EXEC('DROP EVENT SESSION [' + @s + '] ON SERVER');");
    }

    public void Dispose()
    {
        foreach (var p in Process.GetProcessesByName("logminer")) if (p.MainModule?.FileName?.StartsWith(AppContext.BaseDirectory, StringComparison.OrdinalIgnoreCase) == true) { p.Kill(); p.WaitForExit(); }
        _service?.Dispose();
        _service = null;
        if (_nats is not null) StopNats();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
