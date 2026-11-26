using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using LogMiner.Nats;
using Microsoft.Data.SqlClient;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// The service as a pod runs it: configured by environment variables alone, over TCP with a SQL login that has only what
/// infra/sqlserver/create-login.sql grants (not sysadmin), to a NATS server that wants a user and a password over TLS with its own CA and allows
/// each user only the subjects docs/kubernetes.md lists. Then logminer-verify, with the same environment variables. Passes trivially without a
/// nats-server (LOGMINER_NATS_SERVER, ~/tools/nats-server).
/// </summary>
public sealed class ContainerSettingsTests : IAsyncLifetime, IDisposable
{
    private static readonly string? Nats = Executables.NatsServer;

    private readonly string _tag = Guid.NewGuid().ToString("N")[..8];
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lm-pod-" + Guid.NewGuid().ToString("N"));
    private readonly int _port = FreePort(), _health = FreePort();
    private readonly string _password = Guid.NewGuid().ToString("N") + "aA1!";
    private readonly string _natsPassword = Guid.NewGuid().ToString("N");
    private readonly System.Text.StringBuilder _output = new(), _natsOutput = new();
    private Process? _nats, _service;
    private string _db = "";

    private string Login => $"lm_it_pod_{_tag}";
    private string State => $"lm_it_pod_state_{_tag}";
    private string Ca => Path.Combine(_dir, "ca.pem");

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    public async Task InitializeAsync()
    {
        if (Nats is null) return;
        Directory.CreateDirectory(_dir);
        WriteCertificates();
        var conf = Path.Combine(_dir, "nats.conf");
        string Slashes(string p) => p.Replace('\\', '/');
        await File.WriteAllTextAsync(conf, $$"""
            listen: "127.0.0.1:{{_port}}"
            jetstream { store_dir: "{{Slashes(Path.Combine(_dir, "js"))}}" }
            tls { cert_file: "{{Slashes(Path.Combine(_dir, "server.pem"))}}", key_file: "{{Slashes(Path.Combine(_dir, "server-key.pem"))}}" }
            # the permissions of docs/kubernetes.md, "Prepare NATS" (stream POD instead of LOGMINER)
            authorization {
              users: [
                { user: "lm", password: "{{_natsPassword}}", permissions: {
                    publish: { allow: [ "cdc.>", "$JS.API.STREAM.INFO.POD", "$JS.API.STREAM.CREATE.POD", "$JS.API.STREAM.MSG.GET.POD" ] },
                    subscribe: { allow: [ "_INBOX.>" ] } } }
                { user: "lm-verify", password: "{{_natsPassword}}", permissions: {
                    publish: { allow: [ "$JS.API.STREAM.INFO.POD", "$JS.API.CONSUMER.CREATE.POD.>", "$JS.API.CONSUMER.DELETE.POD.>", "$JS.API.CONSUMER.MSG.NEXT.POD.>" ] },
                    subscribe: { allow: [ "_INBOX.>" ] } } }
                { user: "admin", password: "{{_natsPassword}}" }
              ]
            }
            """);
        _nats = Process.Start(new ProcessStartInfo(Nats, $"-c \"{conf}\"") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true })!;
        _nats.OutputDataReceived += (_, e) => { lock (_natsOutput) _natsOutput.AppendLine(e.Data); };
        _nats.ErrorDataReceived += (_, e) => { lock (_natsOutput) _natsOutput.AppendLine(e.Data); };
        _nats.BeginErrorReadLine(); _nats.BeginOutputReadLine();

        await TestServer.ExecAsync($"CREATE DATABASE [{State}];");
        var db = await TestServer.CreateDatabaseAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v nvarchar(20) NULL);", "t");
        _db = $"lm_it_pod_{_tag}_0";
        await TestServer.ExecAsync($"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [{db}] MODIFY NAME = [{_db}]; ALTER DATABASE [{_db}] SET MULTI_USER;");
        // the script as an operator runs it with sqlcmd (sqlcmd's variables and GO, done here)
        var script = (await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "create-login.sql")))
            .Replace("$(Login)", Login, StringComparison.Ordinal).Replace("$(Password)", _password, StringComparison.Ordinal)
            .Replace("$(Databases)", $"lm_it_pod_{_tag}_%", StringComparison.Ordinal).Replace("$(StateDatabase)", State, StringComparison.Ordinal)
            .Replace("$(Sysadmin)", "0", StringComparison.Ordinal);
        foreach (var batch in Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline).Where(b => !string.IsNullOrWhiteSpace(b))) await TestServer.ExecAsync(batch);
    }

    /// <summary>A CA of its own, and the NATS server's certificate for 127.0.0.1 signed by it.</summary>
    private void WriteCertificates()
    {
        var now = DateTimeOffset.UtcNow;
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=logminer test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        using var ca = caRequest.CreateSelfSigned(now.AddDays(-1), now.AddDays(2));
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=127.0.0.1", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(IPAddress.Loopback);
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new System.Security.Cryptography.Oid("1.3.6.1.5.5.7.3.1")], false));
        using var server = request.Create(ca, now.AddDays(-1), now.AddDays(1), RandomNumberGenerator.GetBytes(12));
        File.WriteAllText(Ca, ca.ExportCertificatePem());
        File.WriteAllText(Path.Combine(_dir, "server.pem"), server.ExportCertificatePem());
        File.WriteAllText(Path.Combine(_dir, "server-key.pem"), key.ExportPkcs8PrivateKeyPem());
    }

    /// <summary>The instance the tests use (LOGMINER_IT_CS), as host and port: the service is configured in parts, and has to find the same one.</summary>
    private static (string Host, string? Port) TestSql()
    {
        var source = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(TestServer.ConnectionString()).DataSource;
        if (source.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase)) source = source[4..];
        var comma = source.IndexOf(',', StringComparison.Ordinal);
        var host = comma < 0 ? source : source[..comma];
        return (host is "." or "(local)" or "localhost" or "" ? "localhost" : host, comma < 0 ? null : source[(comma + 1)..].Trim());
    }

    private static string SqlDataSource => TestSql() is var (host, port) ? "tcp:" + host + (port is null ? "" : "," + port) : "";

    private Dictionary<string, string> PodEnvironment(string sqlPassword, string natsPassword)
    {
        var environment = PodSettings(sqlPassword, natsPassword);
        if (TestSql().Port is { } port) environment["LogMiner__Sql__Port"] = port;
        return environment;
    }

    private Dictionary<string, string> PodSettings(string sqlPassword, string natsPassword) => new(StringComparer.Ordinal)
    {
        ["LogMiner__ServerName"] = "pod-" + _tag,
        ["LogMiner__Databases__0"] = $"lm_it_pod_{_tag}_*",
        ["LogMiner__Sql__Host"] = TestSql().Host,
        ["LogMiner__Sql__User"] = Login,
        ["LogMiner__Sql__Password"] = sqlPassword,
        ["LogMiner__Sql__TrustServerCertificate"] = "true", // the local instance has a certificate of its own making
        ["LogMiner__State__Database"] = State,
        ["LogMiner__SelfTest"] = "Off",
        ["LogMiner__StartPosition"] = "Earliest",
        ["LogMiner__ReleaseIntervalMs"] = "500",
        ["LogMiner__CheckpointIntervalMs"] = "200",
        ["LogMiner__HealthPort"] = _health.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["LogMiner__Sink__NatsJetStream__Url"] = $"nats://127.0.0.1:{_port}",
        ["LogMiner__Sink__NatsJetStream__Stream"] = "POD",
        ["LogMiner__Sink__NatsJetStream__User"] = "lm",
        ["LogMiner__Sink__NatsJetStream__Password"] = natsPassword,
        ["LogMiner__Sink__NatsJetStream__TlsMode"] = "Require",
        ["LogMiner__Sink__NatsJetStream__TlsCaFile"] = Ca,
        ["LogMiner__Sink__NatsJetStream__AckTimeoutSeconds"] = "3",
    };

    private void StartService(Dictionary<string, string> environment)
    {
        lock (_output) _output.Clear();
        var info = new ProcessStartInfo(Executables.Service) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true, WorkingDirectory = _dir };
        foreach (var (k, v) in environment) info.Environment[k] = v;
        _service = Process.Start(info)!;
        _service.OutputDataReceived += (_, e) => { lock (_output) _output.AppendLine(e.Data); };
        _service.ErrorDataReceived += (_, e) => { lock (_output) _output.AppendLine(e.Data); };
        _service.BeginOutputReadLine(); _service.BeginErrorReadLine();
    }

    private void StopService()
    {
        if (_service is null) return;
        if (!_service.HasExited) { _service.Kill(); _service.WaitForExit(); }
        _service.Dispose();
        _service = null;
    }

    private string Output { get { lock (_output) return _output.ToString(); } }
    private string NatsOutput { get { lock (_natsOutput) return _natsOutput.ToString(); } }

    private async Task UntilAsync(Func<bool> condition, string what, int seconds = 60)
    {
        var deadline = Stopwatch.StartNew();
        while (!condition())
        {
            if (deadline.Elapsed > TimeSpan.FromSeconds(seconds)) Assert.Fail($"{what} within {seconds} s; service output:\n{Output}\nNATS server:\n{NatsOutput}");
            await Task.Delay(100);
        }
    }

    private async Task<int> ReadyAsync()
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        try { using var r = await http.GetAsync(new Uri($"http://127.0.0.1:{_health}/readyz")); return (int)r.StatusCode; }
        catch (HttpRequestException) { return 0; }
        catch (TaskCanceledException) { return 0; }
    }

    private NatsConnection Client() => new(NatsConnectionOptions.Read(k => k switch
    {
        "Url" => $"nats://127.0.0.1:{_port}", "User" => "admin", "Password" => _natsPassword, "TlsMode" => "Require", "TlsCaFile" => Ca, _ => null,
    }).ApplyTo(NatsOpts.Default));

    [Fact]
    public async Task Configured_by_environment_variables_alone_it_captures_and_verify_reads_the_stream()
    {
        if (Nats is null) return;
        StartService(PodEnvironment(_password, _natsPassword));
        var ready = 0;
        await UntilAsync(() => (ready = ReadyAsync().GetAwaiter().GetResult()) == 200 && Output.Contains("capturing 1 databases", StringComparison.Ordinal), "ready and capturing");
        Assert.Contains($"SQL Server: {SqlDataSource} as login '{Login}', encrypted (Mandatory), server certificate NOT checked", Output, StringComparison.Ordinal);
        Assert.Contains($"sink nats-jetstream: stream POD on nats://127.0.0.1:{_port} as user 'lm', TLS Require, CA {Ca}", Output, StringComparison.Ordinal);

        await using (var sql = new SqlConnection(TestServer.ConnectionString(_db)))
        {
            await sql.OpenAsync();
            await using var cmd = new SqlCommand("INSERT dbo.t VALUES (1, N'pod'); UPDATE dbo.t SET v = N'pod 2' WHERE id = 1;", sql);
            await cmd.ExecuteNonQueryAsync();
        }
        await using var nats = Client();
        await nats.ConnectAsync();
        var js = new NatsJSContext(nats);
        long messages = 0;
        await UntilAsync(() => (messages = js.GetStreamAsync("POD").AsTask().GetAwaiter().GetResult().Info.State.Messages) >= 2, "the insert and the update in the stream");
        Assert.Equal(2L, messages);
        var first = await (await js.GetStreamAsync("POD")).GetAsync(new StreamMsgGetRequest { Seq = 1 });
        Assert.StartsWith($"cdc.pod-{_tag}.{_db}.dbo.t", first.Message.Subject, StringComparison.Ordinal);

        // logminer-verify in the same pod: the service's environment variables, no arguments
        var info = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, OperatingSystem.IsWindows() ? "logminer-verify.exe" : "logminer-verify"))
        { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true, WorkingDirectory = _dir };
        foreach (var (k, v) in PodEnvironment(_password, _natsPassword)) info.Environment[k] = v;
        info.Environment["LogMiner__Sink__NatsJetStream__User"] = "lm-verify"; // its own, read-only user
        using var verify = Process.Start(info)!;
        var said = Task.WhenAll(verify.StandardOutput.ReadToEndAsync(), verify.StandardError.ReadToEndAsync());
        using (var patience = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
        {
            try { await verify.WaitForExitAsync(patience.Token); }
            catch (OperationCanceledException) { verify.Kill(); await verify.WaitForExitAsync(); Assert.Fail($"logminer-verify did not end within 60 s:\n{string.Concat(await said)}\nNATS server:\n{NatsOutput}"); }
        }
        var text = string.Concat(await said);
        Assert.True(verify.ExitCode == 0, $"logminer-verify exit code {verify.ExitCode}:\n{text}\nNATS server:\n{NatsOutput}");
        Assert.Contains($"stream POD at nats://127.0.0.1:{_port}: 2 message(s)", text, StringComparison.Ordinal);
        // it read and compared them (a fetch that is refused looks like an empty stream: "nothing to verify")
        Assert.Contains("RESULT: no difference", text, StringComparison.Ordinal);

        // releasing the log asks the stream whether it holds what it acknowledged (several rounds at ReleaseIntervalMs 500)
        await Task.Delay(TimeSpan.FromSeconds(10));
        Assert.DoesNotContain("releasing log failed", Output, StringComparison.Ordinal);
        Assert.DoesNotContain("IS NOT RELEASED", Output, StringComparison.Ordinal);
        Assert.DoesNotContain("Permissions Violation", NatsOutput, StringComparison.Ordinal);
        Assert.DoesNotContain(_natsPassword, text + Output, StringComparison.Ordinal);
        Assert.DoesNotContain(_password, text + Output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_wrong_password_is_said_in_the_log_and_the_password_is_not()
    {
        if (Nats is null) return;
        const string wrong = "Wrong-Password-1x";
        StartService(PodEnvironment(wrong, _natsPassword));
        await UntilAsync(() => Output.Contains($"Login failed for user '{Login}'", StringComparison.Ordinal), "SQL Server's refusal in the log");
        Assert.NotEqual(200, await ReadyAsync());
        StopService();

        StartService(PodEnvironment(_password, wrong));
        await UntilAsync(() => Output.Contains("capture stopped", StringComparison.Ordinal), "a run that stops for NATS");
        Assert.Matches("(?i)authorization", Output);
        Assert.NotEqual(200, await ReadyAsync());
        Assert.DoesNotContain(wrong, Output, StringComparison.Ordinal);
        Assert.DoesNotContain(_password, Output, StringComparison.Ordinal);
    }

    public async Task DisposeAsync()
    {
        Dispose();
        if (Nats is null) return;
        if (_db.Length > 0) await TestServer.DropDatabaseAsync(_db);
        await TestServer.DropDatabaseAsync(State);
        await TestServer.ExecAsync($"""
            DECLARE @s sysname = (SELECT name FROM sys.server_event_sessions WHERE name LIKE N'logminer_doorbell_pod%{_tag}'); IF @s IS NOT NULL EXEC('DROP EVENT SESSION [' + @s + '] ON SERVER');
            DECLARE @k nvarchar(max) = (SELECT STRING_AGG(CONCAT('KILL ', session_id), ';') FROM sys.dm_exec_sessions WHERE login_name = N'{Login}'); EXEC (@k);
            IF DATABASE_PRINCIPAL_ID(N'{Login}') IS NOT NULL DROP USER [{Login}];
            IF SUSER_ID(N'{Login}') IS NOT NULL DROP LOGIN [{Login}];
            """);
    }

    public void Dispose()
    {
        StopService();
        if (_nats is not null) { _nats.Kill(); _nats.WaitForExit(); _nats.Dispose(); _nats = null; }
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
