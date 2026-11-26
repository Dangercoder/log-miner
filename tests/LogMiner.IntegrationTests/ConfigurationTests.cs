using System.Diagnostics;
using Hegel;
using Hegel.Xunit;
using LogMiner.Nats;
using LogMiner.Service;
using Microsoft.Data.SqlClient;
using NATS.Client.Core;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>How the connections to SQL Server and NATS are configured (a container: LogMiner__* environment variables). No server is needed.</summary>
public sealed class ConfigurationTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("lm-cfg-").FullName;

    private static LogMinerSettings Settings(Action<SqlSettings> sql, string? connectionString = null)
    {
        var s = new LogMinerSettings { ConnectionString = connectionString };
        sql(s.Sql);
        return s;
    }

    [Fact]
    public void Parts_make_an_encrypted_checked_connection_to_master()
    {
        var b = new SqlConnectionStringBuilder(SqlTarget.Resolve(Settings(q => { q.Host = "sql01.example.internal"; q.Port = 1433; q.User = "logminer"; q.Password = "p;w'=\"x"; }), onWindows: false));
        Assert.Equal("tcp:sql01.example.internal,1433", b.DataSource);
        Assert.Equal("master", b.InitialCatalog);
        Assert.False(b.IntegratedSecurity);
        Assert.Equal(("logminer", "p;w'=\"x"), (b.UserID, b.Password));
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, b.Encrypt);
        Assert.False(b.TrustServerCertificate);
        Assert.Equal("logminer", b.ApplicationName);
        Assert.Equal(15, b.ConnectTimeout);
    }

    [Fact]
    public void Without_a_login_it_is_the_process_identity_and_options_are_kept()
    {
        var b = new SqlConnectionStringBuilder(SqlTarget.Resolve(Settings(q =>
        {
            q.Host = @"sql01\tenants"; q.Encrypt = "strict"; q.HostNameInCertificate = "sql01.example.internal"; q.ConnectTimeoutSeconds = 30;
            q.Options = "MultiSubnetFailover=true;Application Name=lm-a";
        }), onWindows: false));
        Assert.Equal(@"tcp:sql01\tenants", b.DataSource);
        Assert.True(b.IntegratedSecurity);
        Assert.Equal(SqlConnectionEncryptOption.Strict, b.Encrypt);
        Assert.Equal("sql01.example.internal", b.HostNameInCertificate);
        Assert.Equal(30, b.ConnectTimeout);
        Assert.True(b.MultiSubnetFailover);
        Assert.Equal("lm-a", b.ApplicationName);
    }

    [Fact]
    public void Nothing_set_is_shared_memory_on_Windows_and_an_error_elsewhere()
    {
        Assert.Equal(SqlTarget.LocalDefault, SqlTarget.Resolve(new LogMinerSettings(), onWindows: true));
        var e = Assert.Throws<ArgumentException>(() => SqlTarget.Resolve(new LogMinerSettings(), onWindows: false));
        Assert.Contains("LogMiner__Sql__Host", e.Message, StringComparison.Ordinal);
        // a whole connection string is used as it is, anywhere
        Assert.Equal("Data Source=x;Initial Catalog=master", SqlTarget.Resolve(new LogMinerSettings { ConnectionString = "Data Source=x;Initial Catalog=master" }, onWindows: false));
    }

    [Theory]
    [InlineData("both", "LogMiner__ConnectionString and LogMiner__Sql__Host, LogMiner__Sql__Password are both set")]
    [InlineData("no host", "LogMiner__Sql__Host is not set")]
    [InlineData("user only", "LogMiner__Sql__User and LogMiner__Sql__Password go together")]
    [InlineData("password only", "LogMiner__Sql__User and LogMiner__Sql__Password go together")]
    [InlineData("encrypt", "LogMiner__Sql__Encrypt: 'sometimes'")]
    [InlineData("port", "LogMiner__Sql__Port: 70000")]
    [InlineData("owned option", "must not set 'Password'")]
    [InlineData("bad option", "LogMiner__Sql__Options is not a valid connection string")]
    [InlineData("certificate", "LogMiner__Sql__ServerCertificate: the file")]
    public void A_wrong_setting_is_named_and_the_password_is_not(string what, string expected)
    {
        const string secret = "S3cret-Value";
        var s = what switch
        {
            "both" => Settings(q => { q.Host = "h"; q.Password = secret; }, connectionString: "Data Source=h"),
            "no host" => Settings(q => { q.User = "u"; q.Password = secret; }),
            "user only" => Settings(q => { q.Host = "h"; q.User = "u"; }),
            "password only" => Settings(q => { q.Host = "h"; q.Password = secret; }),
            "encrypt" => Settings(q => { q.Host = "h"; q.Encrypt = "sometimes"; q.User = "u"; q.Password = secret; }),
            "port" => Settings(q => { q.Host = "h"; q.Port = 70000; q.User = "u"; q.Password = secret; }),
            "owned option" => Settings(q => { q.Host = "h"; q.Options = $"Password={secret}"; }),
            "bad option" => Settings(q => { q.Host = "h"; q.Options = $"NoSuchKeyword={secret}"; }),
            "certificate" => Settings(q => { q.Host = "h"; q.ServerCertificate = Path.Combine(_dir, "none.cer"); q.User = "u"; q.Password = secret; }),
            _ => throw new ArgumentOutOfRangeException(nameof(what)),
        };
        var e = Assert.Throws<ArgumentException>(() => SqlTarget.Resolve(s, onWindows: false));
        Assert.Contains(expected, e.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, e.Message, StringComparison.Ordinal);
    }

    [HegelTest]
    public void What_is_logged_never_holds_a_password(TestCase tc)
    {
        var password = tc.Draw(Generators.Text().MinSize(1).MaxSize(40), "password");
        if (string.IsNullOrWhiteSpace(password) || password.Length < 3) return; // shorter ones are found in any text
        var settings = Settings(q => { q.Host = "sql01"; q.User = "logminer"; q.Password = password; });
        if (password.Contains('\0', StringComparison.Ordinal))
        {
            // no connection string holds it (nor an environment variable): a configuration error that does not show it
            var e = Assert.Throws<ArgumentException>(() => SqlTarget.Resolve(settings, onWindows: false));
            Assert.DoesNotContain(password, e.Message, StringComparison.Ordinal);
            return;
        }
        var cs = SqlTarget.Resolve(settings, onWindows: false);
        Assert.Equal(password, new SqlConnectionStringBuilder(cs).Password);
        Assert.DoesNotContain(password, SqlTarget.Describe(cs), StringComparison.Ordinal);
        var nats = NatsConnectionOptions.Read(k => k switch { "User" => "lm", "Password" => password, _ => null });
        Assert.DoesNotContain(password, nats.Describe(), StringComparison.Ordinal);
        Assert.Equal(password, nats.ApplyTo(NatsOpts.Default).AuthOpts.Password);
    }

    [Fact]
    public void Nats_settings_become_the_client_options()
    {
        File.WriteAllText(Path.Combine(_dir, "ca.pem"), "x");
        var o = NatsConnectionOptions.Read(k => k switch
        {
            "Url" => "tls://n1:4222, tls://n2:4222", "User" => "lm", "Password" => "pw", "TlsMode" => "require", "TlsCaFile" => Path.Combine(_dir, "ca.pem"), _ => " ",
        }).ApplyTo(NatsOpts.Default);
        Assert.Equal("tls://n1:4222, tls://n2:4222", o.Url);
        Assert.Equal(("lm", "pw"), (o.AuthOpts.Username, o.AuthOpts.Password));
        Assert.Equal(TlsMode.Require, o.TlsOpts.Mode);
        Assert.Equal(Path.Combine(_dir, "ca.pem"), o.TlsOpts.CaFile);
        Assert.False(o.TlsOpts.InsecureSkipVerify);
        // nothing set (or only empty values): the defaults of before
        var none = NatsConnectionOptions.Read(_ => "");
        Assert.Equal("nats://127.0.0.1:4222", none.Url);
        Assert.Equal(NatsAuthOpts.Default, none.ApplyTo(NatsOpts.Default).AuthOpts);
        Assert.Equal("nats://127.0.0.1:4222 without credentials, TLS Auto", none.Describe());
        Assert.Equal("nats://***@h:4222 with the user in the URL, TLS Auto", NatsConnectionOptions.Read(k => k == "Url" ? "nats://u:secret@h:4222" : null).Describe());
    }

    [Theory]
    [InlineData("Url=http://n1:4222", "is not a NATS URL")]
    [InlineData("User=lm", "LogMiner__Sink__NatsJetStream__User and LogMiner__Sink__NatsJetStream__Password go together")]
    [InlineData("Password=pw", "go together")]
    [InlineData("User=lm;Password=pw;Token=t", "more than one way to log in to NATS is set (User/Password, Token)")]
    [InlineData("CredentialsFile=missing.creds", "LogMiner__Sink__NatsJetStream__CredentialsFile: the file")]
    [InlineData("TlsMode=always", "LogMiner__Sink__NatsJetStream__TlsMode: 'always' is not one of")]
    [InlineData("TlsCertFile=ca.pem", "TlsCertFile and LogMiner__Sink__NatsJetStream__TlsKeyFile go together")]
    [InlineData("TlsMode=Disable;TlsCaFile=ca.pem", "TlsMode is Disable, but a CA")]
    [InlineData("TlsInsecureSkipVerify=yes", "TlsInsecureSkipVerify: 'yes' is neither")]
    public void A_wrong_nats_setting_is_named(string settings, string expected)
    {
        File.WriteAllText(Path.Combine(_dir, "ca.pem"), "x");
        var map = settings.Split(';').Select(kv => kv.Split('=', 2)).ToDictionary(kv => kv[0], kv => kv[1].EndsWith(".pem", StringComparison.Ordinal) || kv[1].EndsWith(".creds", StringComparison.Ordinal) ? Path.Combine(_dir, kv[1]) : kv[1]);
        var e = Assert.Throws<ArgumentException>(() => NatsConnectionOptions.Read(k => map.GetValueOrDefault(k)));
        Assert.Contains(expected, e.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("pw", e.Message.Replace("pwd", "", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    /// <summary>The real program: a configuration that cannot work ends it at once with exit code 2 and says why, never with a secret.</summary>
    [Theory]
    [InlineData("LogMiner__ConnectionString=Data Source=h;Password=S3cret-Value|LogMiner__Sql__Host=h", "are both set")]
    [InlineData("LogMiner__Sql__Host=h|LogMiner__Sql__User=u", "LogMiner__Sql__User and LogMiner__Sql__Password go together")]
    [InlineData("LogMiner__Sql__Host=h|LogMiner__Sql__Port=fourteen|LogMiner__Sql__User=u|LogMiner__Sql__Password=S3cret-Value", "LogMiner:Sql:Port")]
    [InlineData("LogMiner__Sql__Host=h|LogMiner__Sink__NatsJetStream__Password=S3cret-Value", "go together")]
    [InlineData("LogMiner__Sql__Host=h|LogMiner__Sink__NatsJetStream__TlsCaFile=/no/such/ca.pem", "TlsCaFile: the file /no/such/ca.pem is not there")]
    [InlineData("LogMiner__ServerName= |LogMiner__Sql__Host=h", "ServerName is not set: set LogMiner__ServerName")]
    public async Task The_service_exits_with_2_and_names_the_setting(string environment, string expected)
    {
        var info = new ProcessStartInfo(Executables.Service) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true, WorkingDirectory = _dir };
        foreach (var kv in environment.Split('|').Select(p => p.Split('=', 2))) info.Environment[kv[0]] = kv[1];
        if (!info.Environment.ContainsKey("LogMiner__ServerName")) info.Environment["LogMiner__ServerName"] = "cfg-test";
        using var p = Process.Start(info)!;
        var output = Task.WhenAll(p.StandardOutput.ReadToEndAsync(), p.StandardError.ReadToEndAsync());
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await p.WaitForExitAsync(cts.Token);
        var text = string.Concat(await output);
        Assert.True(p.ExitCode == 2, $"exit code {p.ExitCode}:\n{text}");
        Assert.Contains(expected, text, StringComparison.Ordinal);
        Assert.DoesNotContain("S3cret-Value", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_self_test_reads_the_same_settings()
    {
        var info = new ProcessStartInfo(Executables.Service, "--self-test") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true, WorkingDirectory = _dir };
        info.Environment["LogMiner__Sql__Host"] = "h";
        info.Environment["LogMiner__Sql__Password"] = "S3cret-Value";
        using var p = Process.Start(info)!;
        var output = Task.WhenAll(p.StandardOutput.ReadToEndAsync(), p.StandardError.ReadToEndAsync());
        await p.WaitForExitAsync();
        var text = string.Concat(await output);
        Assert.Equal(2, p.ExitCode);
        Assert.Contains("LogMiner__Sql__User and LogMiner__Sql__Password go together", text, StringComparison.Ordinal);
        Assert.DoesNotContain("S3cret-Value", text, StringComparison.Ordinal);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
