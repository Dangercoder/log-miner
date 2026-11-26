using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using LogMiner.Nats;
using NATS.NKeys;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>A CA of its own and certificates it signed, as PEM files: what a NATS server with TLS is given, and a client that shows a certificate.</summary>
internal static class TestCertificates
{
    public static void Write(string dir)
    {
        var now = DateTimeOffset.UtcNow;
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=logminer test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        using var ca = caRequest.CreateSelfSigned(now.AddDays(-1), now.AddDays(2));
        File.WriteAllText(Path.Combine(dir, "ca.pem"), ca.ExportCertificatePem());
        Issue(ca, dir, "server", "CN=127.0.0.1", "1.3.6.1.5.5.7.3.1", now);
        Issue(ca, dir, "client", "CN=logminer", "1.3.6.1.5.5.7.3.2", now);
    }

    private static void Issue(X509Certificate2 ca, string dir, string name, string subject, string usage, DateTimeOffset now)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(IPAddress.Loopback);
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid(usage)], false));
        using var certificate = request.Create(ca, now.AddDays(-1), now.AddDays(1), RandomNumberGenerator.GetBytes(12));
        File.WriteAllText(Path.Combine(dir, name + ".pem"), certificate.ExportCertificatePem());
        File.WriteAllText(Path.Combine(dir, name + "-key.pem"), key.ExportPkcs8PrivateKeyPem());
    }
}

/// <summary>
/// Each way to log in to NATS that the settings offer, against a real nats-server that wants exactly that one: the sink starts (connects, finds or
/// creates its stream) with the right credentials and does not with wrong ones. (A credentials file of an operator-mode account is not here: it
/// needs an operator, an account and a resolver.) Passes trivially without a nats-server.
/// </summary>
public sealed class NatsLoginTests : IDisposable
{
    private static readonly string? Nats = Executables.NatsServer;
    private readonly string _dir = Directory.CreateTempSubdirectory("lm-login-").FullName;
    private readonly int _port = FreePort();
    private Process? _server;

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    private string P(string name) => Path.Combine(_dir, name);

    private async Task ServeAsync(string settings)
    {
        var conf = P("nats.conf");
        await File.WriteAllTextAsync(conf, $$"""
            listen: "127.0.0.1:{{_port}}"
            jetstream { store_dir: "{{P("js").Replace('\\', '/')}}" }
            {{settings}}
            """);
        _server = Process.Start(new ProcessStartInfo(Nats!, $"-c \"{conf}\"") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true })!;
        _server.BeginErrorReadLine(); _server.BeginOutputReadLine();
        for (var i = 0; i < 100; i++)
        {
            try { using var c = new TcpClient(); await c.ConnectAsync(IPAddress.Loopback, _port); return; }
            catch (SocketException) { await Task.Delay(50); }
        }
        Assert.Fail("nats-server did not start");
    }

    /// <summary>Starts a sink configured as the service configures it (the same names), and says whether it got as far as its stream.</summary>
    private async Task<bool> StartsAsync(params (string Key, string Value)[] settings)
    {
        var map = settings.Append(("Url", $"nats://127.0.0.1:{_port}")).ToDictionary(kv => kv.Item1, kv => kv.Item2);
        await using var sink = new JetStreamSink(new JetStreamSinkOptions
        {
            Connection = NatsConnectionOptions.Read(k => map.GetValueOrDefault(k)), StreamName = "LOGIN", AckTimeout = TimeSpan.FromSeconds(3),
        });
        try { await sink.StartAsync(CancellationToken.None); return true; }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { return false; }
    }

    [Fact]
    public async Task Token()
    {
        if (Nats is null) return;
        await ServeAsync("authorization { token: \"t0ken-of-the-test\" }");
        Assert.True(await StartsAsync(("Token", "t0ken-of-the-test")));
        Assert.False(await StartsAsync(("Token", "another-token")));
        Assert.False(await StartsAsync());
    }

    [Fact]
    public async Task NKey_seed_file()
    {
        if (Nats is null) return;
        using var user = KeyPair.CreatePair(PrefixByte.User);
        using var stranger = KeyPair.CreatePair(PrefixByte.User);
        await File.WriteAllTextAsync(P("user.nk"), user.GetSeed() + "\n");                                       // as nk -gen user writes it
        await File.WriteAllTextAsync(P("both.nk"), $"{user.GetSeed()}\n{user.GetPublicKey()}\n");               // nk -gen user -pubout
        await File.WriteAllTextAsync(P("stranger.nk"), stranger.GetSeed());
        await File.WriteAllTextAsync(P("public.nk"), user.GetPublicKey());
        await ServeAsync($"authorization {{ users: [ {{ nkey: \"{user.GetPublicKey()}\" }} ] }}");
        Assert.True(await StartsAsync(("NKeySeedFile", P("user.nk"))));
        Assert.True(await StartsAsync(("NKeySeedFile", P("both.nk"))));
        Assert.False(await StartsAsync(("NKeySeedFile", P("stranger.nk"))));
        var e = Assert.Throws<ArgumentException>(() => NatsConnectionOptions.Read(k => k == "NKeySeedFile" ? P("public.nk") : null));
        Assert.Contains("holds no user seed", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Client_certificate_and_a_CA_of_its_own()
    {
        if (Nats is null) return;
        TestCertificates.Write(_dir);
        string F(string name) => P(name).Replace('\\', '/');
        await ServeAsync($"tls {{ cert_file: \"{F("server.pem")}\", key_file: \"{F("server-key.pem")}\", ca_file: \"{F("ca.pem")}\", verify: true }}");
        // the refusals first: a TLS session that was accepted with a client certificate may be resumed by a later connection of the process
        Assert.False(await StartsAsync(("TlsCaFile", P("ca.pem")))); // no client certificate
        Assert.False(await StartsAsync(("TlsCertFile", P("client.pem")), ("TlsKeyFile", P("client-key.pem")))); // the server's CA is not trusted
        Assert.True(await StartsAsync(("TlsCaFile", P("ca.pem")), ("TlsCertFile", P("client.pem")), ("TlsKeyFile", P("client-key.pem"))));
    }

    public void Dispose()
    {
        if (_server is not null) { _server.Kill(); _server.WaitForExit(); _server.Dispose(); }
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
