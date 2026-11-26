using NATS.Client.Core;
using NATS.NKeys;

namespace LogMiner.Nats;

/// <summary>
/// Where the NATS servers are and how to log in to them. The service reads it from LogMiner:Sink:NatsJetStream (appsettings.json, or the environment
/// variables LogMiner__Sink__NatsJetStream__*), logminer-verify from the same environment variables, so one Kubernetes Secret serves both.
/// Empty values count as not set: a template with empty secrets changes nothing.
/// </summary>
public sealed record NatsConnectionOptions
{
    /// <summary>Prefix of the environment variables (and the configuration section's path, with ':' for '__').</summary>
    public const string EnvironmentPrefix = "LogMiner__Sink__NatsJetStream__";

    /// <summary>One server, or the servers of a cluster separated by commas: nats://, tls://, ws:// or wss://.</summary>
    public string Url { get; init; } = "nats://127.0.0.1:4222";
    public string? User { get; init; }
    public string? Password { get; init; }
    public string? Token { get; init; }
    /// <summary>File holding a user's NKey seed (a line beginning with SU), as nk or nsc write it; a line with the public key may follow.</summary>
    public string? NKeySeedFile { get; init; }
    /// <summary>Credentials file (JWT and NKey seed) of a user of an operator-mode account.</summary>
    public string? CredentialsFile { get; init; }
    /// <summary>Null: NATS.Net's Auto (TLS with a tls:// URL, a CA or a client certificate, or when the server asks for it).</summary>
    public TlsMode? TlsMode { get; init; }
    /// <summary>PEM file of the CA (or CAs) that signed the server's certificate, when the system does not trust it.</summary>
    public string? TlsCaFile { get; init; }
    /// <summary>Client certificate and its key (PEM), when the server checks clients by certificate.</summary>
    public string? TlsCertFile { get; init; }
    public string? TlsKeyFile { get; init; }
    /// <summary>Accept any server certificate. For a test only: anyone in between could read and change the stream.</summary>
    public bool TlsInsecureSkipVerify { get; init; }

    /// <summary>
    /// Reads and checks the settings by name ("Url", "User", "TlsCaFile", ...). Throws an <see cref="ArgumentException"/> that names the
    /// environment variable when something is wrong; a file is checked to be there, so a missing Secret shows at start, not at the first reconnect.
    /// </summary>
    public static NatsConnectionOptions Read(Func<string, string?> setting)
    {
        ArgumentNullException.ThrowIfNull(setting);
        string? Get(string key) => setting(key) is { } v && !string.IsNullOrWhiteSpace(v) ? v : null;
        string? Text(string key) => Get(key)?.Trim();
        string? File(string key)
        {
            if (Text(key) is not { } path) return null;
            return System.IO.File.Exists(path) ? path : throw new ArgumentException($"{EnvironmentPrefix}{key}: the file {path} is not there (is the Secret mounted?)");
        }

        var url = Text("Url") ?? new NatsConnectionOptions().Url;
        foreach (var server in url.Split(',', StringSplitOptions.TrimEntries))
        {
            if (!Uri.TryCreate(server, UriKind.Absolute, out var uri) || uri.Scheme is not ("nats" or "tls" or "ws" or "wss") || uri.Host.Length == 0)
                throw new ArgumentException($"{EnvironmentPrefix}Url: '{Redact(server)}' is not a NATS URL (nats://host:4222; several separated by commas)");
        }

        TlsMode? mode = null;
        if (Text("TlsMode") is { } m)
        {
            mode = Enum.TryParse<TlsMode>(m, ignoreCase: true, out var parsed) && Enum.IsDefined(parsed)
                ? parsed : throw new ArgumentException($"{EnvironmentPrefix}TlsMode: '{m}' is not one of {string.Join(", ", Enum.GetNames<TlsMode>())}");
        }
        var insecure = false;
        if (Text("TlsInsecureSkipVerify") is { } i && !bool.TryParse(i, out insecure))
            throw new ArgumentException($"{EnvironmentPrefix}TlsInsecureSkipVerify: '{i}' is neither true nor false");

        var o = new NatsConnectionOptions
        {
            Url = url,
            User = Text("User"),
            Password = Get("Password"),
            Token = Get("Token"),
            NKeySeedFile = File("NKeySeedFile"),
            CredentialsFile = File("CredentialsFile"),
            TlsMode = mode,
            TlsCaFile = File("TlsCaFile"),
            TlsCertFile = File("TlsCertFile"),
            TlsKeyFile = File("TlsKeyFile"),
            TlsInsecureSkipVerify = insecure,
        };

        if ((o.User is null) != (o.Password is null))
            throw new ArgumentException($"{EnvironmentPrefix}User and {EnvironmentPrefix}Password go together");
        var ways = new[] { (o.User is not null, "User/Password"), (o.Token is not null, "Token"), (o.NKeySeedFile is not null, "NKeySeedFile"), (o.CredentialsFile is not null, "CredentialsFile") }
            .Where(w => w.Item1).Select(w => w.Item2).ToList();
        if (ways.Count > 1)
            throw new ArgumentException($"more than one way to log in to NATS is set ({string.Join(", ", ways)}): set one of User and Password, Token, NKeySeedFile or CredentialsFile");
        if ((o.TlsCertFile is null) != (o.TlsKeyFile is null))
            throw new ArgumentException($"{EnvironmentPrefix}TlsCertFile and {EnvironmentPrefix}TlsKeyFile go together");
        if (mode == NATS.Client.Core.TlsMode.Disable && (o.TlsCaFile ?? o.TlsCertFile) is not null)
            throw new ArgumentException($"{EnvironmentPrefix}TlsMode is Disable, but a CA or client certificate is set");
        if (o.NKeySeedFile is not null) _ = NKey(o.NKeySeedFile);
        return o;
    }

    /// <summary>The seed in the file and its public key. (NATS.Net's own NKeyFile did not log in with a file that holds the seed alone, as nk and
    /// nsc write it: measured with NATS.Net 3.4.1.)</summary>
    private static (string Seed, string PublicKey) NKey(string file)
    {
        var seed = File.ReadLines(file).Select(l => l.Trim()).FirstOrDefault(l => l.StartsWith("SU", StringComparison.Ordinal))
            ?? throw new ArgumentException($"{EnvironmentPrefix}NKeySeedFile: {file} holds no user seed (a line beginning with SU)");
        try
        {
            using var pair = KeyPair.FromSeed(seed);
            return (seed, pair.GetPublicKey());
        }
        catch (Exception ex) // only its type is said: its message might hold the seed
        {
            throw new ArgumentException($"{EnvironmentPrefix}NKeySeedFile: the seed in {file} cannot be read ({ex.GetType().Name})", ex);
        }
    }

    /// <summary>The connection options of NATS.Net with these servers, credentials and TLS.</summary>
    public NatsOpts ApplyTo(NatsOpts opts)
    {
        ArgumentNullException.ThrowIfNull(opts);
        var auth = NatsAuthOpts.Default;
        if (User is not null) auth = auth with { Username = User, Password = Password };
        else if (Token is not null) auth = auth with { Token = Token };
        else if (NKeySeedFile is not null)
        {
            var (seed, publicKey) = NKey(NKeySeedFile); // read at every connection: a rotated Secret is taken up
            auth = auth with { Seed = seed, NKey = publicKey };
        }
        else if (CredentialsFile is not null) auth = auth with { CredsFile = CredentialsFile };
        var tls = NatsTlsOpts.Default;
        if (TlsMode is { } mode) tls = tls with { Mode = mode };
        if (TlsCaFile is not null) tls = tls with { CaFile = TlsCaFile };
        if (TlsCertFile is not null) tls = tls with { CertFile = TlsCertFile, KeyFile = TlsKeyFile };
        if (TlsInsecureSkipVerify) tls = tls with { InsecureSkipVerify = true };
        return opts with { Url = Url, AuthOpts = auth, TlsOpts = tls };
    }

    /// <summary>The servers, without a user and password a URL may hold.</summary>
    public string Servers => string.Join(",", Url.Split(',', StringSplitOptions.TrimEntries).Select(Redact));

    /// <summary>For the log: servers, how it logs in and TLS, without secrets.</summary>
    public string Describe()
    {
        var servers = Servers;
        var who = User is not null ? $"as user '{User}'"
            : Token is not null ? "with a token"
            : NKeySeedFile is not null ? $"with the NKey seed in {NKeySeedFile}"
            : CredentialsFile is not null ? $"with the credentials in {CredentialsFile}"
            : Url.Contains('@', StringComparison.Ordinal) ? "with the user in the URL"
            : "without credentials";
        var tls = $"TLS {TlsMode?.ToString() ?? "Auto"}";
        if (TlsCaFile is not null) tls += $", CA {TlsCaFile}";
        if (TlsCertFile is not null) tls += $", client certificate {TlsCertFile}";
        if (TlsInsecureSkipVerify) tls += ", server certificate NOT checked";
        return $"{servers} {who}, {tls}";
    }

    /// <summary>A URL without the user and password it may hold (nats://user:password@host).</summary>
    private static string Redact(string url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.UserInfo.Length > 0 ? url.Replace(uri.UserInfo + "@", "***@", StringComparison.Ordinal) : url;
}
