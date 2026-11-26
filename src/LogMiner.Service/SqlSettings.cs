using Microsoft.Data.SqlClient;

namespace LogMiner.Service;

/// <summary>
/// The connection to the SQL Server instance in parts (LogMiner:Sql, or the environment variables LogMiner__Sql__*): what a container is given,
/// the password from a Secret. Empty values count as not set.
/// </summary>
public sealed class SqlSettings
{
    /// <summary>Host name or address of the instance; host\instance for a named instance (found through the SQL Browser) when there is no Port.</summary>
    public string? Host { get; set; }
    /// <summary>TCP port; empty: 1433, or the named instance's port.</summary>
    public int? Port { get; set; }
    /// <summary>SQL login, with <see cref="Password"/>. Empty: the process's own identity (its Windows account; Kerberos on Linux).</summary>
    public string? User { get; set; }
    public string? Password { get; set; }
    /// <summary>Mandatory (default: TLS, after TDS's pre-login), Strict (TDS 8: TLS from the first byte, SQL Server 2022 and later) or Optional
    /// (only if the server insists: the rows of every captured table would cross the network readable).</summary>
    public string? Encrypt { get; set; }
    /// <summary>Accept the server's certificate without checking it (default false). Only on a network nobody else is on.</summary>
    public bool? TrustServerCertificate { get; set; }
    /// <summary>The name the server's certificate is issued to, when it differs from <see cref="Host"/> (an address, an alias, a Kubernetes Service).</summary>
    public string? HostNameInCertificate { get; set; }
    /// <summary>File with the server's own certificate (PEM or DER): accepted when it is exactly that one, whoever signed it (Encrypt Mandatory or Strict).</summary>
    public string? ServerCertificate { get; set; }
    /// <summary>Seconds to wait for a connection (default 15).</summary>
    public int? ConnectTimeoutSeconds { get; set; }
    /// <summary>More SqlClient keywords, e.g. "MultiSubnetFailover=true" (not the ones above: they have settings of their own).</summary>
    public string? Options { get; set; }

    internal IEnumerable<string> Set()
    {
        if (!Blank(Host)) yield return nameof(Host);
        if (Port is not null) yield return nameof(Port);
        if (!Blank(User)) yield return nameof(User);
        if (!Blank(Password)) yield return nameof(Password);
        if (!Blank(Encrypt)) yield return nameof(Encrypt);
        if (TrustServerCertificate is not null) yield return nameof(TrustServerCertificate);
        if (!Blank(HostNameInCertificate)) yield return nameof(HostNameInCertificate);
        if (!Blank(ServerCertificate)) yield return nameof(ServerCertificate);
        if (ConnectTimeoutSeconds is not null) yield return nameof(ConnectTimeoutSeconds);
        if (!Blank(Options)) yield return nameof(Options);
    }

    internal static bool Blank(string? value) => string.IsNullOrWhiteSpace(value);
}

/// <summary>The one connection string the service uses, from <see cref="LogMinerSettings.ConnectionString"/> or <see cref="LogMinerSettings.Sql"/>.</summary>
public static class SqlTarget
{
    /// <summary>On the database host (Windows): shared memory, the process's Windows account.</summary>
    public const string LocalDefault = "Data Source=lpc:.;Initial Catalog=master;Integrated Security=true;TrustServerCertificate=true;Application Name=logminer";

    private const string Prefix = "LogMiner__Sql__";

    /// <summary>Keywords that <see cref="SqlSettings"/> sets itself, so <see cref="SqlSettings.Options"/> must not.</summary>
    private static readonly string[] Owned =
        ["Data Source", "Initial Catalog", "User ID", "Password", "Integrated Security", "Encrypt", "Trust Server Certificate", "Host Name In Certificate", "Server Certificate", "Connect Timeout"];

    /// <summary>The connection string, or an <see cref="ArgumentException"/> that says which setting is wrong (never with the password).</summary>
    public static string Resolve(LogMinerSettings settings, bool onWindows)
    {
        ArgumentNullException.ThrowIfNull(settings);
        var sql = settings.Sql ?? new SqlSettings();
        var set = sql.Set().ToList();
        if (!SqlSettings.Blank(settings.ConnectionString))
        {
            if (set.Count > 0)
                throw new ArgumentException($"LogMiner__ConnectionString and {string.Join(", ", set.Select(n => Prefix + n))} are both set: use the one or the other");
            return Parse(settings.ConnectionString!, "LogMiner__ConnectionString").ConnectionString;
        }
        if (set.Count == 0)
        {
            return onWindows ? LocalDefault
                : throw new ArgumentException($"no SQL Server is configured: set {Prefix}Host (and {Prefix}User and {Prefix}Password for a SQL login), or LogMiner__ConnectionString");
        }
        if (SqlSettings.Blank(sql.Host)) throw new ArgumentException($"{Prefix}Host is not set, but {string.Join(", ", set.Select(n => Prefix + n))} is");
        if (SqlSettings.Blank(sql.User) != SqlSettings.Blank(sql.Password))
            throw new ArgumentException($"{Prefix}User and {Prefix}Password go together (a SQL login); leave both empty for the process's own identity");
        if (sql.Port is < 1 or > 65535) throw new ArgumentException($"{Prefix}Port: {sql.Port} is not a TCP port");
        if (sql.ConnectTimeoutSeconds is < 1) throw new ArgumentException($"{Prefix}ConnectTimeoutSeconds must be positive");

        var b = Parse(sql.Options ?? "", Prefix + "Options");
        if (Owned.FirstOrDefault(b.ShouldSerialize) is { } owned)
            throw new ArgumentException($"{Prefix}Options must not set '{owned}': use the setting of its own ({Prefix}...)");
        b.DataSource = "tcp:" + sql.Host!.Trim() + (sql.Port is { } port ? "," + port.ToString(System.Globalization.CultureInfo.InvariantCulture) : "");
        b.InitialCatalog = "master";
        if (!b.ShouldSerialize("Application Name")) b.ApplicationName = "logminer";
        if (SqlSettings.Blank(sql.User)) b.IntegratedSecurity = true;
        else
        {
            b.IntegratedSecurity = false;
            // the builder refuses what no connection string can hold (a NUL); its message names the keyword, not the value
            try { (b.UserID, b.Password) = (sql.User!.Trim(), sql.Password); }
            catch (ArgumentException ex) { throw new ArgumentException($"{Prefix}User or {Prefix}Password cannot be used: {ex.Message}", ex); }
        }
        b.Encrypt = SqlSettings.Blank(sql.Encrypt) ? SqlConnectionEncryptOption.Mandatory
            : SqlConnectionEncryptOption.TryParse(sql.Encrypt!.Trim(), out var encrypt) ? encrypt!
            : throw new ArgumentException($"{Prefix}Encrypt: '{sql.Encrypt}' is not one of Mandatory, Strict, Optional");
        b.TrustServerCertificate = sql.TrustServerCertificate ?? false;
        if (!SqlSettings.Blank(sql.HostNameInCertificate)) b.HostNameInCertificate = sql.HostNameInCertificate!.Trim();
        if (!SqlSettings.Blank(sql.ServerCertificate))
        {
            var file = sql.ServerCertificate!.Trim();
            if (!File.Exists(file)) throw new ArgumentException($"{Prefix}ServerCertificate: the file {file} is not there (is the Secret mounted?)");
            b.ServerCertificate = file;
        }
        b.ConnectTimeout = sql.ConnectTimeoutSeconds ?? 15;
        return b.ConnectionString;
    }

    /// <summary>For the log: where, as whom and how it is encrypted, without the password.</summary>
    public static string Describe(string connectionString)
    {
        var b = new SqlConnectionStringBuilder(connectionString);
        var who = b.IntegratedSecurity ? "with the process's own identity"
            : b.UserID.Length > 0 ? $"as login '{b.UserID}'"
            : $"with {b.Authentication}";
        var tls = b.DataSource.StartsWith("lpc:", StringComparison.OrdinalIgnoreCase) ? "shared memory"
            : b.Encrypt == SqlConnectionEncryptOption.Optional ? "NOT encrypted unless the server insists"
            : $"encrypted ({(b.Encrypt == SqlConnectionEncryptOption.Strict ? "Strict" : "Mandatory")}), server certificate {(b.TrustServerCertificate ? "NOT checked" : b.ServerCertificate.Length > 0 ? $"pinned to {b.ServerCertificate}" : "checked")}";
        return $"{b.DataSource} {who}, {tls}";
    }

    private static SqlConnectionStringBuilder Parse(string connectionString, string setting)
    {
        try { return new SqlConnectionStringBuilder(connectionString); }
        // the message of a builder names the keyword, never the value
        catch (Exception ex) when (ex is ArgumentException or FormatException or KeyNotFoundException or InvalidOperationException)
        {
            throw new ArgumentException($"{setting} is not a valid connection string: {ex.Message}", ex);
        }
    }
}
