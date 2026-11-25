namespace LogMiner.IntegrationTests;

/// <summary>Programs the tests run, on Windows and on Linux.</summary>
internal static class Executables
{
    /// <summary>LOGMINER_NATS_SERVER, or a nats-server below ~/tools/nats-server; null: tests that need one pass trivially.</summary>
    public static string? NatsServer { get; } = Find();

    /// <summary>The service, as built with these tests.</summary>
    public static string Service { get; } = Path.Combine(AppContext.BaseDirectory, OperatingSystem.IsWindows() ? "logminer.exe" : "logminer");

    private static string? Find()
    {
        if (Environment.GetEnvironmentVariable("LOGMINER_NATS_SERVER") is { Length: > 0 } path) return path;
        var tools = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "tools", "nats-server");
        return Directory.Exists(tools) ? Directory.GetFiles(tools, OperatingSystem.IsWindows() ? "nats-server*.exe" : "nats-server", SearchOption.AllDirectories).FirstOrDefault() : null;
    }
}
