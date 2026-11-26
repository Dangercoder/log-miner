using LogMiner.Service;
using LogMiner.SqlServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// logminer --self-test [connection string]: verify a server (e.g. a test server with the next CU) and exit 0 / 4. Without a connection string, the
// one the service is configured with (appsettings.json, LogMiner__* environment variables): in a pod, `kubectl exec <pod> -- dotnet logminer.dll --self-test`.
if (args.Length > 0 && args[0] == "--self-test")
{
    string connectionString;
    if (args.Length > 1) connectionString = args[1];
    else
    {
        try { connectionString = SqlTarget.Resolve(Host.CreateApplicationBuilder([]).Configuration.GetSection("LogMiner").Get<LogMinerSettings>() ?? new(), OperatingSystem.IsWindows()); }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            Console.Error.WriteLine($"configuration: {ex.Message}");
            return 2;
        }
        Console.WriteLine($"SQL Server: {SqlTarget.Describe(connectionString)}");
    }
    var result = await SelfTest.RunAsync(connectionString, CancellationToken.None);
    Console.WriteLine($"SQL Server {result.ProductVersion}: {(result.Passed ? "verified" : "FAILED")} in {result.Elapsed.TotalSeconds:F1} s");
    foreach (var p in result.Problems) Console.WriteLine($"  {p}");
    if (result.Passed) Console.WriteLine(result.DoorbellProblem is { } bell ? $"  doorbell: not pushed ({bell}); the service would poll instead (higher latency)" : "  doorbell: log flushes are pushed");
    return result.Passed ? 0 : 4;
}

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWindowsService(o => o.ServiceName = "logminer");
builder.Services.Configure<LogMinerSettings>(builder.Configuration.GetSection("LogMiner"));
builder.Services.AddSingleton<ISinkProvider, NatsJetStreamSinkProvider>();
builder.Services.AddSingleton<ISinkProvider, FileSinkProvider>();
builder.Services.AddSingleton<ISinkProvider, NullSinkProvider>();
builder.Services.AddSingleton<CaptureWorker>();
builder.Services.AddHostedService(s => s.GetRequiredService<CaptureWorker>());
builder.Services.AddHostedService<HealthServer>();
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
await builder.Build().RunAsync();
return Environment.ExitCode;
