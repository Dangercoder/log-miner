using LogMiner.Service;
using LogMiner.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// logminer.exe --self-test [connection string]: verify a server (e.g. a test server with the next CU) and exit 0 / 4
if (args.Length > 0 && args[0] == "--self-test")
{
    var result = await SelfTest.RunAsync(args.Length > 1 ? args[1] : new LogMinerSettings().ConnectionString, CancellationToken.None);
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
