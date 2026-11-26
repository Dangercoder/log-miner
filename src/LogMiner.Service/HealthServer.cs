using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LogMiner.Service;

/// <summary>
/// Probes and metrics over HTTP for where nobody reads the log (a pod): <c>/livez</c> (not stuck), <c>/readyz</c> (capturing, or waiting as the standby)
/// and <c>/metrics</c> (Prometheus text). GET only, one answer per connection, nothing secret, no dependency. Off unless HealthPort is set.
/// </summary>
public sealed class HealthServer(IOptions<LogMinerSettings> settings, CaptureWorker worker, ILogger<HealthServer> log) : BackgroundService
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(5);
    private readonly SemaphoreSlim _clients = new(32, 32);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int port;
        try { port = settings.Value.HealthPort; }
        catch (InvalidOperationException) { return; } // a value that cannot be read: the worker says which, and the process ends with exit code 2
        if (port <= 0) return;
        var delay = TimeSpan.FromSeconds(1);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var listener = new TcpListener(Socket.OSSupportsIPv6 ? IPAddress.IPv6Any : IPAddress.Any, port);
                if (Socket.OSSupportsIPv6) listener.Server.DualMode = true;
                listener.Start();
                log.LogInformation("probes and metrics on port {Port}: /livez, /readyz, /metrics", port);
                while (true)
                {
                    await _clients.WaitAsync(stoppingToken);
                    TcpClient client;
                    try { client = await listener.AcceptTcpClientAsync(stoppingToken); }
                    catch { _clients.Release(); throw; }
                    _ = AnswerAsync(client, stoppingToken);
                }
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested) { return; }
            catch (SocketException ex)
            {
                log.LogError(ex, "the probe port {Port} cannot be used; trying again", port); // capturing goes on
                try { await Task.Delay(delay, stoppingToken); } catch (OperationCanceledException) { return; }
                delay = TimeSpan.FromSeconds(Math.Min(30, delay.TotalSeconds * 2));
            }
        }
    }

    private async Task AnswerAsync(TcpClient client, CancellationToken stoppingToken)
    {
        try
        {
            using (client)
            using (var patience = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken))
            {
                patience.CancelAfter(Patience);
                var stream = client.GetStream();
                var buffer = new byte[4096];
                var length = 0;
                while (length < buffer.Length)
                {
                    var read = await stream.ReadAsync(buffer.AsMemory(length), patience.Token);
                    if (read == 0) return;
                    length += read;
                    if (buffer.AsSpan(0, length).IndexOf("\r\n\r\n"u8) >= 0) break;
                }
                var (status, body) = Answer(Encoding.ASCII.GetString(buffer, 0, length).Split("\r\n", 2)[0]);
                var bytes = Encoding.UTF8.GetBytes(body);
                await stream.WriteAsync(Encoding.ASCII.GetBytes($"HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {bytes.Length}\r\nConnection: close\r\n\r\n"), patience.Token);
                await stream.WriteAsync(bytes, patience.Token);
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException) { } // whoever asked is gone
        catch (Exception ex) { log.LogWarning(ex, "a probe could not be answered"); }
        finally { _clients.Release(); }
    }

    private (string Status, string Body) Answer(string request)
    {
        var parts = request.Split(' ');
        if (parts.Length != 3 || parts[0] != "GET") return ("405 Method Not Allowed", "GET only\n");
        return parts[1].Split('?')[0] switch
        {
            "/livez" => Probe(worker.Stuck),
            "/readyz" => Probe(worker.NotReady),
            "/metrics" => ("200 OK", worker.Metrics()),
            _ => ("404 Not Found", "/livez, /readyz, /metrics\n"),
        };
    }

    private static (string, string) Probe(string? problem) => problem is null ? ("200 OK", "ok\n") : ("503 Service Unavailable", problem + "\n");
}
