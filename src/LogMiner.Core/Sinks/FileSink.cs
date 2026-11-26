using System.Text;
using System.Text.Json;

namespace LogMiner.Sinks;

public sealed class FileSinkOptions
{
    public required string Directory { get; init; }
    /// <summary>Roll to a new file after this many bytes.</summary>
    public long MaxFileBytes { get; init; } = 256L * 1024 * 1024;
}

/// <summary>
/// Appends messages as JSON lines (<c>{"id":..,"destination":..,"key":..,"headers":{..},"payload":&lt;event&gt;}</c>) to rolling files and
/// acknowledges after an fsync. Useful for debugging, audits and air-gapped transfer; also the simplest example of an
/// <see cref="ISink"/>.
/// </summary>
public sealed class FileSink(FileSinkOptions options) : ISink
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private FileStream? _file;
    private long _written;

    public string Name => $"file({options.Directory})";
    public SinkHealth Health => new(Name, true);

    public Task StartAsync(CancellationToken ct)
    {
        System.IO.Directory.CreateDirectory(options.Directory);
        return Task.CompletedTask;
    }

    public async ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct)
    {
        var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            foreach (var m in messages)
            {
                w.Reset(buffer);
                w.WriteStartObject();
                w.WriteString("id", m.MessageId);
                w.WriteString("destination", m.Destination);
                w.WriteString("key", m.Key);
                w.WriteStartObject("headers");
                foreach (var (k, v) in m.Headers) w.WriteString(k, v);
                w.WriteEndObject();
                w.WritePropertyName("payload");
                if (m.ContentType == "application/json") w.WriteRawValue(m.Payload.Span, skipInputValidation: true);
                else w.WriteBase64StringValue(m.Payload.Span);
                w.WriteEndObject();
                w.Flush();
                buffer.WriteByte((byte)'\n');
            }
        }
        await _gate.WaitAsync(ct);
        try
        {
            if (_file is null || _written > options.MaxFileBytes)
            {
                if (_file is not null) await _file.DisposeAsync();
                _file = null;
                for (var n = 0; _file is null; n++)
                {
                    var name = $"logminer-{DateTime.UtcNow:yyyyMMdd-HHmmss-fff}-{Environment.ProcessId}{(n == 0 ? "" : "-" + n.ToString(System.Globalization.CultureInfo.InvariantCulture))}.jsonl";
                    try { _file = new FileStream(Path.Combine(options.Directory, name), FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1); }
                    catch (IOException) when (n < 1000 && File.Exists(Path.Combine(options.Directory, name))) { }
                }
                _written = 0;
            }
            var done = false;
            try
            {
                // not cancelled half-way; a call of any size is written in pieces
                for (long at = 0; at < buffer.Length; at += 1 << 24)
                    await _file.WriteAsync(buffer.GetBuffer().AsMemory((int)at, (int)Math.Min(1 << 24, buffer.Length - at)), CancellationToken.None);
                _file.Flush(true);
                done = true;
            }
            finally
            {
                if (!done) await AbandonAsync();
            }
            _written += buffer.Length;
        }
        finally { _gate.Release(); }
        return Task.CompletedTask;
    }

    /// <summary>What was written of a failed call is taken back, so that the messages that are sent again do not continue half a line. If even
    /// that fails, nothing more is appended to the file.</summary>
    private async Task AbandonAsync()
    {
        var file = _file!;
        try
        {
            file.SetLength(_written);
            file.Seek(0, SeekOrigin.End);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _file = null;
            try { await file.DisposeAsync(); } catch (IOException) { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_file is not null) await _file.DisposeAsync();
        _gate.Dispose();
    }
}
