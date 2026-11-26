using LogMiner.Nats;
using LogMiner.Sinks;
using Microsoft.Extensions.Configuration;

namespace LogMiner.Service;

/// <summary>
/// Creates a sink from its configuration section ("LogMiner:Sink:{Type}"). Register additional providers in DI to add
/// destinations (Kafka, Event Hubs, HTTP...) without touching the capture pipeline.
/// </summary>
public interface ISinkProvider
{
    string Type { get; }
    ISink Create(IConfigurationSection section);
    /// <summary>What is wrong with the section (null: nothing), checked before anything starts: the service then exits with code 2.</summary>
    string? Problem(IConfigurationSection section) => null;
    /// <summary>Where the sink sends to, for the log; never with a secret.</summary>
    string? Describe(IConfigurationSection section) => null;
}

public sealed class NatsJetStreamSinkProvider : ISinkProvider
{
    public string Type => "nats-jetstream";

    public ISink Create(IConfigurationSection section) => new JetStreamSink(Options(section));

    public string? Problem(IConfigurationSection section)
    {
        try { _ = Options(section); return null; }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return ex.Message; }
    }

    public string? Describe(IConfigurationSection section)
    {
        var o = Options(section);
        return $"stream {o.StreamName} on {o.Connection.Describe()}";
    }

    private static JetStreamSinkOptions Options(IConfigurationSection section)
    {
        var o = new JetStreamSinkOptions();
        return new JetStreamSinkOptions
        {
            Connection = NatsConnectionOptions.Read(key => section[key]),
            StreamName = section["Stream"] is { Length: > 0 } stream ? stream : o.StreamName,
            StreamSubjects = section.GetSection("Subjects").Get<string[]>() ?? o.StreamSubjects,
            Replicas = section.GetValue("Replicas", o.Replicas),
            DuplicateWindow = TimeSpan.FromMinutes(section.GetValue("DuplicateWindowMinutes", o.DuplicateWindow.TotalMinutes)),
            MaxAge = TimeSpan.FromDays(section.GetValue("MaxAgeDays", o.MaxAge.TotalDays)),
            AckTimeout = TimeSpan.FromSeconds(section.GetValue("AckTimeoutSeconds", o.AckTimeout.TotalSeconds)),
            ManageStream = section.GetValue("ManageStream", o.ManageStream),
        };
    }
}

public sealed class FileSinkProvider : ISinkProvider
{
    public string Type => "file";
    public ISink Create(IConfigurationSection section) =>
        new FileSink(new FileSinkOptions { Directory = section["Directory"] ?? Path.Combine(AppContext.BaseDirectory, "events") });
}

/// <summary>Discards everything (acknowledges immediately). For measuring the capture side alone.</summary>
public sealed class NullSinkProvider : ISinkProvider
{
    public string Type => "null";
    public ISink Create(IConfigurationSection section) => new NullSink();

    private sealed class NullSink : ISink
    {
        public string Name => "null";
        public SinkHealth Health => new(Name, true);
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct) => ValueTask.FromResult(Task.CompletedTask);
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
