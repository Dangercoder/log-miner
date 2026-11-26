namespace LogMiner.Sinks;

/// <summary>Keeps messages in memory and acknowledges immediately (tests, dry runs).</summary>
public sealed class MemorySink : ISink
{
    private readonly Lock _gate = new();
    private readonly List<SinkMessage> _messages = [];

    public string Name => "memory";
    public SinkHealth Health => new(Name, true);
    public int Count { get { lock (_gate) return _messages.Count; } }

    public IReadOnlyList<SinkMessage> Snapshot() { lock (_gate) return [.. _messages]; }
    public void Clear() { lock (_gate) _messages.Clear(); }

    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct)
    {
        lock (_gate) _messages.AddRange(messages);
        return ValueTask.FromResult(Task.CompletedTask);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
