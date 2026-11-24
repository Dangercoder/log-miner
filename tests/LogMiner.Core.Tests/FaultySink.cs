using LogMiner.Sinks;

namespace LogMiner.Core.Tests;

public enum SinkOutcome
{
    Ack,
    /// <summary>The destination stored the batch but the acknowledgement was lost.</summary>
    FailAfterStore,
    /// <summary>The destination did not store the batch.</summary>
    FailWithoutStore,
    /// <summary>Only the first half of the batch was stored, then the connection dropped.</summary>
    FailHalfStored,
    /// <summary>SendAsync itself throws (connection down); nothing is handed over.</summary>
    Throw,
}

/// <summary>
/// A remote destination that follows the <see cref="ISink"/> contract, with asynchronous acknowledgements (a background
/// "network" thread processes handed-over messages with small delays, so several batches are in flight at once) and scripted
/// failures. Per key it never stores a message ahead of an earlier one: when a message is not stored, every already handed-over
/// message with the same key fails too. <see cref="Stored"/> is what the destination durably holds, duplicates included.
/// </summary>
public sealed class FaultySink : ISink
{
    private sealed class Batch(SinkOutcome outcome, int count)
    {
        public readonly SinkOutcome Outcome = outcome;
        public readonly int Count = count;
        public readonly TaskCompletionSource Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Done;
        public bool Failed;
    }

    private sealed record Item(SinkMessage Message, Batch Batch, int Index);

    private readonly object _gate = new(); // Monitor.Wait/Pulse
    private readonly Queue<SinkOutcome> _script = new();
    private readonly LinkedList<Item> _pending = new();
    private readonly List<SinkMessage> _stored = [];
    private readonly Thread _network;
    private volatile bool _stop;

    public FaultySink()
    {
        _network = new Thread(NetworkLoop) { IsBackground = true, Name = "faulty-sink-network" };
        _network.Start();
    }

    public string Name => "faulty";
    public SinkHealth Health => new(Name, true);
    public int Sends { get; private set; }

    public IReadOnlyList<SinkMessage> Stored { get { lock (_gate) return [.. _stored]; } }
    public int Pending { get { lock (_gate) return _pending.Count; } }

    public void Script(IEnumerable<SinkOutcome> outcomes) { lock (_gate) foreach (var o in outcomes) _script.Enqueue(o); }
    public void Heal() { lock (_gate) _script.Clear(); }

    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct)
    {
        lock (_gate)
        {
            Sends++;
            var outcome = _script.Count > 0 ? _script.Dequeue() : SinkOutcome.Ack;
            if (outcome == SinkOutcome.Throw) return ValueTask.FromException<Task>(new IOException("connection refused (scripted)"));
            var batch = new Batch(outcome, messages.Count);
            if (messages.Count == 0) batch.Tcs.SetResult();
            for (var i = 0; i < messages.Count; i++) _pending.AddLast(new Item(messages[i], batch, i));
            Monitor.PulseAll(_gate);
            return ValueTask.FromResult(batch.Tcs.Task);
        }
    }

    private void NetworkLoop()
    {
        var rng = new Random(12345);
        // Outcomes reach the producer one network step late (like a real remote): the producer may hand over more in the
        // window between "the destination failed a message" and "the producer learns about it".
        var deferred = new List<(Batch Batch, bool Failed)>();
        while (!_stop)
        {
            lock (_gate)
            {
                while (_pending.Count == 0 && deferred.Count == 0 && !_stop) Monitor.Wait(_gate, 50);
            }
            if (_stop) return;
            if (rng.Next(3) == 0) Thread.Sleep(1); // let the producer hand over more: overlapping in-flight batches
            var completions = new List<(Batch Batch, bool Failed)>();
            lock (_gate)
            {
                var budget = rng.Next(1, 4);
                while (budget-- > 0 && _pending.First is { } node)
                {
                    _pending.RemoveFirst();
                    var (m, b, index) = node.Value;
                    var stored = b.Outcome switch
                    {
                        SinkOutcome.Ack or SinkOutcome.FailAfterStore => true,
                        SinkOutcome.FailHalfStored => index < b.Count / 2,
                        _ => false,
                    };
                    if (stored) _stored.Add(m);
                    if (!stored || b.Outcome == SinkOutcome.FailAfterStore) b.Failed = true;
                    Finish(b, completions);
                    if (!stored)
                    {
                        // contract: nothing with the same key may overtake the failed message
                        for (var n = _pending.First; n is not null;)
                        {
                            var next = n.Next;
                            if (n.Value.Message.Key == m.Key)
                            {
                                _pending.Remove(n);
                                n.Value.Batch.Failed = true;
                                Finish(n.Value.Batch, completions);
                            }
                            n = next;
                        }
                    }
                }
            }
            foreach (var (b, failed) in deferred)
                if (failed) b.Tcs.TrySetException(new IOException($"scripted {b.Outcome}"));
                else b.Tcs.TrySetResult();
            deferred = completions;
        }
    }

    private static void Finish(Batch b, List<(Batch, bool)> completions)
    {
        if (++b.Done == b.Count) completions.Add((b, b.Failed));
    }

    /// <summary>The remote outlives its clients (a restarted spool disposes its inner sink); stop it explicitly.</summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Shutdown()
    {
        _stop = true;
        lock (_gate) Monitor.PulseAll(_gate);
    }
}
