namespace LogMiner.Sinks;

/// <summary>Position of a change in its database's commit order: (commit LSN, index within the transaction).</summary>
public readonly record struct SourcePosition(Lsn CommitLsn, int Sequence) : IComparable<SourcePosition>
{
    public int CompareTo(SourcePosition other)
    {
        var c = CommitLsn.CompareTo(other.CommitLsn);
        return c != 0 ? c : Sequence.CompareTo(other.Sequence);
    }

    public static bool operator <(SourcePosition a, SourcePosition b) => a.CompareTo(b) < 0;
    public static bool operator >(SourcePosition a, SourcePosition b) => a.CompareTo(b) > 0;
    public static bool operator <=(SourcePosition a, SourcePosition b) => a.CompareTo(b) <= 0;
    public static bool operator >=(SourcePosition a, SourcePosition b) => a.CompareTo(b) >= 0;
}

/// <summary>A serialized change event ready for a transport.</summary>
public sealed class SinkMessage
{
    /// <summary>Source database name.</summary>
    public string? Database { get; init; }
    /// <summary>One ordered source stream: a database <i>incarnation</i> (name + recovery fork). Positions are only comparable
    /// within a stream - a restored or re-created database starts a new one, so de-duplication never drops its events.
    /// Null for messages outside a commit order (snapshot rows).</summary>
    public string? Stream { get; init; }
    /// <summary>Position in the database's commit order; positions of one database only increase, except on replay.</summary>
    public SourcePosition? Position { get; init; }
    /// <summary>Subject / topic / path, rendered from the routing template (tokens already sanitised).</summary>
    public required string Destination { get; init; }
    /// <summary>Ordering / partition key. Messages with the same key must be delivered in the order they were sent.</summary>
    public required string Key { get; init; }
    /// <summary>Deterministic idempotency key (identical when the same change is replayed). Pass it to the destination's de-duplication when it has one.</summary>
    public required string MessageId { get; init; }
    public required ReadOnlyMemory<byte> Payload { get; init; }
    public IReadOnlyList<KeyValuePair<string, string>> Headers { get; init; } = [];
    public string? ContentType { get; init; }

    public long Size => Payload.Length + Destination.Length + Key.Length + MessageId.Length + 64;
}

public sealed record SinkHealth(
    string Sink,
    bool Healthy,
    long PendingMessages = 0,
    long PendingBytes = 0,
    TimeSpan? OldestPendingAge = null,
    string? LastError = null,
    DateTimeOffset? LastErrorAt = null,
    /// <summary>It will not recover by itself: close it and start a new one.</summary>
    bool Failed = false);

/// <summary>
/// A destination for change events. Implement this to add a transport (NATS JetStream, Kafka, Event Hubs, HTTP, files...).
/// <para><b>Ordering (no overtaking per key).</b> Callers keep at most one unacknowledged <see cref="SendAsync"/> per key
/// (a failure detected inside the sink cannot stop a later call the caller already made, so only the caller can rule out
/// overtaking - Kafka needs broker sequence numbers for the same reason). Within one call, the sink stores the messages of a
/// key in order, and once one fails it must not store any later message of that key from the same call. Different keys may
/// be processed in parallel.</para>
/// <para><b>Acknowledgement.</b> The task returned by <see cref="SendAsync"/> completes only when every message is
/// durably accepted by the destination (e.g. JetStream PubAck, Kafka acks=all, HTTP 2xx) and faults if any is
/// rejected or times out. The capture pipeline checkpoints - and lets SQL Server truncate its log - only behind
/// completed acknowledgements, so a faulted task never loses data: the database is replayed from its checkpoint.</para>
/// <para><b>Duplicates.</b> Delivery is at-least-once. Replays reuse <see cref="SinkMessage.MessageId"/>; sinks
/// should forward it to destination-side de-duplication (JetStream Nats-Msg-Id, HTTP Idempotency-Key, ...).</para>
/// <para><b>Retries.</b> A sink must not retry in a way that reorders messages. The simplest correct choice is not to
/// retry at all and fault the acknowledgement; the pipeline (or <see cref="SpoolSink"/>) resends in order.</para>
/// </summary>
public interface ISink : IAsyncDisposable
{
    string Name { get; }

    /// <summary>Connects and prepares the destination (e.g. creates the stream or topic when configured to).</summary>
    Task StartAsync(CancellationToken ct);

    /// <summary>
    /// Hands <paramref name="messages"/> to the transport in order. The ValueTask completes once they are queued so that a
    /// later call cannot overtake them (it may wait for back-pressure); the inner Task completes when all are durably
    /// accepted by the destination.
    /// </summary>
    ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct);

    SinkHealth Health { get; }

    /// <summary>The largest payload the destination takes. Larger events come with their largest values marked as unavailable.</summary>
    long MaxPayloadBytes => long.MaxValue;

    /// <summary>
    /// Does the destination hold what it acknowledged for these databases? Asked before SQL Server may discard the log behind it, which is the
    /// point of no return. Null: yes (or it cannot tell); otherwise what is wrong.
    /// </summary>
    ValueTask<string?> AuditAsync(IReadOnlyCollection<string> databases, CancellationToken ct) => ValueTask.FromResult<string?>(null);
}
