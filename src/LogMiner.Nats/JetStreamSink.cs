using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using LogMiner.Sinks;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace LogMiner.Nats;

public sealed class JetStreamSinkOptions
{
    public string Url { get; init; } = "nats://127.0.0.1:4222";
    public string StreamName { get; init; } = "LOGMINER";
    /// <summary>Subjects the stream captures (must cover the encoder's destination template).</summary>
    public string[] StreamSubjects { get; init; } = ["cdc.>"];
    public TimeSpan MaxAge { get; init; } = TimeSpan.FromDays(7);
    /// <summary>JetStream de-duplication window for Nats-Msg-Id. Keep it well above the longest expected restart.</summary>
    public TimeSpan DuplicateWindow { get; init; } = TimeSpan.FromMinutes(10);
    public int Replicas { get; init; } = 1;
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(10);
    /// <summary>Create the stream when it does not exist (disable when the stream is managed elsewhere). A stream that exists is never changed.</summary>
    public bool ManageStream { get; init; } = true;
    public string? CredentialsFile { get; init; }
    public string ClientName { get; init; } = $"logminer@{Environment.MachineName}";
}

/// <summary>
/// NATS JetStream <see cref="ISink"/>.
/// <para><b>Ordering.</b> One lane per <see cref="SinkMessage.Key"/> with at most one unacknowledged message: the next message of
/// a key is published only after the previous one's PubAck. A lost message therefore can never be overtaken by a later one.
/// When a message is rejected, it and every queued message of that key fail; other keys are unaffected and keep flowing.</para>
/// <para><b>Nothing is sent late.</b> The client keeps what it could not send and sends it when it is connected again, by then possibly after
/// newer messages and after the duplicate window. So a connection on which anything went unanswered (or that broke) is given up for good, with
/// everything in it, and a new one is opened; meanwhile every send fails at once.</para>
/// <para><b>Acknowledgement.</b> Only a PubAck that names the configured stream and a sequence number counts (a subscriber that happens to answer
/// does not, nor does another stream that captures the subject).</para>
/// <para><b>Idempotency.</b> Nats-Msg-Id = <see cref="SinkMessage.MessageId"/>, so resends inside the duplicate window are
/// dropped by the server.</para>
/// </summary>
public sealed class JetStreamSink(JetStreamSinkOptions options) : ISink
{
    private sealed class Item(SinkMessage message, Lane lane)
    {
        public readonly SinkMessage Message = message;
        public readonly Lane Lane = lane;
        public readonly TaskCompletionSource Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public long Deadline;
    }

    private sealed class Lane(string key)
    {
        public readonly string Key = key;
        public readonly Queue<Item> Queue = new();
        public Item? InFlight;
    }

    private readonly Lock _gate = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<string, (ulong Seq, string Id)> _acked = new(StringComparer.Ordinal);
    private Connection? _current;
    private Task? _reconnecting;
    private DateTimeOffset? _streamCreated;
    private long _duplicates, _connections;
    private string? _lastError;
    private DateTimeOffset? _lastErrorAt;

    public string Name => $"nats-jetstream({options.StreamName})";
    public long Duplicates => Interlocked.Read(ref _duplicates);
    /// <summary>Connections opened so far.</summary>
    public long Connections => Interlocked.Read(ref _connections);

    public SinkHealth Health
    {
        get
        {
            var c = _current;
            return new SinkHealth(Name, c is { Broken: false }, c?.Queued ?? 0, 0, null, _lastError, _lastErrorAt);
        }
    }

    /// <summary>The server's max_payload (headers count, too) and the stream's max message size.</summary>
    public long MaxPayloadBytes => Interlocked.Read(ref _maxPayload);
    private long _maxPayload = long.MaxValue;

    public async Task StartAsync(CancellationToken ct) => _current = await ConnectAsync(ct);

    public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_stop.IsCancellationRequested, this);
        var c = _current ?? throw new InvalidOperationException("sink not started");
        return ValueTask.FromResult(c.Send(messages));
    }

    /// <summary>Does the stream hold what it acknowledged? Asked before SQL Server may give up its log.</summary>
    public async ValueTask<string?> AuditAsync(IReadOnlyCollection<string> databases, CancellationToken ct)
    {
        var c = _current;
        if (c is null or { Broken: true }) return $"not connected to the stream {options.StreamName}";
        var js = new NatsJSContext(c.Nats);
        var stream = await js.GetStreamAsync(options.StreamName, cancellationToken: ct);
        if (_streamCreated is { } created && stream.Info.Created != created) return $"the stream {options.StreamName} was deleted and created again ({stream.Info.Created:O}); what was published before is gone";
        foreach (var db in databases)
        {
            if (!_acked.TryGetValue(db, out var last)) continue;
            if (last.Seq > stream.Info.State.LastSeq) return $"the stream {options.StreamName} ends at {stream.Info.State.LastSeq}, but acknowledged {last.Seq} ({last.Id}): it lost acknowledged messages";
            try
            {
                var got = await stream.GetAsync(new StreamMsgGetRequest { Seq = last.Seq }, ct);
                var headers = got.Message.Hdrs is null ? "" : Encoding.UTF8.GetString(Convert.FromBase64String(got.Message.Hdrs));
                if (!headers.Contains($"Nats-Msg-Id: {last.Id}\r\n", StringComparison.Ordinal)) return $"message {last.Seq} of the stream {options.StreamName} is not {last.Id}, which was acknowledged as such";
            }
            catch (NatsJSApiException ex) when (ex.Error.Code == 404)
            {
                return $"the stream {options.StreamName} does not hold message {last.Seq} ({last.Id}) any more, which it acknowledged (purged, or beyond its limits)";
            }
        }
        return null;
    }

    private void Note(Exception ex)
    {
        _lastError = $"{ex.GetType().Name}: {ex.Message}";
        _lastErrorAt = DateTimeOffset.UtcNow;
    }

    private async Task<Connection> ConnectAsync(CancellationToken ct)
    {
        var nats = new NatsConnection(NatsOpts.Default with
        {
            Url = options.Url,
            Name = options.ClientName,
            AuthOpts = options.CredentialsFile is null ? NatsAuthOpts.Default : NatsAuthOpts.Default with { CredsFile = options.CredentialsFile },
            SubPendingChannelFullMode = System.Threading.Channels.BoundedChannelFullMode.Wait,
            HeaderEncoding = Encoding.UTF8, // database and table names are any Unicode; message ids are ASCII by themselves
            IgnoreAuthErrorAbort = true,
        });
        try
        {
            await nats.ConnectAsync().AsTask().WaitAsync(options.AckTimeout, ct);
            var js = new NatsJSContext(nats);
            INatsJSStream stream;
            try { stream = await js.GetStreamAsync(options.StreamName, cancellationToken: ct); }
            catch (NatsJSApiException ex) when (ex.Error.Code == 404 && options.ManageStream)
            {
                stream = await js.CreateStreamAsync(new StreamConfig(options.StreamName, options.StreamSubjects)
                {
                    Storage = StreamConfigStorage.File,
                    Retention = StreamConfigRetention.Limits,
                    Discard = StreamConfigDiscard.Old,
                    MaxAge = options.MaxAge,
                    DuplicateWindow = options.DuplicateWindow,
                    NumReplicas = options.Replicas,
                    MaxMsgSize = 8 * 1024 * 1024,
                    Description = "logminer change events (SQL Server transaction log)",
                }, ct);
            }
            if (_streamCreated is { } created && stream.Info.Created != created) Note(new NatsJSException($"the stream {options.StreamName} was deleted and created again; what was published before is gone"));
            _streamCreated ??= stream.Info.Created;
            var limit = Math.Min(nats.ServerInfo?.MaxPayload is > 0 and var server ? server : long.MaxValue, stream.Info.Config.MaxMsgSize is > 0 and var size ? size : long.MaxValue);
            Interlocked.Exchange(ref _maxPayload, limit == long.MaxValue ? limit : Math.Max(1024, limit - 8192));
            if (stream.Info.Config.DuplicateWindow < options.DuplicateWindow)
                Note(new NatsJSException($"the stream {options.StreamName} de-duplicates over {stream.Info.Config.DuplicateWindow} only (configured: {options.DuplicateWindow}); it is not changed"));
            var inbox = $"_INBOX.logminer.{Guid.NewGuid():N}";
            var acks = await nats.SubscribeCoreAsync<byte[]>($"{inbox}.*", cancellationToken: CancellationToken.None);
            Interlocked.Increment(ref _connections);
            return new Connection(this, options, nats, inbox, acks);
        }
        catch
        {
            await CloseAsync(nats);
            throw;
        }
    }

    /// <summary>The connection is over: a new one is opened in its place.</summary>
    private void Lost(Connection c, Exception why)
    {
        Note(why);
        lock (_gate)
        {
            if (!ReferenceEquals(_current, c) || _reconnecting is { IsCompleted: false } || _stop.IsCancellationRequested) return;
            _reconnecting = Task.Run(ReconnectAsync, CancellationToken.None);
        }
    }

    private async Task ReconnectAsync()
    {
        var wait = TimeSpan.FromMilliseconds(50);
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                var c = await ConnectAsync(_stop.Token);
                lock (_gate)
                {
                    if (!_stop.IsCancellationRequested) { _current = c; return; }
                }
                await c.DisposeAsync();
                return;
            }
            catch (Exception ex) when (!_stop.IsCancellationRequested) { Note(ex); }
            catch (Exception) { return; }
            try { await Task.Delay(wait, _stop.Token); } catch (OperationCanceledException) { return; }
            wait = TimeSpan.FromMilliseconds(Math.Min(5000, wait.TotalMilliseconds * 2));
        }
    }

    private static async Task CloseAsync(NatsConnection nats)
    {
        try { await nats.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)); }
        catch (Exception) { /* it is given up either way */ }
    }

    public async ValueTask DisposeAsync()
    {
        Connection? c;
        Task? reconnecting;
        lock (_gate)
        {
            if (_stop.IsCancellationRequested) return;
            _stop.Cancel();
            (c, reconnecting) = (_current, _reconnecting);
        }
        if (reconnecting is not null) try { await reconnecting.WaitAsync(TimeSpan.FromSeconds(5)); } catch (TimeoutException) { }
        if (c is not null) await c.DisposeAsync();
    }

    /// <summary>One connection, and everything that is sent over it. It ends as a whole.</summary>
    private sealed class Connection : IAsyncDisposable
    {
        private readonly JetStreamSink _sink;
        private readonly JetStreamSinkOptions _options;
        private readonly string _inbox;
        private readonly INatsSub<byte[]> _acks;
        private readonly Lock _gate = new();
        private readonly Dictionary<string, Lane> _lanes = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<long, Item> _awaiting = new();
        private readonly CancellationTokenSource _end = new();
        private readonly Task _ackLoop, _timeoutLoop;
        private Exception? _broken;
        private long _seq, _queued;
        private Task? _closing;

        public Connection(JetStreamSink sink, JetStreamSinkOptions options, NatsConnection nats, string inbox, INatsSub<byte[]> acks)
        {
            (_sink, _options, Nats, _inbox, _acks) = (sink, options, nats, inbox, acks);
            nats.ConnectionDisconnected += (_, _) => { Break(new IOException("the connection to NATS was lost")); return default; };
            _ackLoop = Task.Run(AckLoopAsync);
            _timeoutLoop = Task.Run(TimeoutLoopAsync);
            if (nats.ConnectionState != NatsConnectionState.Open) Break(new IOException("the connection to NATS was lost"));
        }

        public NatsConnection Nats { get; }
        public bool Broken => Volatile.Read(ref _broken) is not null;
        public long Queued => Interlocked.Read(ref _queued);

        public Task Send(IReadOnlyList<SinkMessage> messages)
        {
            var acks = new Task[messages.Count];
            var start = new List<Item>();
            lock (_gate)
            {
                if (_broken is not null) throw new IOException("not connected to NATS: " + _broken.Message, _broken);
                for (var i = 0; i < messages.Count; i++)
                {
                    var m = messages[i];
                    if (!_lanes.TryGetValue(m.Key, out var lane)) _lanes[m.Key] = lane = new Lane(m.Key);
                    var item = new Item(m, lane);
                    acks[i] = item.Tcs.Task;
                    if (lane.InFlight is null) { lane.InFlight = item; start.Add(item); }
                    else lane.Queue.Enqueue(item);
                    Interlocked.Increment(ref _queued);
                }
            }
            foreach (var item in start) _ = TransmitAsync(item);
            return acks.Length == 1 ? acks[0] : Task.WhenAll(acks);
        }

        /// <summary>Never throws: whatever happens ends in the message's lane or in the connection.</summary>
        private async Task TransmitAsync(Item item)
        {
            var id = Interlocked.Increment(ref _seq);
            item.Deadline = Environment.TickCount64 + (long)_options.AckTimeout.TotalMilliseconds;
            _awaiting[id] = item; // before it is sent: it has a deadline whatever happens to the sending
            try
            {
                var m = item.Message;
                var headers = new NatsHeaders { ["Nats-Msg-Id"] = m.MessageId, ["Nats-Expected-Stream"] = _options.StreamName };
                if (m.ContentType is not null) headers["Content-Type"] = m.ContentType;
                foreach (var (k, v) in m.Headers) headers[k] = v;
                await Nats.PublishAsync(m.Destination, m.Payload.ToArray(), headers: headers, replyTo: $"{_inbox}.{id}", cancellationToken: _end.Token);
            }
            catch (Exception ex) when (ex is NatsPayloadTooLargeException or ArgumentException or FormatException or InvalidOperationException)
            {
                if (_awaiting.TryRemove(id, out _)) Fail(item.Lane, ex); // this message cannot be sent at all
            }
            catch (Exception ex) { Break(ex); } // it may or may not be on its way
        }

        private void Acknowledged(Item item)
        {
            Item? next;
            lock (_gate)
            {
                var lane = item.Lane;
                if (!ReferenceEquals(lane.InFlight, item)) return; // the lane was failed meanwhile
                next = lane.InFlight = lane.Queue.Count > 0 ? lane.Queue.Dequeue() : null;
                if (next is null) _lanes.Remove(lane.Key);
            }
            Interlocked.Decrement(ref _queued);
            item.Tcs.TrySetResult();
            if (next is not null) _ = TransmitAsync(next);
        }

        /// <summary>Fails the lane's in-flight message and every queued message of that key (nothing may overtake the failure).</summary>
        private void Fail(Lane lane, Exception ex)
        {
            _sink.Note(ex);
            List<Item> failed;
            lock (_gate) failed = Remove(lane);
            Finish(failed, ex);
        }

        private List<Item> Remove(Lane lane)
        {
            var failed = new List<Item>();
            if (lane.InFlight is not null) failed.Add(lane.InFlight);
            failed.AddRange(lane.Queue);
            lane.Queue.Clear();
            lane.InFlight = null;
            if (_lanes.TryGetValue(lane.Key, out var current) && ReferenceEquals(current, lane)) _lanes.Remove(lane.Key);
            return failed;
        }

        private void Finish(List<Item> failed, Exception ex)
        {
            foreach (var item in failed)
            {
                Interlocked.Decrement(ref _queued);
                item.Tcs.TrySetException(ex);
            }
        }

        /// <summary>
        /// Something is unanswered, or the connection broke: nothing that was handed to it may still be sent later, so it is closed with all
        /// that is in it <i>before</i> anyone learns of the failure (and sends again).
        /// </summary>
        private void Break(Exception why)
        {
            lock (_gate)
            {
                if (_broken is not null) return;
                _broken = why;
            }
            _closing = Task.Run(async () =>
            {
                await _end.CancelAsync();
                await CloseAsync(Nats);
                var failed = new List<Item>();
                lock (_gate) foreach (var lane in _lanes.Values.ToArray()) failed.AddRange(Remove(lane));
                _awaiting.Clear();
                _sink.Lost(this, why);
                Finish(failed, why);
            });
        }

        private async Task AckLoopAsync()
        {
            Exception why = new IOException("the subscription for acknowledgements ended");
            try
            {
                await foreach (var msg in _acks.Msgs.ReadAllAsync(_end.Token))
                {
                    var dot = msg.Subject.LastIndexOf('.');
                    if (!long.TryParse(msg.Subject.AsSpan(dot + 1), out var id) || !_awaiting.TryGetValue(id, out var item)) continue;
                    var (seq, duplicate, problem) = msg.HasNoResponders
                        ? (0UL, false, $"no JetStream stream captures '{item.Message.Destination}' (is stream '{_options.StreamName}' configured?)")
                        : PubAck.Parse(msg.Data, _options.StreamName);
                    if (problem is not null && !msg.HasNoResponders && !problem.StartsWith(PubAck.Rejected, StringComparison.Ordinal))
                    {
                        _sink.Note(new NatsJSException(problem)); // somebody else answers on the subject; the stream's own acknowledgement may still come
                        continue;
                    }
                    if (problem is null)
                    {
                        if (!_awaiting.TryRemove(id, out _)) continue;
                        if (duplicate) Interlocked.Increment(ref _sink._duplicates);
                        if (item.Message.Database is { } db) _sink._acked[db] = (seq, item.Message.MessageId);
                        Acknowledged(item);
                    }
                    else if (_awaiting.TryRemove(id, out _)) Fail(item.Lane, new NatsJSException(problem));
                }
            }
            catch (OperationCanceledException) when (_end.IsCancellationRequested) { return; }
            catch (Exception ex) { why = ex; }
            Break(why);
        }

        private async Task TimeoutLoopAsync()
        {
            try
            {
                using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Clamp(_options.AckTimeout.TotalMilliseconds / 4, 10, 250)));
                while (await timer.WaitForNextTickAsync(_end.Token))
                {
                    var now = Environment.TickCount64;
                    if (_awaiting.Values.Any(i => now > i.Deadline)) Break(new TimeoutException($"no JetStream acknowledgement within {_options.AckTimeout}"));
                }
            }
            catch (OperationCanceledException) { }
        }

        public async ValueTask DisposeAsync()
        {
            Break(new ObjectDisposedException(nameof(JetStreamSink)));
            if (_closing is not null) await _closing;
            try { await Task.WhenAll(_ackLoop, _timeoutLoop).WaitAsync(TimeSpan.FromSeconds(5)); } catch (TimeoutException) { }
        }
    }
}

/// <summary>JetStream's answer to a publish.</summary>
internal static class PubAck
{
    public const string Rejected = "JetStream rejected the message";

    /// <summary>The sequence number the message has in <paramref name="stream"/>, or why this is not such an acknowledgement.</summary>
    public static (ulong Seq, bool Duplicate, string? Problem) Parse(ReadOnlyMemory<byte> data, string stream)
    {
        try
        {
            using var doc = JsonDocument.Parse(data);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return (0, false, "the answer is not a JetStream acknowledgement: " + Text(data));
            if (root.TryGetProperty("error", out var error) && error.ValueKind != JsonValueKind.Null) return (0, false, $"{Rejected}: {error}");
            if (!root.TryGetProperty("stream", out var name) || name.ValueKind != JsonValueKind.String || !root.TryGetProperty("seq", out var number) || number.ValueKind != JsonValueKind.Number
                || !number.TryGetUInt64(out var seq) || seq == 0) return (0, false, "the answer is not a JetStream acknowledgement: " + Text(data));
            if (name.GetString() != stream) return (0, false, $"acknowledged by stream '{name}', not by '{stream}'");
            return (seq, root.TryGetProperty("duplicate", out var d) && d.ValueKind == JsonValueKind.True, null);
        }
        catch (JsonException) { return (0, false, "the answer is not a JetStream acknowledgement: " + Text(data)); }
        catch (InvalidOperationException) { return (0, false, "the answer is not a JetStream acknowledgement: " + Text(data)); } // invalid UTF-16 in a string
    }

    private static string Text(ReadOnlyMemory<byte> data) => Encoding.UTF8.GetString(data.Span[..Math.Min(200, data.Length)]);
}
