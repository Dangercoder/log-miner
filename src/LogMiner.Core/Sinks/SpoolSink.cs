using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Threading.Channels;

namespace LogMiner.Sinks;

public enum SpoolAck
{
    /// <summary>Acknowledge when the message is fsync'd to the local spool. The source log can be released immediately;
    /// a remote outage fills the spool instead of SQL Server's transaction logs.</summary>
    Spooled,
    /// <summary>Acknowledge when the inner sink accepted the message. The source log is retained until remote delivery;
    /// the spool still decouples and replays.</summary>
    Delivered,
}

public sealed class SpoolOptions
{
    public required string Directory { get; init; }
    public SpoolAck AckOn { get; init; } = SpoolAck.Spooled;
    public long SegmentBytes { get; init; } = 64L * 1024 * 1024;
    /// <summary>Spool capacity. When full, <see cref="SpoolSink.SendAsync"/> waits (back-pressure to the source).</summary>
    public long MaxBytes { get; init; } = 20L * 1024 * 1024 * 1024;
    public int ForwardBatchMessages { get; init; } = 512;
    public int ForwardMaxInFlightMessages { get; init; } = 8192;
    public int LiveQueueMessages { get; init; } = 65536;
    public TimeSpan CursorFlushInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan RetryMin { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan RetryMax { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Drop a message whose id is already in the spool (replays after a restart). Exact ids only: a message that was
    /// never durably spooled can never be mistaken for a duplicate.</summary>
    public bool Deduplicate { get; init; } = true;
    /// <summary>How many recent message ids are remembered for de-duplication. Older duplicates pass through (the destination's
    /// own de-duplication, e.g. Nats-Msg-Id, catches them).</summary>
    public int DedupeWindowMessages { get; init; } = 1_000_000;
}

public sealed class SpoolCorruptedException(string message) : Exception(message);

/// <summary>
/// Durable local store-and-forward in front of any <see cref="ISink"/>.
/// <list type="bullet">
/// <item><b>Durability.</b> Records are appended to checksummed segment files and fsync'd (group commit) before the
/// <see cref="SpoolAck.Spooled"/> acknowledgement completes. A batch's acknowledgement waits for every earlier record too,
/// including the original copy of anything that was de-duplicated (by exact message id).</item>
/// <item><b>Forwarding.</b> Records are forwarded in spool order as soon as they are written (from memory when possible,
/// from disk after an outage or restart). On an inner failure the forwarder goes back to the first unacknowledged record and
/// resends in order (go-back-N); duplicates carry the same message id.</item>
/// <item><b>Retention.</b> A segment is deleted only when all of its records are delivered <i>and</i> the delivery cursor is
/// persisted. The cursor is written atomically (temp file, fsync, rename).</item>
/// <item><b>Recovery.</b> On start every segment is validated record by record (magic, length, CRC-32C, contiguous
/// sequence numbers). An invalid tail of the last segment is a torn write and is truncated, unless a valid record of a later group commit
/// follows it: then what is damaged had been on disk, and that - like a missing segment or any other damage - fails closed.</item>
/// <item><b>Delivered mode</b> acknowledges nothing that is only in the spool: what an earlier run left is dropped on start and sent again by the
/// source. (Sent from here it could come long after what another instance has published meanwhile.)</item>
/// </list>
/// </summary>
public sealed class SpoolSink : ISink
{
    private sealed class Segment
    {
        public required string Path;
        public long FirstSeq;
        public long LastSeq;
        public long Length;
        /// <summary>Sparse index seq -> byte offset (every <see cref="IndexEvery"/> records) for seeking on replay.</summary>
        public readonly List<(long Seq, long Offset)> Index = [];
        public long Count => LastSeq >= FirstSeq && FirstSeq > 0 ? LastSeq - FirstSeq + 1 : 0;
    }

    private const int IndexEvery = 256;
    private const uint CursorMagic = 0x31534D4C, CursorMagicV2 = SpoolCodec.Magic;
    private const string CursorFile = "cursor.lmc";
    /// <summary>Present while the directory may hold records that were acknowledged because they are in it (Spooled mode).</summary>
    private const string SpooledMarker = "spooled.lmc";

    private readonly ISink _inner;
    private readonly SpoolOptions _o;
    private readonly Lock _appendLock = new();
    private readonly Lock _waitGate = new();
    private readonly Lock _segLock = new();
    private readonly List<(long Seq, TaskCompletionSource Tcs)> _waiters = [];
    private readonly HashSet<string> _recentIds = new(StringComparer.Ordinal);
    private readonly Queue<string> _recentOrder = new();
    private readonly List<Segment> _segments = [];
    private readonly SemaphoreSlim _dataSignal = new(0, int.MaxValue);
    private readonly SemaphoreSlim _spaceSignal = new(0, int.MaxValue);
    private Channel<SpoolRecord> _writeQueue = Channel.CreateUnbounded<SpoolRecord>(new() { SingleReader = true });
    private Channel<SpoolRecord> _live;
    private FileStream? _active;
    private long _nextSeq = 1;
    private long _queuedBytes, _diskBytes;
    private long _writtenSeq, _durableSeq, _deliveredSeq, _persistedCursor;
    private long _oldestPendingAppendedMs;
    private volatile Exception? _fatal;
    private string? _lastError;
    private DateTimeOffset? _lastErrorAt;
    private CancellationTokenSource _cts = new();
    private Task? _writer, _forwarder, _persister;
    private FileStream? _lock;

    public SpoolSink(ISink inner, SpoolOptions options)
    {
        _inner = inner;
        _o = options;
        _live = Channel.CreateBounded<SpoolRecord>(new BoundedChannelOptions(options.LiveQueueMessages) { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true });
    }

    public string Name => $"spool({_inner.Name})";
    public long MaxPayloadBytes => Math.Min(_inner.MaxPayloadBytes, SpoolCodec.MaxBodySize / 2);
    public ValueTask<string?> AuditAsync(IReadOnlyCollection<string> databases, CancellationToken ct) => _inner.AuditAsync(databases, ct);
    public long DurableSeq => Interlocked.Read(ref _durableSeq);
    public long DeliveredSeq => Interlocked.Read(ref _deliveredSeq);
    public long LastSeq { get { lock (_appendLock) return _nextSeq - 1; } }
    public long DiskBytes => Interlocked.Read(ref _diskBytes);

    public SinkHealth Health
    {
        get
        {
            var pending = LastSeq - DeliveredSeq;
            var oldest = Interlocked.Read(ref _oldestPendingAppendedMs);
            TimeSpan? age = pending > 0 && oldest > 0 ? DateTimeOffset.UtcNow - DateTimeOffset.FromUnixTimeMilliseconds(oldest) : null;
            var inner = _inner.Health;
            return new SinkHealth(Name, _fatal is null && inner.Healthy, pending, DiskBytes, age, _fatal?.Message ?? _lastError ?? inner.LastError, _lastErrorAt ?? inner.LastErrorAt, _fatal is not null || inner.Failed);
        }
    }

    // ---------------------------------------------------------------- start / recovery

    public async Task StartAsync(CancellationToken ct)
    {
        Directory.CreateDirectory(_o.Directory);
        _lock = new FileStream(System.IO.Path.Combine(_o.Directory, "lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose); // one process per directory
        var marker = System.IO.Path.Combine(_o.Directory, SpooledMarker);
        if (_o.AckOn == SpoolAck.Spooled) { if (!File.Exists(marker)) File.WriteAllText(marker, ""); }
        else if (!File.Exists(marker)) DiscardUnacknowledged();
        Recover();
        if (_o.AckOn == SpoolAck.Delivered && _deliveredSeq >= _nextSeq - 1) File.Delete(marker);
        await _inner.StartAsync(ct);
        _cts = new CancellationTokenSource();
        _writer = Task.Run(() => WriterLoopAsync(_cts.Token), CancellationToken.None);
        _forwarder = Task.Run(() => ForwarderLoopAsync(_cts.Token), CancellationToken.None);
        _persister = Task.Run(() => PersisterLoopAsync(_cts.Token), CancellationToken.None);
    }

    /// <summary>The directory holds records that were acknowledged because they are in it and are not known to be delivered.</summary>
    public static bool HasUndelivered(string directory)
    {
        if (!File.Exists(System.IO.Path.Combine(directory, SpooledMarker))) return false;
        var cursor = ReadCursor(directory) ?? 0;
        foreach (var file in Directory.GetFiles(directory, "*.seg"))
        {
            var data = File.ReadAllBytes(file).AsSpan();
            while (SpoolCodec.TryRead(data, out var r, out var consumed))
            {
                if (r!.Seq > cursor) return true;
                data = data[consumed..];
            }
        }
        return false;
    }

    /// <summary>Nothing in the directory was acknowledged: the source still has it, and sends it again. Sequence numbers go on.</summary>
    private void DiscardUnacknowledged()
    {
        var last = ReadCursor(_o.Directory) ?? 0;
        var files = Directory.GetFiles(_o.Directory, "*.seg");
        foreach (var file in files)
        {
            if (long.TryParse(System.IO.Path.GetFileNameWithoutExtension(file), NumberStyles.None, CultureInfo.InvariantCulture, out var first)) last = Math.Max(last, first);
            var data = File.ReadAllBytes(file).AsSpan();
            while (SpoolCodec.TryRead(data, out var r, out var consumed)) { last = Math.Max(last, r!.Seq); data = data[consumed..]; }
        }
        if (files.Length == 0) return;
        WriteAtomic(CursorFile, w => BinaryPrimitives.WriteInt64LittleEndian(w(8), last)); // first: what is deleted next counts as dealt with
        foreach (var file in files) File.Delete(file);
    }

    private void Recover()
    {
        var cursor = ReadCursor(_o.Directory);
        // from the very first record on: without it a missing first segment could not be told from one that was delivered
        if (cursor is null && !Directory.EnumerateFiles(_o.Directory, "*.seg").Any()) WriteAtomic(CursorFile, w => BinaryPrimitives.WriteInt64LittleEndian(w(8), 0));
        _persistedCursor = cursor ?? 0; // damaged: send everything again (duplicates, never loss)
        var files = Directory.GetFiles(_o.Directory, "*.seg")
            .Select(f => (Path: f, First: long.TryParse(System.IO.Path.GetFileNameWithoutExtension(f), NumberStyles.None, CultureInfo.InvariantCulture, out var s) ? s : -1))
            .OrderBy(f => f.First).ToList();
        long expected = -1;
        for (var i = 0; i < files.Count; i++)
        {
            var (path, first) = files[i];
            var isLast = i == files.Count - 1;
            if (first <= 0) throw new SpoolCorruptedException($"unexpected spool file name {path}");
            var data = File.ReadAllBytes(path);
            var seg = new Segment { Path = path };
            long offset = 0;
            while (offset < data.Length && SpoolCodec.TryRead(data.AsSpan((int)offset), out var read, out var consumed))
            {
                var rec = read!;
                if ((expected != -1 && rec.Seq != expected) || (seg.FirstSeq == 0 && rec.Seq != first))
                    throw new SpoolCorruptedException($"{path}: sequence {rec.Seq} where {(expected == -1 ? first : expected)} was expected (a segment is missing, renamed or damaged)");
                if (seg.FirstSeq == 0) seg.FirstSeq = rec.Seq;
                if ((rec.Seq - seg.FirstSeq) % IndexEvery == 0) seg.Index.Add((rec.Seq, offset));
                seg.LastSeq = rec.Seq;
                Remember(rec.Message.MessageId);
                expected = rec.Seq + 1;
                offset += consumed;
            }
            if (offset < data.Length)
            {
                if (!isLast) throw new SpoolCorruptedException($"{path}: invalid record at offset {offset} in a sealed segment");
                var damaged = expected == -1 ? first : expected;
                if (LaterBatch(data.AsSpan((int)offset), damaged) is { } later)
                    throw new SpoolCorruptedException($"{path}: record {damaged} at offset {offset} is damaged, but record {later}, written after it was on disk, is not: acknowledged records are lost");
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
                fs.SetLength(offset); // torn tail of the active segment: data that was never acknowledged
                fs.Flush(true);
            }
            seg.Length = offset;
            if (seg.Count == 0) { File.Delete(path); continue; }
            _segments.Add(seg);
            _diskBytes += seg.Length;
        }
        var lastSeq = _segments.Count > 0 ? _segments[^1].LastSeq : 0;
        var firstAvailable = _segments.Count > 0 ? _segments[0].FirstSeq : lastSeq + 1;
        // a segment is only deleted behind the persisted cursor
        if (cursor is { } delivered && _segments.Count > 0 && delivered < firstAvailable - 1)
            throw new SpoolCorruptedException($"{_o.Directory}: records {delivered + 1} to {firstAvailable - 1} were never delivered and are missing");
        // A cursor beyond the last surviving record is legitimate: records are forwarded before their fsync, so a crash can
        // tear away records the remote already acknowledged. They were never acknowledged to the producer, which replays them.
        _nextSeq = Math.Max(lastSeq, _persistedCursor) + 1;
        _writtenSeq = _durableSeq = _nextSeq - 1;
        _deliveredSeq = Math.Max(_persistedCursor, firstAvailable - 1);
        DeleteDeliveredSegments(keepActive: false);
        OpenActive();
    }

    /// <summary>A valid record of a group commit that began after <paramref name="damaged"/>.</summary>
    private static long? LaterBatch(ReadOnlySpan<byte> data, long damaged)
    {
        Span<byte> magic = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(magic, SpoolCodec.Magic);
        for (var at = 1; at < data.Length;)
        {
            var next = data[at..].IndexOf(magic);
            if (next < 0) return null;
            at += next;
            if (SpoolCodec.TryRead(data[at..], out var r, out _) && r!.Batch > damaged) return r.Seq;
            at++;
        }
        return null;
    }

    private void OpenActive()
    {
        lock (_segLock)
        {
            if (_segments.Count > 0 && _segments[^1].Length < _o.SegmentBytes)
            {
                var seg = _segments[^1];
                _active = new FileStream(seg.Path, FileMode.Open, FileAccess.Write, FileShare.Read, 1, FileOptions.None);
                _active.Seek(seg.Length, SeekOrigin.Begin);
            }
            else NewSegment(_nextSeq);
        }
    }

    /// <summary>Starts a segment whose first record will be <paramref name="firstSeq"/> (the file name). Caller holds _segLock.</summary>
    private void NewSegment(long firstSeq)
    {
        _active?.Dispose();
        var path = System.IO.Path.Combine(_o.Directory, firstSeq.ToString("D20", CultureInfo.InvariantCulture) + ".seg");
        _active = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, 1, FileOptions.None);
        _segments.Add(new Segment { Path = path });
    }

    // ---------------------------------------------------------------- append

    public async ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct)
    {
        if (_fatal is { } f) throw new InvalidOperationException("spool failed", f);
        long bytes = 0;
        foreach (var m in messages)
        {
            if (SpoolCodec.Problem(m) is { } why) throw new InvalidOperationException(why);
            bytes += m.Size;
        }
        while (true)
        {
            var used = Interlocked.Read(ref _diskBytes) + Interlocked.Read(ref _queuedBytes);
            // when everything is delivered only the active segment is left, which is not deleted: waiting would not make room
            if (used + bytes <= _o.MaxBytes || (Interlocked.Read(ref _queuedBytes) == 0 && DeliveredSeq >= LastSeq)) break;
            await _spaceSignal.WaitAsync(TimeSpan.FromMilliseconds(250), ct);
            if (_fatal is { } f2) throw new InvalidOperationException("spool failed", f2);
        }
        long waitSeq;
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        lock (_appendLock)
        {
            foreach (var m in messages)
            {
                if (_o.Deduplicate && _recentIds.Contains(m.MessageId))
                    continue; // already in the spool (replay after a restart); the acknowledgement below still waits for it
                var rec = new SpoolRecord(_nextSeq++, now, m);
                Remember(m.MessageId);
                Interlocked.Add(ref _queuedBytes, m.Size);
                _writeQueue.Writer.TryWrite(rec);
                _live.Writer.TryWrite(rec); // drops when the forwarder is far behind; it then reads from disk
            }
            waitSeq = _nextSeq - 1;
        }
        return Wait(waitSeq);
    }

    /// <summary>Adds an id to the bounded de-duplication window. Caller holds _appendLock (or runs during recovery).</summary>
    private void Remember(string id)
    {
        if (!_o.Deduplicate || !_recentIds.Add(id)) return;
        _recentOrder.Enqueue(id);
        while (_recentOrder.Count > _o.DedupeWindowMessages) _recentIds.Remove(_recentOrder.Dequeue());
    }

    private Task Wait(long seq)
    {
        lock (_waitGate)
        {
            var reached = _o.AckOn == SpoolAck.Spooled ? _durableSeq : _deliveredSeq;
            if (seq <= reached) return Task.CompletedTask;
            if (_fatal is { } f) return Task.FromException(new InvalidOperationException("spool failed", f));
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var i = _waiters.Count;
            while (i > 0 && _waiters[i - 1].Seq > seq) i--;
            _waiters.Insert(i, (seq, tcs));
            return tcs.Task;
        }
    }

    private void Release(long reached)
    {
        lock (_waitGate)
        {
            var n = 0;
            while (n < _waiters.Count && _waiters[n].Seq <= reached) { _waiters[n].Tcs.TrySetResult(); n++; }
            _waiters.RemoveRange(0, n);
        }
    }

    // ---------------------------------------------------------------- writer (group commit)

    private async Task WriterLoopAsync(CancellationToken ct)
    {
        var buffer = new ArrayBufferWriter<byte>(1 << 20);
        var batch = new List<SpoolRecord>();
        try
        {
            while (await _writeQueue.Reader.WaitToReadAsync(ct))
            {
                batch.Clear();
                while (batch.Count < 4096 && _writeQueue.Reader.TryRead(out var rec)) batch.Add(rec);
                WriteBatch(batch, buffer);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { Fail(ex); }
    }

    private void WriteBatch(List<SpoolRecord> batch, ArrayBufferWriter<byte> buffer)
    {
        long releasedBytes = 0, written = 0;
        lock (_segLock)
        {
            var seg = _segments[^1];
            if (StallBeforeFsync) _stallLengthBefore = seg.Length;
            buffer.ResetWrittenCount();
            foreach (var rec in batch)
            {
                var before = buffer.WrittenCount;
                SpoolCodec.Write(rec with { Batch = batch[0].Seq }, buffer);
                if (seg.Length + before > 0 && seg.Length + buffer.WrittenCount > _o.SegmentBytes)
                {
                    // seal the current segment with what precedes this record, then start a new one with it
                    var tail = buffer.WrittenSpan[before..].ToArray();
                    _active!.Write(buffer.WrittenSpan[..before]);
                    seg.Length += before;
                    written += before;
                    _active.Flush(true);
                    NewSegment(rec.Seq);
                    seg = _segments[^1];
                    buffer.ResetWrittenCount();
                    buffer.Write(tail);
                    before = 0;
                }
                if (seg.FirstSeq == 0) seg.FirstSeq = rec.Seq;
                if ((rec.Seq - seg.FirstSeq) % IndexEvery == 0) seg.Index.Add((rec.Seq, seg.Length + before));
                seg.LastSeq = rec.Seq;
                releasedBytes += rec.Message.Size;
            }
            _active!.Write(buffer.WrittenSpan);
            seg.Length += buffer.WrittenCount;
            written += buffer.WrittenCount;
        }
        Interlocked.Add(ref _diskBytes, written);
        var last = batch[^1].Seq;
        Interlocked.Exchange(ref _writtenSeq, last); // readable (OS cache): the forwarder may send before the fsync
        _dataSignal.Release();
        if (StallBeforeFsync) { _stalled.TrySetResult(); throw new OperationCanceledException("test: crash before fsync"); }
        _active.Flush(true); // fsync: the acknowledgement point
        Interlocked.Add(ref _queuedBytes, -releasedBytes);
        Interlocked.Exchange(ref _durableSeq, last);
        if (_o.AckOn == SpoolAck.Spooled) Release(last);
    }

    // ---------------------------------------------------------------- forwarder

    private async Task ForwarderLoopAsync(CancellationToken ct)
    {
        var inflight = new Queue<(long First, long Last, long AppendedMs, Task Ack)>();
        var inflightCount = 0L;
        var next = Interlocked.Read(ref _deliveredSeq) + 1;
        SpoolRecord? peek = null;
        var backoff = _o.RetryMin;
        using var reader = new DiskReader(this);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // retire acknowledgements in order
                while (inflight.Count > 0 && inflight.Peek().Ack.IsCompleted)
                {
                    var head = inflight.Dequeue();
                    inflightCount -= head.Last - head.First + 1;
                    if (!head.Ack.IsCompletedSuccessfully)
                    {
                        Note(head.Ack.Exception?.GetBaseException() ?? new OperationCanceledException("inner sink acknowledgement canceled"));
                        inflight.Clear(); inflightCount = 0; peek = null; reader.Reset();
                        next = Interlocked.Read(ref _deliveredSeq) + 1; // go back: resend everything not acknowledged, in order
                        await Task.Delay(backoff, ct);
                        backoff = TimeSpan.FromTicks(Math.Min(_o.RetryMax.Ticks, backoff.Ticks * 2));
                        break;
                    }
                    Interlocked.Exchange(ref _deliveredSeq, head.Last);
                    Interlocked.Exchange(ref _oldestPendingAppendedMs, inflight.Count > 0 ? inflight.Peek().AppendedMs : 0);
                    if (_o.AckOn == SpoolAck.Delivered) Release(head.Last);
                    backoff = _o.RetryMin;
                }

                // send more
                var written = Interlocked.Read(ref _writtenSeq);
                // One unacknowledged batch at a time: a failure detected later can then never be overtaken by a batch
                // handed over meanwhile (the inner sink cannot know the caller is about to go back).
                if (inflight.Count == 0 && next <= written)
                {
                    var batch = new List<SpoolRecord>(_o.ForwardBatchMessages);
                    var limit = Math.Min(written, next + _o.ForwardBatchMessages - 1);
                    while (batch.Count == 0 || batch[^1].Seq < limit)
                    {
                        if (peek is null && !_live.Reader.TryRead(out peek)) break;
                        if (peek.Seq < next + batch.Count) { peek = null; continue; } // stale (already sent)
                        if (peek.Seq != next + batch.Count || peek.Seq > limit) break; // gap or not yet written
                        batch.Add(peek); peek = null;
                    }
                    if (batch.Count == 0) batch = reader.Read(next, limit);
                    if (batch.Count > 0)
                    {
                        Task ack;
                        try { ack = await _inner.SendAsync(batch.Select(r => r.Message).ToArray(), ct).AsTask().WaitAsync(ct); }
                        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
                        {
                            // Nothing of this batch was handed over. Handing over anything later now would let it overtake
                            // this batch at the destination: go back immediately and resend from the first unacknowledged record.
                            Note(ex);
                            inflight.Clear(); inflightCount = 0; peek = null; reader.Reset();
                            next = Interlocked.Read(ref _deliveredSeq) + 1;
                            await Task.Delay(backoff, ct);
                            backoff = TimeSpan.FromTicks(Math.Min(_o.RetryMax.Ticks, backoff.Ticks * 2));
                            continue;
                        }
                        if (inflight.Count == 0) Interlocked.Exchange(ref _oldestPendingAppendedMs, batch[0].AppendedUnixMs);
                        inflight.Enqueue((batch[0].Seq, batch[^1].Seq, batch[0].AppendedUnixMs, ack));
                        inflightCount += batch.Count;
                        next = batch[^1].Seq + 1;
                        continue;
                    }
                }

                // wait for an acknowledgement, new data, or a timeout
                var dataWait = _dataSignal.WaitAsync(TimeSpan.FromMilliseconds(200), ct);
                if (inflight.Count > 0) await Task.WhenAny(inflight.Peek().Ack, dataWait);
                else await dataWait;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { Fail(ex); }
    }

    /// <summary>Sequential reader over spooled records (replay after outages/restarts).</summary>
    private sealed class DiskReader(SpoolSink spool) : IDisposable
    {
        private FileStream? _fs;
        private string? _path;
        private long _nextSeqAtPosition = -1;
        private byte[] _buf = new byte[1 << 20];

        public void Reset() { _fs?.Dispose(); _fs = null; _path = null; _nextSeqAtPosition = -1; }
        public void Dispose() => Reset();

        public List<SpoolRecord> Read(long from, long to)
        {
            var result = new List<SpoolRecord>();
            Segment? seg;
            (long Seq, long Offset) start;
            lock (spool._segLock)
            {
                seg = spool._segments.FirstOrDefault(s => s.FirstSeq <= from && from <= s.LastSeq);
                if (seg is null) return result;
                var idx = seg.Index.LastOrDefault(i => i.Seq <= from);
                start = idx.Seq == 0 ? (seg.FirstSeq, 0L) : idx;
            }
            if (_path != seg.Path || _nextSeqAtPosition != from)
            {
                Reset();
                _fs = new FileStream(seg.Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 1, FileOptions.SequentialScan);
                _fs.Seek(start.Offset, SeekOrigin.Begin);
                _path = seg.Path;
                _nextSeqAtPosition = start.Seq;
            }
            while (_nextSeqAtPosition <= Math.Min(to, seg.LastSeq))
            {
                var rec = ReadOne() ?? throw new SpoolCorruptedException($"{seg.Path}: cannot read record {_nextSeqAtPosition}");
                if (rec.Seq != _nextSeqAtPosition) throw new SpoolCorruptedException($"{seg.Path}: record {rec.Seq} where {_nextSeqAtPosition} was expected");
                _nextSeqAtPosition++;
                if (rec.Seq >= from) result.Add(rec);
                if (result.Count >= spool._o.ForwardBatchMessages) break;
            }
            return result;
        }

        private SpoolRecord? ReadOne()
        {
            Span<byte> header = stackalloc byte[SpoolCodec.HeaderSize];
            var pos = _fs!.Position;
            if (_fs.Read(header) != header.Length) return null;
            var length = BinaryPrimitives.ReadUInt32LittleEndian(header[4..]);
            if (length > SpoolCodec.MaxBodySize) return null;
            var total = SpoolCodec.HeaderSize + (int)length;
            if (_buf.Length < total) _buf = new byte[total];
            header.CopyTo(_buf);
            _fs.ReadExactly(_buf, SpoolCodec.HeaderSize, (int)length);
            if (!SpoolCodec.TryRead(_buf.AsSpan(0, total), out var rec, out _)) { _fs.Position = pos; return null; }
            return rec;
        }
    }

    // ---------------------------------------------------------------- cursor persistence and retention

    private async Task PersisterLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_o.CursorFlushInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { PersistCursor(); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { Note(ex); } // e.g. a virus scanner holds the file: later
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { Fail(ex); }
    }

    private void PersistCursor()
    {
        var delivered = Interlocked.Read(ref _deliveredSeq);
        if (delivered <= _persistedCursor) return;
        WriteAtomic(CursorFile, w => BinaryPrimitives.WriteInt64LittleEndian(w(8), delivered));
        _persistedCursor = delivered;
        DeleteDeliveredSegments(keepActive: true);
    }

    private void DeleteDeliveredSegments(bool keepActive)
    {
        List<Segment> doomed;
        lock (_segLock)
        {
            doomed = _segments.Where((s, i) => s.Count > 0 && s.LastSeq <= _persistedCursor && (!keepActive || i < _segments.Count - 1)).ToList();
        }
        if (doomed.Count == 0) return;
        foreach (var s in doomed)
        {
            File.Delete(s.Path);
            lock (_segLock) _segments.Remove(s);
            Interlocked.Add(ref _diskBytes, -s.Length);
        }
        _spaceSignal.Release();
    }

    private static long? ReadCursor(string directory) =>
        ReadAtomic(System.IO.Path.Combine(directory, CursorFile)) is { Length: 8 } data ? BinaryPrimitives.ReadInt64LittleEndian(data) : null;

    /// <summary>[magic u32][length u32][crc u32][body] written to a temp file, fsync'd, then renamed over the target.</summary>
    private void WriteAtomic(string name, Action<Func<int, Span<byte>>> fill)
    {
        var body = new ArrayBufferWriter<byte>();
        fill(n => { var s = body.GetSpan(n)[..n]; body.Advance(n); return s; });
        var path = System.IO.Path.Combine(_o.Directory, name);
        var tmp = path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1, FileOptions.WriteThrough))
        {
            Span<byte> header = stackalloc byte[12];
            BinaryPrimitives.WriteUInt32LittleEndian(header, CursorMagic);
            BinaryPrimitives.WriteUInt32LittleEndian(header[4..], (uint)body.WrittenCount);
            BinaryPrimitives.WriteUInt32LittleEndian(header[8..], Crc32C.Compute(body.WrittenSpan));
            fs.Write(header);
            fs.Write(body.WrittenSpan);
            fs.Flush(true);
        }
        File.Move(tmp, path, overwrite: true);
    }

    private static byte[]? ReadAtomic(string path)
    {
        if (!File.Exists(path)) return null;
        var data = File.ReadAllBytes(path);
        if (data.Length < 12 || BinaryPrimitives.ReadUInt32LittleEndian(data) is not (CursorMagic or CursorMagicV2)) return null;
        var length = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4));
        if (12 + length != data.Length) return null;
        var body = data.AsSpan(12);
        return Crc32C.Compute(body) == BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(8)) ? body.ToArray() : null;
    }

    // ---------------------------------------------------------------- failure and shutdown

    private void Note(Exception ex)
    {
        _lastError = $"{ex.GetType().Name}: {ex.Message}";
        _lastErrorAt = DateTimeOffset.UtcNow;
    }

    /// <summary>Local I/O failure: stop accepting and fail every pending acknowledgement (the source retains its log).</summary>
    private void Fail(Exception ex)
    {
        _fatal ??= ex;
        Note(ex);
        lock (_waitGate)
        {
            foreach (var (_, tcs) in _waiters) tcs.TrySetException(new InvalidOperationException("spool failed", ex));
            _waiters.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _writeQueue.Writer.TryComplete();
        try { if (_writer is not null) await _writer; } catch (OperationCanceledException) { }
        await _cts.CancelAsync();
        try { if (_forwarder is not null) await _forwarder; if (_persister is not null) await _persister; } catch (OperationCanceledException) { }
        try { if (_fatal is null) PersistCursor(); } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { Note(ex); }
        lock (_segLock) { _active?.Dispose(); _active = null; }
        Fail(new ObjectDisposedException(nameof(SpoolSink)));
        try { await _inner.DisposeAsync(); }
        finally
        {
            _lock?.Dispose();
            _cts.Dispose();
        }
    }

    // ---------------------------------------------------------------- test hooks

    internal bool IsInDedupeWindow(string id) { lock (_appendLock) return _recentIds.Contains(id); }

    /// <summary>Test hook: the next batch is written (and may be forwarded) but the writer dies before its fsync.</summary>
    internal volatile bool StallBeforeFsync;
    private readonly TaskCompletionSource _stalled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _stallLengthBefore = -1;
    internal Task Stalled => _stalled.Task;

    /// <summary>Simulates a process crash: loops stop without draining, nothing more is written or persisted, and every
    /// unacknowledged waiter fails. Returns the active segment path and its last fsync'd length so a test can tear the tail.</summary>
    internal async Task<(string? ActivePath, long DurableLength, long CurrentLength)> SimulateCrashAsync()
    {
        long durableLength, currentLength;
        string? path;
        await _cts.CancelAsync();
        _writeQueue.Writer.TryComplete();
        try { if (_writer is not null) await _writer; } catch (OperationCanceledException) { }
        try { if (_forwarder is not null) await _forwarder; if (_persister is not null) await _persister; } catch (OperationCanceledException) { }
        lock (_segLock)
        {
            path = _segments.Count > 0 ? _segments[^1].Path : null;
            currentLength = _segments.Count > 0 ? _segments[^1].Length : 0;
            durableLength = _stallLengthBefore >= 0 ? _stallLengthBefore : currentLength; // every completed batch was fsync'd
            _active?.Dispose();
            _active = null;
        }
        _lock?.Dispose();
        Fail(new InvalidOperationException("simulated crash"));
        return (path, durableLength, currentLength);
    }
}
