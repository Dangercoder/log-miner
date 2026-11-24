using System.Diagnostics;
using Hegel;
using Hegel.Xunit;
using LogMiner.Sinks;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>
/// Stateful model test of <see cref="SpoolSink"/> against a faulty remote. Random histories of sends, replays (the pipeline
/// resending after a restart), remote failures, graceful restarts and crashes - including crashes after a batch was written
/// and forwarded but before its fsync, with the unsynced tail torn and garbage appended - must keep:
/// <list type="bullet">
/// <item><b>No phantoms:</b> the remote only ever holds messages that were sent.</item>
/// <item><b>Order:</b> per source stream, first deliveries are in position order (duplicates are allowed, reordering is not) -
/// given a producer that keeps the pipeline's contract: after a crash, restart or failed send it re-sends everything not yet
/// acknowledged, in order, before anything new.</item>
/// <item><b>No loss:</b> after settling, every message whose acknowledgement completed is held by the remote.</item>
/// <item><b>De-duplication:</b> replaying messages the spool already holds appends nothing.</item>
/// </list>
/// </summary>
public class SpoolProperties
{
    [HegelTest(TestCases = 60)]
    public void Acknowledged_messages_are_delivered_in_order_across_failures_and_crashes(TestCase tc)
    {
        using var machine = new SpoolMachine(tc.Draw(Generators.SampledFrom(new[] { SpoolAck.Spooled, SpoolAck.Delivered }), "ackOn"));
        Stateful.Run(machine, tc, new StatefulOptions { StepCount = 25 });
        machine.Settle();
    }

    private static T Sync<T>(Func<Task<T>> f) => LogMiner.Tests.Blocking.Run(f);

    /// <summary>Waits for a task to finish (successfully or not) without throwing.</summary>
    private static void WaitQuietly(Task t, TimeSpan timeout) => Sync(() => Task.WhenAny(t, Task.Delay(timeout)));
    private static void Sync(Func<Task> f) => LogMiner.Tests.Blocking.Run(f);

    private sealed class SpoolMachine : IDisposable
    {
        private static readonly string[] Streams = ["db1#a", "db2#a", "db3#b"];
        private readonly SpoolAck _ackOn;
        private readonly string _dir = Path.Combine(Path.GetTempPath(), "lm-spool-" + Guid.NewGuid().ToString("N"));
        private readonly FaultySink _remote = new();
        private readonly Dictionary<string, int> _nextPosition = Streams.ToDictionary(s => s, _ => 1);
        private readonly Dictionary<string, List<SinkMessage>> _sentByStream = Streams.ToDictionary(s => s, _ => new List<SinkMessage>());
        private readonly HashSet<string> _sentIds = [];
        private readonly List<(string[] Ids, Task Ack)> _acks = [];
        private readonly List<SinkMessage> _sentInOrder = [];
        private bool _mustReplay;
        private SpoolSink _spool;
        private int _payloadCounter;

        public SpoolMachine(SpoolAck ackOn)
        {
            _ackOn = ackOn;
            _spool = Open();
        }

        private SpoolSink Open()
        {
            var s = new SpoolSink(_remote, new SpoolOptions
            {
                Directory = _dir, AckOn = _ackOn, SegmentBytes = 2048, ForwardBatchMessages = 4, ForwardMaxInFlightMessages = 12,
                LiveQueueMessages = 6, CursorFlushInterval = TimeSpan.FromMilliseconds(5), RetryMin = TimeSpan.FromMilliseconds(1), RetryMax = TimeSpan.FromMilliseconds(5),
            });
            Sync(() => s.StartAsync(CancellationToken.None));
            return s;
        }

        private SinkMessage NewMessage(TestCase tc, string stream)
        {
            var pos = _nextPosition[stream]++;
            var payload = tc.Draw(Generators.Binary().MaxSize(300), "payload");
            var m = new SinkMessage
            {
                Database = stream.Split('#')[0], Stream = stream, Position = new SourcePosition(new Lsn(1, (uint)pos, 1), 0),
                Destination = $"cdc.test.{stream.Split('#')[0]}", Key = stream, MessageId = $"{stream}:{pos}:{++_payloadCounter}",
                Payload = payload, Headers = [new("lm-db", stream)],
            };
            _sentIds.Add(m.MessageId);
            _sentByStream[stream].Add(m);
            _sentInOrder.Add(m);
            return m;
        }

        private void Send(IReadOnlyList<SinkMessage> batch, bool expectAck = true)
        {
            try
            {
                var ack = Sync(async () => await _spool.SendAsync(batch, CancellationToken.None));
                _acks.Add((batch.Select(m => m.MessageId).ToArray(), ack));
                if (expectAck) WaitQuietly(ack, TimeSpan.FromMilliseconds(_ackOn == SpoolAck.Spooled ? 1000 : 20));
                if (ack.IsFaulted) _mustReplay = true;
            }
            catch (InvalidOperationException) { _mustReplay = true; } // spool failed (after a simulated crash): not acknowledged
        }

        /// <summary>The pipeline's contract after a failure: re-send everything not acknowledged, in order, before anything new.</summary>
        private void ReplayUnacknowledged()
        {
            if (!_mustReplay) return;
            _mustReplay = false;
            var acked = Acked();
            var pending = _sentInOrder.Where(m => !acked.Contains(m.MessageId)).ToList();
            for (var i = 0; i < pending.Count; i += 4) Send(pending.Skip(i).Take(4).ToList());
        }

        [Rule(Weight = 5)]
        public void SendNew(TestCase tc)
        {
            ReplayUnacknowledged();
            var n = tc.Draw(Generators.Integers().Min(1).Max(4), "n");
            var batch = new List<SinkMessage>();
            for (var i = 0; i < n; i++) batch.Add(NewMessage(tc, tc.Draw(Generators.SampledFrom(Streams), "stream")));
            Send(batch);
        }

        [Rule]
        public void ReplaySuffix(TestCase tc)
        {
            ReplayUnacknowledged();
            var stream = tc.Draw(Generators.SampledFrom(Streams), "stream");
            var sent = _sentByStream[stream];
            if (sent.Count == 0) return;
            var k = tc.Draw(Generators.Integers().Min(1).Max(Math.Min(5, sent.Count)), "k");
            var replay = sent.TakeLast(k).ToList();
            var before = _spool.LastSeq;
            var allSpooled = replay.All(m => _spool.IsInDedupeWindow(m.MessageId));
            Send(replay);
            if (allSpooled) Assert.Equal(before, _spool.LastSeq); // exact duplicates of spooled messages: nothing appended
        }

        [Rule]
        public void RemoteFails(TestCase tc)
        {
            var n = tc.Draw(Generators.Integers().Min(1).Max(3), "failures");
            var outcomes = new List<SinkOutcome>();
            for (var i = 0; i < n; i++)
                outcomes.Add(tc.Draw(Generators.SampledFrom(new[] { SinkOutcome.FailAfterStore, SinkOutcome.FailWithoutStore, SinkOutcome.FailHalfStored, SinkOutcome.Throw }), "outcome"));
            _remote.Script(outcomes);
        }

        [Rule]
        public void GracefulRestart(TestCase tc)
        {
            Sync(async () => await _spool.DisposeAsync());
            _spool = Open();
            _mustReplay = true;
        }

        [Rule(Weight = 2)]
        public void Crash(TestCase tc)
        {
            var beforeFsync = tc.Draw(Generators.Booleans(), "crashBeforeFsync");
            if (beforeFsync)
            {
                ReplayUnacknowledged(); // contract: never send something new before re-sending what was lost
                _spool.StallBeforeFsync = true;
                var batch = new List<SinkMessage> { NewMessage(tc, tc.Draw(Generators.SampledFrom(Streams), "stream")) };
                Send(batch, expectAck: false); // written (maybe forwarded) but never fsync'd: must not be acknowledged
                WaitQuietly(_spool.Stalled, TimeSpan.FromSeconds(2));
            }
            var (path, durable, current) = Sync(() => _spool.SimulateCrashAsync());
            if (path is not null && File.Exists(path))
            {
                // the unsynced tail may be partially persisted, then garbage may follow
                var keep = tc.Draw(Generators.Longs().Min(durable).Max(Math.Max(durable, current)), "keepBytes");
                var garbage = tc.Draw(Generators.Binary().MaxSize(40), "garbage");
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
                fs.SetLength(Math.Min(keep, fs.Length));
                fs.Seek(0, SeekOrigin.End);
                fs.Write(garbage);
            }
            _spool = Open();
            _mustReplay = true;
        }

        [Rule]
        public void SettleNow(TestCase tc) => Settle();

        private HashSet<string> Acked()
        {
            var acked = new HashSet<string>();
            foreach (var (ids, ack) in _acks) if (ack.IsCompletedSuccessfully) acked.UnionWith(ids);
            return acked;
        }

        [Invariant]
        public void Remote_holds_only_sent_messages(TestCase tc)
        {
            foreach (var m in _remote.Stored) Assert.Contains(m.MessageId, _sentIds);
        }

        [Invariant]
        public void First_deliveries_are_in_position_order_per_stream(TestCase tc)
        {
            var seen = new HashSet<string>();
            var last = new Dictionary<string, SourcePosition>();
            foreach (var m in _remote.Stored)
            {
                if (!seen.Add(m.MessageId)) continue;
                if (last.TryGetValue(m.Stream!, out var prev)) Assert.True(m.Position!.Value > prev, $"{m.MessageId} delivered after {prev}");
                last[m.Stream!] = m.Position!.Value;
            }
        }

        /// <summary>Heal the remote, wait until the spool has forwarded everything, then check nothing acknowledged is missing.</summary>
        public void Settle()
        {
            _remote.Heal();
            var sw = Stopwatch.StartNew();
            while (_spool.DeliveredSeq < _spool.LastSeq && sw.Elapsed < TimeSpan.FromSeconds(3)) Thread.Sleep(2);
            Assert.True(_spool.DeliveredSeq >= _spool.LastSeq, $"spool did not drain: delivered {_spool.DeliveredSeq} of {_spool.LastSeq}");
            foreach (var (_, ack) in _acks) WaitQuietly(ack, TimeSpan.FromMilliseconds(200));
            var stored = _remote.Stored.Select(m => m.MessageId).ToHashSet();
            var missing = Acked().Where(id => !stored.Contains(id)).ToList();
            Assert.True(missing.Count == 0, $"acknowledged but never delivered: {string.Join(", ", missing)}");
        }

        public void Dispose()
        {
            try { Sync(async () => await _spool.DisposeAsync()); } catch (InvalidOperationException) { }
            _remote.Shutdown();
            try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
        }
    }
}
