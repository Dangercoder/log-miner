using System.Diagnostics;
using System.Text.Json;
using Hegel;
using Hegel.Xunit;
using LogMiner.Capture;
using LogMiner.Log;
using LogMiner.Pipeline;
using LogMiner.Schema;
using LogMiner.Sinks;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>
/// End-to-end model test of the capture pipeline: <see cref="DatabaseCapture"/> + <see cref="CheckpointCoordinator"/> (+ optionally
/// <see cref="SpoolSink"/>) against a simulated SQL Server log. The simulator models what matters for durability:
/// interleaved transactions with row locks, savepoints / partial rollbacks (compensation records), aborts, activity on tables
/// that are not captured, DDL (index rebuilds that give the table a new rowset, column type changes that give it a new physical layout,
/// on their own, inside transactions with row changes, rolled back, or committing while the catalog is being read), the server-side record
/// filter, and log truncation exactly as replication allows it (nothing newer than
/// the oldest active transaction or the oldest replicated transaction not yet released by sp_repldone). Reading from an LSN that
/// is not in the log raises an error, like error 9005. The sink fails in every way a remote can; the process crashes at random.
/// <para>Properties:</para>
/// <list type="bullet">
/// <item><b>Exactly the committed changes</b> (after de-duplication by message id) are delivered, with correct before/after
/// images, in commit order - nothing lost, nothing invented, nothing from rolled-back work.</item>
/// <item><b>Log release safety:</b> sp_repldone is never called beyond the durable checkpoint.</item>
/// <item><b>Gaps are never silent and never invented:</b> when someone else releases the log and it is truncated before it was read, a
/// gap is recorded before the stream goes on; without a recorded gap nothing is missing; ordinary truncation is not a gap.</item>
/// </list>
/// </summary>
public class CapturePipelineProperties
{
    /// <summary>Coverage counters (diagnostics): how often the interesting situations actually occur in generated histories.</summary>
    public static readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> Coverage = new();
    private static void Cover(string what) => Coverage.AddOrUpdate(what, 1, (_, n) => n + 1);

    /// <summary>The model test is only as good as the scenarios it reaches: each of these must occur in a normal run.</summary>
    [Fact]
    public void Model_test_reaches_the_interesting_scenarios()
    {
        Coverage.Clear();
        Property.Test(tc => Every_committed_change_is_delivered_once_in_commit_order(tc), new Settings { TestCases = 160 });
        string[] required =
        [
            "pump", "settle-with-5+-events", "crash", "crash-with-checkpoint", "crash-with-open-txn", "remote-fails(spool)", "remote-fails(direct)",
            "checkpoint-while-pumping", "latest-start", "snapshot-requested", "snapshot-finished", "chunk-read-fails",
            "ddl", "ddl-in-transaction", "ddl-rolled-back", "ddl-during-catalog-read", "crash-behind-ddl", "hidden-change",
            "foreign-release", "gap", "resumed-after-truncation", "engine-forgets",
            "checkpoint-while-preparing", "fault-while-preparing", "crash-while-preparing", "nothing-left-of-it", "power-failure-with-loss", "rewound-to-safe",
            "settled-in-block", "refused-in-block", "load-arrived-whole",
        ];
        var report = string.Join(" | ", Coverage.OrderBy(k => k.Key).Select(k => $"{k.Key}={k.Value}"));
        var missing = required.Where(r => !Coverage.ContainsKey(r)).ToList();
        Assert.True(missing.Count == 0, $"never reached: {string.Join(", ", missing)}; coverage: {report}");
    }

    [HegelTest(TestCases = 80)]
    public void Every_committed_change_is_delivered_once_in_commit_order(TestCase tc)
    {
        var useSpool = tc.Draw(Generators.Booleans(), "useSpool");
        // "Latest" start over pre-existing rows: only a historical load (snapshot) can deliver them
        var preexisting = tc.Draw(Generators.Booleans(), "latestWithHistory") ? tc.Draw(Generators.Integers().Min(1).Max(12), "historyRows") : 0;
        var finalFailures = preexisting > 0 ? tc.Draw(Generators.Integers().Min(0).Max(3), "failuresDuringFinalSnapshot") : 0;
        var maxRows = tc.Draw(Generators.Integers().Min(2).Max(40), "maxRowsPerRead");
        var maxBatch = tc.Draw(Generators.Integers().Min(1).Max(8), "maxBatchEvents");
        using var m = new PipelineMachine(useSpool, preexisting, finalFailures, maxRows, maxBatch, delayed: tc.Draw(Generators.Booleans(), "delayedRelease"));
        Stateful.Run(m, tc, new StatefulOptions { StepCount = 60 });
        m.Settle();
    }

    [Fact]
    public void What_is_gone_from_the_block_of_the_position_is_reported()
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0, maxRows: 3); // the marker, the change, the commit
        m.GoneFromTheBlockOfThePosition();
        m.Settle();
        Assert.Equal(1, m.Gaps);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void What_is_released_is_behind_the_safe_position_that_is_stored(int looksBefore)
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0, maxRows: 40, delayed: true);
        m.PowerFailsRightAfterALook(looksBefore);
        m.Settle();
        Assert.Equal(0, m.Gaps);
    }

    [Fact]
    public void What_is_lost_after_an_upgrade_is_published_again()
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0, maxRows: 40, delayed: true);
        m.LostAfterAnUpgrade();
        m.Settle();
        Assert.Equal(0, m.Gaps);
    }

    /// <summary>A delete that is gone was never published, and no row of a load says so. Its markers do: what a whole load does not mention is not there.</summary>
    [Fact]
    public void Deletes_that_are_gone_are_found_by_the_next_load()
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0);
        m.DeletesGoneInAGap(rows: 9, deleted: 3);
        m.Settle();
        Assert.Equal(1, m.Gaps);
        Assert.Equal(3, m.PhantomsFound);
    }

    /// <summary>Found by the model: the stream began again wherever the log began by then, and nothing said so.</summary>
    [Fact]
    public void What_was_lost_before_anything_was_safe_is_reported_when_it_cannot_be_published_again()
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0, delayed: true);
        m.LostAndTruncatedInTheFirstSeconds();
        m.Settle();
        Assert.Equal(1, m.Gaps);
    }

    /// <summary>Found by the model: after a restart the log is re-read from the oldest transaction that was open; what was published before is
    /// passed over again, which must not take the position back with it when a checkpoint is written meanwhile.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void Re_reading_after_a_restart_does_not_move_the_position_back(int pumps)
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0);
        m.RestartBehindALongTransaction(pumps);
        m.The_durable_position_never_moves_back(null!);
        m.Settle();
    }

    /// <summary>Found by the model: the sink fails while a checkpoint is being saved. The stream rewinds to the previous one, then the newer one
    /// becomes durable: the next checkpoint must not claim less.</summary>
    [Fact]
    public void A_fault_while_a_checkpoint_is_saved_does_not_move_the_position_back()
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0);
        m.FaultWhileACheckpointIsSaved();
        m.The_durable_position_never_moves_back(null!);
        m.Settle();
        m.The_durable_position_never_moves_back(null!);
    }

    /// <summary>Found by the model: the position was truncated, reading went on behind it and was checkpointed, and then the miner was restarted
    /// before the commit of a transaction whose beginning is gone. It must still not be published in part.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void A_transaction_cut_by_a_truncation_is_not_published_in_part_after_a_restart(int pumpsBeforeRestart)
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0);
        m.CutATransactionAndRestart(pumpsBeforeRestart);
        m.Settle();
        Assert.True(m.Gaps > 0, "the cut transaction was not recorded");
    }

    /// <summary>Found by the model: the start point of a "Latest" start is durable, and before the first read somebody else releases the log and it is
    /// truncated. The read then begins behind the changes that are gone, which must be reported.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Log_that_is_gone_before_the_first_read_is_reported(bool releasedByOthers)
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 3);
        m.TruncateBeforeTheFirstRead(releasedByOthers);
        m.Settle();
        Assert.Equal(releasedByOthers, m.Gaps > 0);
    }

    /// <summary>An acknowledgement may come before the destination has the event on disk. SQL Server keeps every change for at least the release
    /// interval after its checkpoint, however many follow.</summary>
    [Fact]
    public void Log_is_released_no_sooner_than_the_release_interval()
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0);
        m.ReleaseWaits(TimeSpan.FromMilliseconds(400));
    }

    /// <summary>A transaction that cannot be decoded stops the stream. What committed before it still goes out; it and everything behind it waits,
    /// however often it is tried again and whatever is checkpointed meanwhile - and comes out once it can be decoded.</summary>
    [HegelTest(TestCases = 60)]
    public void A_transaction_that_cannot_be_decoded_holds_back_what_follows_and_nothing_else(TestCase tc)
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0, maxRows: tc.Draw(Generators.Integers().Min(2).Max(40), "maxRowsPerRead"), maxBatch: tc.Draw(Generators.Integers().Min(1).Max(8), "maxBatchEvents"));
        m.UndecodableTransaction(tc);
        m.Settle();
    }

    /// <summary>The restart position is only where reading starts again: the commit records say where each transaction began. One that is too
    /// late (by whatever mistake) must not lose anything.</summary>
    [HegelTest(TestCases = 60)]
    public void A_restart_position_that_is_too_late_loses_nothing(TestCase tc)
    {
        using var m = new PipelineMachine(useSpool: false, preexistingRows: 0, maxRows: tc.Draw(Generators.Integers().Min(2).Max(40), "maxRowsPerRead"), maxBatch: tc.Draw(Generators.Integers().Min(1).Max(8), "maxBatchEvents"));
        m.RestartTooLate(tc);
        m.Settle();
        Assert.Equal(0, m.Gaps);
    }

    /// <summary>One database with its own log, destination and stored position (several of them share one release pass, as in the engine).</summary>
    private sealed class Tenant : IDisposable
    {
        public void Dispose() => Sink.Shutdown();
        public readonly MemoryCheckpointStore Store = new();
        public readonly FaultySink Sink = new();
        public readonly SimulatedLog Db;
        public DatabaseCapture Capture;
        private readonly EventEncoder _encoder = new(new EncoderOptions { ServerName = "test" });
        private readonly CaptureSettings _settings = new() { StartPosition = StartPosition.Earliest, MaxRowsPerRead = 40, MaxBatchEvents = 8 };
        private readonly int _id;

        public Tenant(int id)
        {
            _id = id;
            Db = new SimulatedLog(() => Sync(() => Store.LoadAsync("db", CancellationToken.None))?.LastCommitLsn ?? default);
            Db.HasPosition = () => true;
            Capture = new DatabaseCapture(id, "db", "inc1", null, StartPosition.Earliest);
        }

        public void Pump()
        {
            for (var i = 0; i < 1000 && Sync(() => Capture.PumpAsync(Db, _encoder, Sink, _settings, CancellationToken.None, Db, null)); i++) { }
        }
        public void Save() => Sync(() => CheckpointCoordinator.SaveAsync([Capture], Store, CancellationToken.None, Sink.ReceiptOf));
        public void Work() { Db.CommitNewInsert(); Pump(); Save(); }

        /// <summary>The process starts again; what CaptureEngine.CheckReceiptsAsync does before anything is sent.</summary>
        public ReceiptState Restart()
        {
            var position = Sync(() => Store.LoadAsync("db", CancellationToken.None))!;
            var state = position.Receipt is { } receipt
                ? Sync(() => Sink.VerifyAsync(new Dictionary<string, string> { ["db"] = receipt }, CancellationToken.None).AsTask())["db"] : ReceiptState.Unknown;
            if (state == ReceiptState.Lost)
            {
                position = position.Rewound() ?? throw new Xunit.Sdk.XunitException("no safe position");
                Sync(() => Store.SaveAsync([position], CancellationToken.None));
            }
            Sink.NewProducer();
            Capture = new DatabaseCapture(_id, "db", "inc1", position, StartPosition.Earliest);
            return state;
        }
    }

    private sealed class StallingReleaser(Tenant a, Tenant b) : ILogReleaser
    {
        /// <summary>What happens while the next sp_repldone of database a waits.</summary>
        public Action? Stall { get; set; }
        public async Task ReleaseAsync(DatabaseRef database, Lsn beginLsn, Lsn commitLsn, CancellationToken ct)
        {
            if (database.Id != 5) { await b.Db.ReleaseAsync(database, beginLsn, commitLsn, ct); return; }
            var stall = Stall;
            Stall = null;
            if (stall is not null) await Task.Run(stall, ct);
            await a.Db.ReleaseAsync(database, beginLsn, commitLsn, ct);
        }
    }

    /// <summary>
    /// "The position that was durable at the last look is an interval old" holds only if a look is stamped with the time it happens at. A release pass
    /// stamps every look with the time the pass began, and waits for sp_repldone database by database (30 s command timeout): behind one that waits,
    /// a position that is milliseconds old is recorded as looked at when the pass began, and the next pass makes it the safe position at once.
    /// </summary>
    [Fact]
    public void A_release_that_waits_in_one_database_does_not_make_a_fresh_position_of_another_safe()
    {
        var interval = TimeSpan.FromMilliseconds(1500);
        using var a = new Tenant(5);
        using var b = new Tenant(6);
        var releaser = new StallingReleaser(a, b);
        void Pass() => Sync(() => CheckpointCoordinator.ReleaseAsync([a.Capture, b.Capture], releaser, interval, null, CancellationToken.None)); // as CaptureEngine.ReleaseAsync calls it
        void Wait() => Thread.Sleep(interval + TimeSpan.FromMilliseconds(50));
        try
        {
            a.Work(); b.Work(); Pass();
            Wait(); a.Work(); b.Work(); Pass();
            Wait(); a.Work(); b.Work(); Pass(); a.Save(); b.Save();
            Wait(); Pass(); a.Save(); b.Save();
            // the premise: both are idle, their newest position is the safe one, and a release of each is due
            Assert.Equal(a.Capture.Durable!.LastCommitLsn, a.Capture.Durable.Safe!.CommitLsn);
            Assert.Equal(b.Capture.Durable!.LastCommitLsn, b.Capture.Durable.Safe!.CommitLsn);
            Wait();

            // sp_repldone waits in database a for longer than the interval; at the end of that time a change of b is published and its position saved
            Stopwatch? durableSince = null;
            Lsn fresh = default;
            releaser.Stall = () =>
            {
                Thread.Sleep(interval + TimeSpan.FromMilliseconds(300));
                b.Sink.Sync(); // the destination has everything up to here on disk
                b.Db.CommitNewInsert();
                b.Pump();
                b.Save();
                fresh = b.Capture.Durable!.LastCommitLsn;
                durableSince = Stopwatch.StartNew();
            };
            Pass();     // a's release waits; then b is looked at, with the time at which the pass began
            Assert.NotNull(durableSince);
            Pass();     // the next tick of the timer (which is overdue)
            b.Save();   // the checkpoint loop
            var age = durableSince!.Elapsed;
            var safe = b.Capture.Durable!.Safe!.CommitLsn;
            Pass();     // releases behind it

            // the power fails before the destination's next sync
            var lost = b.Sink.LoseTail(12);
            var state = b.Restart();
            b.Pump();
            b.Save();
            var delivered = b.Sink.Stored.Select(m => m.MessageId).Distinct().Count();
            Assert.True(delivered == b.Db.Expected.Count,
                $"{b.Db.Expected.Count - delivered} committed change(s) of b are not in the destination and are not published again: the position {fresh} became the safe position ({safe}) " +
                $"{age.TotalMilliseconds:F0} ms after it was saved (release interval {interval.TotalMilliseconds:F0} ms); the destination lost {lost} message(s), the receipt was {state}, " +
                $"the stream went back to {b.Capture.Durable!.LastCommitLsn}, sp_repldone through {b.Db.ReleasedThrough}");
        }
        finally { a.Sink.Shutdown(); b.Sink.Shutdown(); }
    }

    /// <summary>
    /// Rows of a load do not move the position, so no checkpoint is saved for them, and the receipt that is stored stays that of an older message. The
    /// destination loses the end of the load; the receipt is still there; nothing says that anything was lost, and the request stays "done".
    /// </summary>
    [Fact]
    public void The_end_of_a_load_that_the_destination_lost_is_noticed_at_the_next_start()
    {
        using var t = new Tenant(5);
        try
        {
            for (var i = 0; i < 4; i++) t.Work();
            t.Sink.Sync(); // on disk
            var load = new TableSnapshot(SimulatedLog.Layout, "load1", 3);
            t.Capture.RequestSnapshot(load);
            var sw = Stopwatch.StartNew();
            while (!load.Done && sw.Elapsed < TimeSpan.FromSeconds(10)) { t.Pump(); t.Save(); }
            Assert.True(load.Done && !load.Failed, "the load ended (the engine reports its request as done)");
            t.Save();
            var acknowledged = t.Sink.ReceiptOf("db");
            var stored = Sync(() => t.Store.LoadAsync("db", CancellationToken.None))!.Receipt;
            var lost = t.Sink.LoseTail(12); // the power fails before the destination's next sync
            Assert.True(lost > 0);
            var state = t.Restart();
            Assert.True(state == ReceiptState.Lost,
                $"the destination lost {lost} acknowledged message(s) of the load (rows and its end marker); the newest acknowledged message was {acknowledged}, the receipt stored with the position " +
                $"is {stored}, and it is {state}: nothing is published or requested again");
        }
        finally { t.Sink.Shutdown(); }
    }

    private static T Sync<T>(Func<Task<T>> f) => LogMiner.Tests.Blocking.Run(f);
    private static void Sync(Func<Task> f) => LogMiner.Tests.Blocking.Run(f);

    private sealed record Expected(Lsn CommitLsn, int Seq, string Op, int Id, int? Before, int? After);

    /// <summary>Checkpoint store whose next save can be held open, to interleave work between "save" and "release".</summary>
    private sealed class GatedCheckpointStore : ICheckpointStore
    {
        private readonly MemoryCheckpointStore _inner = new();
        private TaskCompletionSource? _gate;   // consumed by the next save
        private TaskCompletionSource? _armed;  // what Open() releases, even after the save consumed _gate
        private TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => _entered.Task;
        public void Arm()
        {
            _armed = _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        public void Open() { _armed?.TrySetResult(); _gate = null; }

        public Task<Checkpoint?> LoadAsync(string database, CancellationToken ct) => _inner.LoadAsync(database, ct);

        /// <summary>First time a checkpoint was saved that claims less than the one before it.</summary>
        public string? Regression { get; private set; }
        /// <summary>The destination lost what it had acknowledged: the position goes back on purpose.</summary>
        public bool Rewinding { get; set; }
        /// <summary>The first checkpoint ever saved: where the stream's guaranteed range starts.</summary>
        public Checkpoint? First { get; private set; }
        public Checkpoint? Last { get; private set; }

        public async Task SaveAsync(IReadOnlyCollection<Checkpoint> checkpoints, CancellationToken ct)
        {
            var gate = _gate;
            _gate = null;
            if (gate is not null) { _entered.TrySetResult(); await gate.Task; }
            foreach (var c in checkpoints)
                if (!Rewinding && await _inner.LoadAsync(c.Database, ct) is { } before && c.LastCommitLsn < before.LastCommitLsn)
                    Regression ??= $"the durable position went back from {before.LastCommitLsn} to {c.LastCommitLsn}";
            await _inner.SaveAsync(checkpoints, ct);
            First ??= checkpoints.FirstOrDefault();
            Last = checkpoints.LastOrDefault() ?? Last;
        }
    }

    /// <summary>A single-table database whose log the pipeline mines.</summary>
    private sealed class SimulatedLog : ILogSource, ILogReleaser, ISnapshotSource
    {
        public const long UntrackedAu = 999;
        private sealed record Op(LogOperation Kind, long Au, int Id, int? Before, int? After, Lsn Previous, bool Hidden = false, Action? UndoDdl = null);
        private sealed class Txn
        {
            public required ulong Xid;
            public required Lsn Begin;
            /// <summary>The transaction's newest record (head of its backward chain).</summary>
            public required Lsn Last;
            public readonly List<Op> Ops = [];
            public readonly Stack<int> Savepoints = new();
            /// <summary>Replicated records written so far (even if later rolled back to a savepoint): with any, replication holds its log.</summary>
            public int Replicated;
            public bool HadTracked => Replicated > 0;
        }

        private readonly List<LogRecord> _log = [];
        private readonly Dictionary<ulong, Txn> _open = [];
        private readonly Dictionary<int, (int? Value, ulong? Lock)> _rows = []; // value null = deleted (row slot kept while locked)
        private readonly List<(Lsn Begin, Lsn Commit)> _replicatedCommits = [];
        private readonly SortedDictionary<int, int> _committed = []; // committed table state (what a locking reader sees)

        /// <summary>A physical layout of the table: id, then every column "v" ever had (all but the last are dropped).</summary>
        private sealed record Physical(long Rowset, IReadOnlyList<int> VSizes)
        {
            public TableLayout Layout { get; } = MakeLayout(Rowset, VSizes);
        }

        private readonly List<(Lsn After, Physical Version)> _versions = [(default, new Physical(100, [4]))];
        private Physical Now => _versions[^1].Version;
        private long _nextRowset = 100;
        private long Au => Now.Rowset;
        /// <summary>DDL that commits while the catalog is being read.</summary>
        public Action? DuringCatalogRead { get; set; }
        public Lsn LastDdl { get; private set; }

        public IReadOnlyDictionary<int, int> Committed => _committed;
        /// <summary>Database activity while a snapshot chunk is being read (commits land inside [L, H)); argument: the chunk's lower key bound.</summary>
        public Action<int>? DuringChunkRead { get; set; }
        private readonly Func<Lsn> _durableCommit;
        private uint _block = 1;
        private ushort _slot;
        private ulong _nextXid = 0x400;
        private int _nextId = 1, _nextValue = 1;

        public SimulatedLog(Func<Lsn> durableCommit) => _durableCommit = durableCommit;

        public List<Expected> Expected { get; } = [];
        public Lsn ReleasedThrough { get; private set; }
        public Lsn LogEnd => End();
        public int OpenCount => _open.Count;

        public static readonly TableLayout Layout = MakeLayout(100, [4]);

        private static TableLayout MakeLayout(long rowset, IReadOnlyList<int> vSizes)
        {
            var columns = new List<ColumnLayout>
            {
                new() { ColumnId = 1, PartitionColumnId = 1, Name = "id", Type = SqlType.Int, MaxLength = 4, MaxInRowLength = 4, LeafOffset = 4, LeafNullBit = 1, KeyOrdinal = 1 },
            };
            var offset = 8;
            for (var i = 0; i < vSizes.Count; i++)
            {
                var live = i == vSizes.Count - 1;
                columns.Add(new ColumnLayout
                {
                    ColumnId = live ? 2 : 0, PartitionColumnId = live ? 2 : 1000 + i, Name = live ? "v" : $"#{i}", Type = vSizes[i] == 8 ? SqlType.BigInt : SqlType.Int,
                    MaxLength = (short)vSizes[i], MaxInRowLength = (short)vSizes[i], LeafOffset = (short)offset, LeafNullBit = (short)(i + 2), IsNullable = true, IsDropped = !live,
                });
                offset += vSizes[i];
            }
            return new TableLayout(5, "db", 1, "dbo", "t", rowset, DataCompression.None, columns);
        }

        /// <summary>A row as the current layout stores it. Dropped columns keep junk: decoding with the wrong layout shows.</summary>
        private byte[] Row(int id, int v)
        {
            var sizes = Now.VSizes;
            var end = 8 + sizes.Sum();
            var columns = sizes.Count + 1;
            var b = new byte[end + 2 + (columns + 7) / 8];
            b[0] = 0x10;
            BitConverter.GetBytes((ushort)end).CopyTo(b, 2);
            BitConverter.GetBytes(id).CopyTo(b, 4);
            var offset = 8;
            for (var i = 0; i < sizes.Count; i++)
            {
                var value = i == sizes.Count - 1 ? v : -7777 - i;
                if (sizes[i] == 8) BitConverter.GetBytes((long)value).CopyTo(b, offset); else BitConverter.GetBytes(value).CopyTo(b, offset);
                offset += sizes[i];
            }
            BitConverter.GetBytes((ushort)columns).CopyTo(b, end);
            return b;
        }

        /// <summary>Schema modification lock: no open transaction may have touched the table.</summary>
        private bool CanAlter(Txn? self) => _open.Values.All(t => t == self || (!t.HadTracked && t.Ops.All(o => o.Au == UntrackedAu)));

        public bool DdlOp(bool rebuild)
        {
            if (!CanAlter(null)) return false;
            var t = BeginTxn();
            Alter(t, rebuild);
            Commit(t);
            return true;
        }

        /// <summary>Row changes and DDL in one transaction: every record has the layout of its own moment.</summary>
        public bool DdlTransactionOp(bool rebuildFirst, bool abort)
        {
            if (!CanAlter(null)) return false;
            var t = BeginTxn();
            Insert(t, untracked: false);
            Alter(t, rebuildFirst);
            Insert(t, untracked: false);
            Alter(t, !rebuildFirst);
            Insert(t, untracked: false);
            if (abort) Abort(t); else Commit(t);
            return true;
        }

        /// <summary>The catalog change, then the marker the engine writes for replicated tables.</summary>
        private void Alter(Txn t, bool rebuild)
        {
            var at = Log(t, LogOperation.Other);
            var next = rebuild ? new Physical(++_nextRowset, [Now.VSizes[^1]]) : new Physical(Now.Rowset, [.. Now.VSizes, Now.VSizes[^1] == 4 ? 8 : 4]);
            var count = _versions.Count;
            t.Ops.Add(new Op(LogOperation.Other, Au, 0, null, null, t.Last, Hidden: true, UndoDdl: () => _versions.RemoveRange(count, _versions.Count - count)));
            _versions.Add((at, next));
            Log(t, LogOperation.ReplCommand, LogRecordBits.Replicated);
            t.Replicated++;
            LastDdl = at;
        }

        private Lsn Log(Txn t, LogOperation operation, LogRecordBits flags = LogRecordBits.None)
        {
            t.Last = Next();
            Append(new LogRecord { Lsn = t.Last, Operation = operation, Context = LogContext.Null, TransactionId = t.Xid, Flags = flags });
            return t.Last;
        }

        private Lsn Next()
        {
            if (++_slot > 30) { _block++; _slot = 1; }
            return new Lsn(1, _block, _slot);
        }

        /// <summary>log_end_lsn: the LSN the next record will get.</summary>
        private Lsn End() => _slot + 1 > 30 ? new Lsn(1, _block + 1, 1) : new Lsn(1, _block, (ushort)(_slot + 1));

        /// <summary>The next n chunk reads fail (lock timeout, ...).</summary>
        public int FailChunkReads { get; set; }

        public async Task<SnapshotChunk> ReadChunkAsync(TableLayout table, object?[]? afterKey, int chunkSize, CancellationToken ct)
        {
            await Task.Yield();
            if (FailChunkReads > 0) { FailChunkReads--; throw new TimeoutException("simulated lock timeout"); }
            var low = End();
            var after = afterKey is null ? int.MinValue : (int)afterKey[0]!;
            DuringChunkRead?.Invoke(after);                  // commits before the rows are read
            var rows = _committed.Where(kv => kv.Key > after).Take(chunkSize)
                .Select(kv => new LogMiner.Rows.RowImage(table, [kv.Key, kv.Value])).ToList();
            DuringChunkRead?.Invoke(after);                  // commits after the rows were read: the chunk now holds older values
            var high = End();
            return new SnapshotChunk { Table = table, Low = low, High = high, Rows = rows, LastKey = rows.Count == 0 ? null : [rows[^1].Values[0]] };
        }

        /// <summary>A block is closed when it is full or written to disk, and what a read returns is on disk: nothing is added to a block that was read
        /// (experiments/40).</summary>
        private void Append(LogRecord r)
        {
            if (_blockClosed || _inBlock == 7) { _logBlock++; _inBlock = 0; _blockClosed = false; }
            _inBlock++;
            _logBlockOf[r.Lsn] = _logBlock;
            _log.Add(r);
        }

        /// <summary>Other activity filled or wrote the block: the next record begins another.</summary>
        public void CloseBlock() => _blockClosed = true;

        private readonly Dictionary<Lsn, int> _logBlockOf = [];
        private int _logBlock, _inBlock;
        private bool _blockClosed;

        private Txn? Pick(TestCase tc) =>
            _open.Count == 0 ? null : _open[tc.Draw(Generators.SampledFrom(_open.Keys.Order().ToList()), "txn")];

        private Txn PickOrBegin(TestCase tc) => Pick(tc) ?? BeginTxn();

        public void Begin() => BeginTxn();

        /// <summary>A small committed transaction on the captured table (sets up work that the pipeline will acknowledge).</summary>
        /// <summary>Commits an update (or delete) of the first committed, unlocked row above <paramref name="afterKey"/> - i.e. a row
        /// of the chunk being read - to land conflicting changes inside chunk reads.</summary>
        public void MutateCommittedRow(bool delete, int afterKey = int.MinValue)
        {
            var id = _rows.Where(r => r.Key > afterKey && r.Value.Value is not null && r.Value.Lock is null).Select(r => r.Key).Order().FirstOrDefault();
            if (id == 0) { CommitNewInsert(); return; }
            var t = BeginTxn();
            if (delete) DeleteRow(t, id); else UpdateRow(t, id);
            Commit(t);
        }

        public void CommitNewInsert()
        {
            var t = BeginTxn();
            Insert(t, untracked: false);
            Commit(t);
        }

        private Txn BeginTxn()
        {
            var begin = Next();
            var t = new Txn { Xid = _nextXid++, Begin = begin, Last = begin };
            Append(new LogRecord { Lsn = t.Begin, Operation = LogOperation.BeginXact, Context = LogContext.Null, TransactionId = t.Xid, Flags = LogRecordBits.None });
            _open[t.Xid] = t;
            return t;
        }

        public void InsertOp(TestCase tc) => Insert(PickOrBegin(tc), untracked: false);
        public void UntrackedOp(TestCase tc) => Insert(PickOrBegin(tc), untracked: true);
        public void UpdateOp(TestCase tc) { var t = PickOrBegin(tc); if (PickRow(tc, t) is { } id) UpdateRow(t, id); }
        public void DeleteOp(TestCase tc) { var t = PickOrBegin(tc); if (PickRow(tc, t) is { } id) DeleteRow(t, id); }

        /// <summary>A change of the captured table's rows that is not logged for replication (a statement's internal work, LOB chunks): the
        /// miner never reads it, but it does read its undo.</summary>
        public void HiddenOp(TestCase tc)
        {
            var t = PickOrBegin(tc);
            var previous = t.Last;
            t.Last = Next();
            Append(new LogRecord { Lsn = t.Last, Operation = LogOperation.ModifyRow, Context = LogContext.Clustered, TransactionId = t.Xid, AllocUnitId = Au, PartitionId = Au });
            t.Ops.Add(new Op(LogOperation.ModifyRow, Au, 0, null, null, previous, Hidden: true));
        }
        public void SavepointOp(TestCase tc) { if (Pick(tc) is { } t) t.Savepoints.Push(t.Ops.Count); }
        public void RollbackToSavepointOp(TestCase tc) { if (Pick(tc) is { } t) Undo(t, t.Savepoints.Count > 0 ? t.Savepoints.Pop() : 0); }
        public void CommitOp(TestCase tc) { if (Pick(tc) is { } t) Commit(t); }
        public void AbortOp(TestCase tc) { if (Pick(tc) is { } t) Abort(t); }

        private void Insert(Txn t, bool untracked)
        {
            var id = _nextId++;
            var v = _nextValue++;
            var au = untracked ? UntrackedAu : Au;
            if (!untracked) { _rows[id] = (v, t.Xid); t.Replicated++; }
            t.Ops.Add(new Op(LogOperation.InsertRows, au, id, null, v, t.Last));
            t.Last = Next();
            Append(new LogRecord { Lsn = t.Last, Operation = LogOperation.InsertRows, Context = LogContext.Clustered, TransactionId = t.Xid, AllocUnitId = au, PartitionId = au,
                Flags = untracked ? LogRecordBits.None : LogRecordBits.Replicated, RowLog0 = Row(id, v) });
        }

        private int? PickRow(TestCase tc, Txn t)
        {
            var candidates = _rows.Where(r => r.Value.Value is not null && (r.Value.Lock is null || r.Value.Lock == t.Xid)).Select(r => r.Key).Order().ToList();
            return candidates.Count == 0 ? null : tc.Draw(Generators.SampledFrom(candidates), "row");
        }

        private void UpdateRow(Txn t, int id)
        {
            var before = _rows[id].Value!.Value;
            var after = _nextValue++;
            _rows[id] = (after, t.Xid);
            t.Replicated++;
            t.Ops.Add(new Op(LogOperation.ModifyRow, Au, id, before, after, t.Last));
            t.Last = Next();
            Append(new LogRecord { Lsn = t.Last, Operation = LogOperation.ModifyRow, Context = LogContext.Clustered, TransactionId = t.Xid, AllocUnitId = Au, PartitionId = Au,
                Flags = LogRecordBits.Replicated, OffsetInRow = 0, RowLog0 = Row(id, before), RowLog1 = Row(id, after) });
        }

        private void DeleteRow(Txn t, int id)
        {
            var before = _rows[id].Value!.Value;
            _rows[id] = (null, t.Xid);
            t.Replicated++;
            t.Ops.Add(new Op(LogOperation.DeleteRows, Au, id, before, null, t.Last));
            t.Last = Next();
            Append(new LogRecord { Lsn = t.Last, Operation = LogOperation.DeleteRows, Context = LogContext.MarkAsGhost, TransactionId = t.Xid, AllocUnitId = Au, PartitionId = Au,
                Flags = LogRecordBits.Replicated, RowLog0 = Row(id, before) });
        }

        /// <summary>SQL Server undoes backwards with one compensation record per undone change; each names the record before the undone one.</summary>
        private void Undo(Txn t, int keep)
        {
            for (var i = t.Ops.Count - 1; i >= keep; i--)
            {
                var op = t.Ops[i];
                op.UndoDdl?.Invoke();
                if (op.Au != UntrackedAu && !op.Hidden)
                    _rows[op.Id] = op.Kind switch { LogOperation.InsertRows => (null, t.Xid), _ => (op.Before, t.Xid) };
                var inverse = op.Kind switch { LogOperation.InsertRows => LogOperation.DeleteRows, LogOperation.DeleteRows => LogOperation.InsertRows, _ => LogOperation.ModifyRow };
                t.Last = Next();
                Append(new LogRecord { Lsn = t.Last, Operation = inverse, Context = inverse == LogOperation.DeleteRows ? LogContext.MarkAsGhost : LogContext.Clustered,
                    TransactionId = t.Xid, AllocUnitId = op.Au, PartitionId = op.Au, Flags = LogRecordBits.Compensation, UndoneAfter = op.Previous });
            }
            t.Ops.RemoveRange(keep, t.Ops.Count - keep);
            while (t.Savepoints.Count > 0 && t.Savepoints.Peek() > keep) t.Savepoints.Pop();
        }

        private void ReleaseLocks(Txn t)
        {
            foreach (var id in _rows.Where(r => r.Value.Lock == t.Xid).Select(r => r.Key).ToList())
            {
                if (_rows[id].Value is null) _rows.Remove(id); else _rows[id] = (_rows[id].Value, null);
            }
        }

        private void Commit(Txn t)
        {
            var tracked = t.Ops.Where(o => o.Au != UntrackedAu && !o.Hidden).ToList();
            var lsn = Next();
            Append(new LogRecord { Lsn = lsn, Operation = LogOperation.CommitXact, Context = LogContext.Null, TransactionId = t.Xid, TransactionBegin = t.Begin,
                Flags = t.HadTracked ? LogRecordBits.Replicated : LogRecordBits.None, EndTime = DateTime.UnixEpoch, ReplicatedRecords = t.Replicated });
            for (var i = 0; i < tracked.Count; i++)
            {
                var o = tracked[i];
                Expected.Add(new Expected(lsn, i, o.Kind switch { LogOperation.InsertRows => "c", LogOperation.DeleteRows => "d", _ => "u" }, o.Id, o.Before, o.After));
            }
            if (t.HadTracked) _replicatedCommits.Add((t.Begin, lsn));
            foreach (var o in tracked)
                if (o.Kind == LogOperation.DeleteRows) _committed.Remove(o.Id); else _committed[o.Id] = o.After!.Value;
            ReleaseLocks(t);
            _open.Remove(t.Xid);
        }

        private void Abort(Txn t)
        {
            Undo(t, 0);
            Append(new LogRecord { Lsn = Next(), Operation = LogOperation.AbortXact, Context = LogContext.Null, TransactionId = t.Xid, Flags = LogRecordBits.None });
            ReleaseLocks(t);
            _open.Remove(t.Xid);
        }

        /// <summary>Log backup: drop everything older than what active transactions and undistributed replicated transactions need.</summary>
        public void Truncate()
        {
            if (_log.Count == 0) return;
            var min = _log[^1].Lsn; // the active portion always holds the end of the log
            foreach (var t in _open.Values) if (t.Begin < min) min = t.Begin;
            foreach (var (begin, commit) in _replicatedCommits)
                if (commit > ReleasedThrough && begin < min && !(ForgetsLongTransactions && commit > _replicatedCommits.First(c => c.Commit > ReleasedThrough).Commit)) min = begin;
            _log.RemoveAll(r => r.Lsn < min);
            // before the stream's first durable position nothing is promised: what is gone by then was never in its range
            if (!HasPosition()) foreach (var (begin, commit) in _replicatedCommits) if (begin < min) GoneBeforeStart.Add(commit);
        }

        private static bool PassesServerFilter(LogRecord r) =>
            (r.Flags & (LogRecordBits.Replicated | LogRecordBits.Compensation)) != 0 || r.Operation is LogOperation.CommitXact or LogOperation.AbortXact;

        public async IAsyncEnumerable<LogRecord> ReadAsync(DatabaseRef database, Lsn? start, int maxRows, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct, bool settled = false)
        {
            await Task.Yield();
            var snapshot = _log.ToList();
            _blockClosed = true;
            if (snapshot.Count == 0) yield break;
            // like the engine source: behind a settled position the beginning of its log block may be missing
            if (settled && start is { } s && s < snapshot[0].Lsn && _logBlockOf[s] == _logBlockOf[snapshot[0].Lsn]) { Cover("settled-in-block"); start = null; }
            if (start is { } wanted && wanted < snapshot[0].Lsn) { Cover(_logBlockOf[wanted] == _logBlockOf[snapshot[0].Lsn] ? "refused-in-block" : "truncated-before-read"); throw new LogTruncatedException(wanted, snapshot[0].Lsn); }
            var from = start ?? snapshot[0].Lsn;
            if (!snapshot.Any(r => r.Lsn == from)) throw new InvalidOperationException($"error 9005: LSN {from} is not in the log");
            var n = 0;
            foreach (var r in snapshot)
            {
                if (r.Lsn < from || (r.Lsn != from && !PassesServerFilter(r))) continue;
                if (n++ >= maxRows) yield break;
                // the record a read starts at always comes, as a marker if it is nothing else (so the beginning of the log is known from the first row on)
                yield return PassesServerFilter(r) ? r : new LogRecord { Lsn = r.Lsn, Operation = LogOperation.Other, Context = LogContext.Null, TransactionId = r.TransactionId };
            }
        }

        /// <summary>The catalog as of now, and (like the engine source) layouts as of every LSN from <paramref name="from"/> on - and not before.</summary>
        public Task VerifyAsync(DatabaseRef database, CancellationToken ct) => Task.CompletedTask;

        /// <summary>Runs at the start of a catalog read (once), and may fail it.</summary>
        public Action? BeforeCatalogRead { get; set; }
        /// <summary>Until then no layout is known for rows written from here on.</summary>
        public Lsn? UndecodableFrom { get; set; }

        public async Task<DatabaseSchema> LoadSchemaAsync(DatabaseRef database, Lsn? from, IReadOnlySet<long> unresolvedRowsets, CancellationToken ct)
        {
            await Task.Yield();
            var before = BeforeCatalogRead;
            BeforeCatalogRead = null;
            before?.Invoke();
            var asOf = new Lsn(1, End().Block, 1); // like log_end_lsn: the start of the last block
            var versions = _versions.ToList();
            var covers = from ?? default;
            var changed = versions.Where(v => v.After > covers).ToList();
            var first = versions.Last(v => v.After <= covers);
            var boundaries = changed.Prepend((After: default(Lsn), first.Version)).ToList();
            if (UndecodableFrom is { } bad)
            {
                var known = boundaries.Select(b => new LayoutVersion(b.After, b.Version.Layout)).Where(v => v.After < bad).Append(new LayoutVersion(bad, (TableLayout?)null)).ToList();
                return new DatabaseSchema(database.Id, database.Name, [versions[^1].Version.Layout], asOf, history: new LayoutHistory(covers, versions.Select(v => v.Version.Rowset).Distinct().ToDictionary(r => r, _ => (IReadOnlyList<LayoutVersion>)known)));
            }
            var history = changed.Count == 0 ? [] : boundaries.Select(b => b.Version.Rowset).Distinct().ToDictionary(r => r,
                r => (IReadOnlyList<LayoutVersion>)boundaries.Select(b => new LayoutVersion(b.After, b.Version.Rowset == r ? b.Version.Layout : null)).ToList());
            DuringCatalogRead?.Invoke();
            return new DatabaseSchema(database.Id, database.Name, [versions[^1].Version.Layout], asOf, history: new LayoutHistory(covers, history));
        }

        /// <summary>An open transaction that changed the captured table; returns how to end it.</summary>
        public Action<bool> LongTransaction()
        {
            var t = BeginTxn();
            Insert(t, untracked: false);
            return commit => { if (_open.ContainsKey(t.Xid)) { if (commit) { Insert(t, untracked: false); Commit(t); } else Abort(t); } };
        }

        /// <summary>The engine keeps only what the bookmark's own transaction needs, not what later transactions need (worse than documented).</summary>
        public bool ForgetsLongTransactions { get; set; }

        public Lsn LastLsn => _log.Count == 0 ? default : _log[^1].Lsn;
        public IReadOnlyList<Lsn> Records => _log.Select(r => r.Lsn).ToList();
        public Lsn LastReplicatedCommitNot(Func<Lsn, bool> excluded) => _replicatedCommits.LastOrDefault(c => !excluded(c.Commit)).Commit;

        public Lsn LastReplicatedCommit => _replicatedCommits.Count == 0 ? default : _replicatedCommits[^1].Commit;
        public void NewTransaction(int rows, bool ddl)
        {
            var t = BeginTxn();
            for (var i = 0; i < rows; i++) Insert(t, untracked: false);
            if (ddl && CanAlter(t)) { Alter(t, rebuild: false); Insert(t, untracked: false); }
            Commit(t);
        }
        /// <summary>A replicated transaction of which nothing is left.</summary>
        public void UndoneTransaction()
        {
            var t = BeginTxn();
            Insert(t, untracked: false);
            Undo(t, 0);
            Commit(t);
        }

        public Func<bool> HasPosition { get; set; } = () => false;
        public HashSet<Lsn> GoneBeforeStart { get; } = [];

        private Lsn? OldestUnreleased()
        {
            Lsn? oldest = null;
            foreach (var (begin, commit) in _replicatedCommits) if (commit > ReleasedThrough && (oldest is null || begin < oldest)) oldest = begin;
            return oldest;
        }

        /// <summary>What the engine source does: is the held-back transaction still unreleased?</summary>
        public Task<(LogContinuity Continuity, string Detail)> CheckContinuityAsync(DatabaseRef database, Checkpoint position, CancellationToken ct)
        {
            if (!position.Held || position.LastBeginLsn.IsZero) return Task.FromResult((LogContinuity.Unknown, "not held"));
            var intact = OldestUnreleased() is { } oldest && oldest <= position.LastBeginLsn;
            if (intact) Cover("resumed-after-truncation");
            return Task.FromResult(intact ? (LogContinuity.Intact, "held") : (LogContinuity.Broken, "released by someone else"));
        }

        /// <summary>Somebody else marks everything as distributed (sp_repldone @reset = 1, a CDC capture job, CDC disabled).</summary>
        public void ForeignRelease()
        {
            if (_replicatedCommits.Count == 0) return;
            ReleasedThrough = _replicatedCommits[^1].Commit;
            ForeignReleases++;
        }

        public int ForeignReleases { get; private set; }

        /// <summary>Activity on tables that are not captured (moves the end of the log).</summary>
        public void UntrackedTransaction()
        {
            var t = BeginTxn();
            for (var i = 0; i < 12; i++) Insert(t, untracked: true);
            Commit(t);
        }

        public Lsn BeginOf(Lsn commit) => _replicatedCommits.First(c => c.Commit == commit).Begin;

        /// <summary>First sp_repldone call beyond the durable checkpoint (recorded, because the coordinator deliberately
        /// swallows release errors - an assertion thrown here would never reach the test).</summary>
        public string? ReleaseViolation { get; private set; }

        public Task ReleaseAsync(DatabaseRef database, Lsn beginLsn, Lsn commitLsn, CancellationToken ct)
        {
            var durable = _durableCommit();
            if (commitLsn > durable) ReleaseViolation ??= $"sp_repldone({commitLsn}) beyond the durable checkpoint {durable}";
            if (commitLsn > ReleasedThrough) ReleasedThrough = commitLsn;
            return Task.CompletedTask;
        }
    }

    private sealed class PipelineMachine : IDisposable
    {
        private readonly GatedCheckpointStore _store = new();
        private readonly FaultySink _remote = new();
        private readonly EventEncoder _encoder = new(new EncoderOptions { ServerName = "test" });
        private readonly CaptureSettings _settings;
        private readonly bool _useSpool;
        private readonly string _spoolDir = Path.Combine(Path.GetTempPath(), "lm-pipe-" + Guid.NewGuid().ToString("N"));
        private readonly SimulatedLog _db;
        private SpoolSink? _spool;
        private DatabaseCapture _capture;

        private readonly StartPosition _start;
        private int _snapshotCount;
        private string? _lastFault;

        private readonly int _finalFailures;

        // With a release interval (the engine's way) the clock is the model's; without one everything that is behind a durable position is released at once.
        private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);
        private readonly bool _delayed;
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch + TimeSpan.FromDays(1);

        public PipelineMachine(bool useSpool, int preexistingRows, int finalFailures = 0, int maxRows = 7, int maxBatch = 3, bool delayed = false)
        {
            _delayed = delayed && !useSpool;
            _settings = new() { StartPosition = StartPosition.Earliest, MaxRowsPerRead = maxRows, MaxBatchEvents = maxBatch };
            _finalFailures = finalFailures;
            _useSpool = useSpool;
            _db = new SimulatedLog(() => Sync(() => _store.LoadAsync("db", CancellationToken.None))?.LastCommitLsn ?? default);
            _db.HasPosition = () => _store.First is not null;
            for (var i = 0; i < preexistingRows; i++) _db.CommitNewInsert();   // history committed before capture starts
            _start = preexistingRows > 0 ? StartPosition.Latest : StartPosition.Earliest;
            if (_start == StartPosition.Latest) Cover("latest-start");
            _capture = NewCapture();
            if (useSpool) OpenSpool();
        }

        private ISink Sink => _useSpool ? _spool! : _remote;

        private DatabaseCapture NewCapture()
        {
            var position = Sync(() => _store.LoadAsync("db", CancellationToken.None));
            // what the engine does before anything is sent
            if (position?.Receipt is { } receipt && Sync(() => _remote.VerifyAsync(new Dictionary<string, string> { ["db"] = receipt }, CancellationToken.None).AsTask())["db"] == ReceiptState.Lost)
            {
                Cover("rewound-to-safe");
                position = position.Rewound() ?? throw new Xunit.Sdk.XunitException($"there is no position to go back to: {position}");
                // what it goes back to is still held by SQL Server, unless somebody else released it
                Assert.True(!position.Held || _db.ForeignReleases > 0 || _db.ReleasedThrough < position.LastCommitLsn, $"going back to {position.LastCommitLsn}, but {_db.ReleasedThrough} is released");
                _store.Rewinding = true;
                Sync(() => _store.SaveAsync([position], CancellationToken.None));
                _store.Rewinding = false;
            }
            return new(5, "db", "inc1", position, _start, _start == StartPosition.Latest ? _db.LogEnd : default);
        }

        /// <summary>Time passes: longer than the destination takes to have everything on disk.</summary>
        [Rule(Weight = 2)]
        public void TimePasses(TestCase tc)
        {
            _remote.Sync();
            _now += Interval;
        }

        /// <summary>Power failure of the host everything runs on: the destination comes back without (some of) what it has not synced, the miner with its
        /// durable position, which may be behind that.</summary>
        [Rule]
        public void PowerFailure(TestCase tc)
        {
            if (!_delayed) return;
            var flush = tc.Draw(Generators.Booleans(), "a checkpoint just before");
            if (flush) Flush();
            if (_remote.LoseTail(tc.Draw(Generators.Integers().Min(0).Max(12), "lost")) > 0) { Cover("power-failure-with-loss"); _powerFailures++; }
            Crash(tc);
        }

        private void OpenSpool()
        {
            _spool = new SpoolSink(_remote, new SpoolOptions
            {
                Directory = _spoolDir, SegmentBytes = 4096, ForwardBatchMessages = 3, LiveQueueMessages = 4,
                CursorFlushInterval = TimeSpan.FromMilliseconds(5), RetryMin = TimeSpan.FromMilliseconds(1), RetryMax = TimeSpan.FromMilliseconds(5),
            });
            Sync(() => _spool.StartAsync(CancellationToken.None));
        }

        [Rule(Weight = 2)] public void BeginTransaction(TestCase tc) => _db.Begin();
        [Rule(Weight = 5)] public void InsertRow(TestCase tc) => _db.InsertOp(tc);
        [Rule(Weight = 1)] public void UntrackedTableWrite(TestCase tc) => _db.UntrackedOp(tc);
        [Rule(Weight = 2)] public void UpdateRow(TestCase tc) => _db.UpdateOp(tc);
        [Rule(Weight = 1)] public void DeleteRow(TestCase tc) => _db.DeleteOp(tc);
        [Rule(Weight = 1)] public void Savepoint(TestCase tc) => _db.SavepointOp(tc);
        [Rule(Weight = 1)] public void RollbackToSavepoint(TestCase tc) => _db.RollbackToSavepointOp(tc);
        [Rule(Weight = 4)] public void CommitTransaction(TestCase tc) => _db.CommitOp(tc);
        [Rule(Weight = 1)] public void AbortTransaction(TestCase tc) => _db.AbortOp(tc);
        [Rule(Weight = 1)] public void HiddenChange(TestCase tc) { _db.HiddenOp(tc); Cover("hidden-change"); }

        [Rule(Weight = 2)]
        public void Ddl(TestCase tc) { if (_db.DdlOp(tc.Draw(Generators.Booleans(), "rebuild"))) Cover("ddl"); }

        [Rule(Weight = 1)]
        public void DdlInTransaction(TestCase tc)
        {
            var abort = tc.Draw(Generators.Booleans(), "abort");
            if (_db.DdlTransactionOp(tc.Draw(Generators.Booleans(), "rebuildFirst"), abort)) Cover(abort ? "ddl-rolled-back" : "ddl-in-transaction");
        }

        /// <summary>DDL (and row changes right after it) commits between the catalog read and its use, once.</summary>
        [Rule]
        public void ArmDdlDuringCatalogRead(TestCase tc)
        {
            var rebuild = tc.Draw(Generators.Booleans(), "rebuild");
            _db.DuringCatalogRead = () =>
            {
                _db.DuringCatalogRead = null;
                if (!_db.DdlOp(rebuild)) return;
                Cover("ddl-during-catalog-read");
                _db.CommitNewInsert();
            };
        }

        /// <summary>Preparing a committed transaction takes a catalog read. A checkpoint is taken at that very moment; then the read fails, or the
        /// process dies: the transaction is neither open nor published, and must not be lost between the two.</summary>
        [Rule(Weight = 2)]
        public void CheckpointWhilePreparing(TestCase tc)
        {
            var fails = tc.Draw(Generators.Booleans(), "catalogReadFails");
            var dies = tc.Draw(Generators.Booleans(), "processDies");
            if (!_db.DdlTransactionOp(tc.Draw(Generators.Booleans(), "rebuildFirst"), abort: false)) return;
            for (var i = 0; i < 200 && _capture.LastLsn < _db.LastReplicatedCommit; i++)
            {
                var preparing = false;
                _db.BeforeCatalogRead = () =>
                {
                    preparing = _capture.LastLsn >= _db.LastReplicatedCommit;
                    Flush();
                    Cover("checkpoint-while-preparing");
                    if (!preparing || !(fails || dies)) return;
                    Cover(dies ? "crash-while-preparing" : "fault-while-preparing");
                    throw new TimeoutException("simulated catalog lock");
                };
                var faultsBefore = _capture.ConsecutiveFaults;
                var more = PumpOnce();
                if (preparing && dies) { Flush(); Crash(tc); }
                if (preparing || (!more && _capture.ConsecutiveFaults == faultsBefore) || _capture.HoldingForBaseline) break;
            }
            _db.BeforeCatalogRead = null;
        }

        [Rule]
        public void NothingLeftOfATransaction(TestCase tc) { _db.UndoneTransaction(); Cover("nothing-left-of-it"); }

        public void UndecodableTransaction(TestCase tc)
        {
            for (var n = tc.Draw(Generators.Integers().Min(0).Max(3), "committedAndDelivered"); n > 0; n--) _db.CommitNewInsert();
            while (PumpOnce()) { }
            Flush();
            var good = tc.Draw(Generators.Integers().Min(0).Max(4), "committedBefore");
            for (var i = 0; i < good; i++) _db.NewTransaction(tc.Draw(Generators.Integers().Min(1).Max(4), "rows"), ddl: false);
            var lastGood = _db.Expected.Count == 0 ? default : _db.Expected.Max(e => e.CommitLsn);
            _db.UndecodableFrom = _db.LastLsn;
            _capture = NewCapture(); // the layouts are loaded again
            for (var n = tc.Draw(Generators.Integers().Min(1).Max(4), "undecodable"); n > 0; n--) _db.NewTransaction(tc.Draw(Generators.Integers().Min(1).Max(4), "rows"), ddl: tc.Draw(Generators.Booleans(), "ddl"));
            _expectUndecodable = true;
            for (var attempts = tc.Draw(Generators.Integers().Min(1).Max(8), "attempts"); attempts > 0; attempts--)
            {
                if (tc.Draw(Generators.Booleans(), "checkpointWhilePreparing")) _db.BeforeCatalogRead = Flush;
                PumpOnce();
                if (tc.Draw(Generators.Booleans(), "checkpoint")) Flush();
                if (tc.Draw(Generators.Integers().Min(0).Max(3), "crash") == 0) Crash(tc);
                var delivered = FirstDeliveries();
                Assert.True(delivered.All(d => d.Commit <= lastGood), "something behind the undecodable transaction was delivered");
                Assert.True(_store.Last is not { } cp || cp.LastCommitLsn <= lastGood, "the durable position passed the undecodable transaction");
            }
            for (var i = 0; i < 200 && FirstDeliveries().Count < _db.Expected.Count(e => e.CommitLsn <= lastGood); i++) PumpOnce();
            Assert.Equal(_db.Expected.Where(e => e.CommitLsn <= lastGood).Select(e => e.Id), FirstDeliveries().Select(d => d.Id)); // not withheld
            _db.UndecodableFrom = null;
            _db.BeforeCatalogRead = null;
            _expectUndecodable = false;
        }

        private bool _expectUndecodable;

        public void RestartTooLate(TestCase tc)
        {
            // one that was open when the position was saved: reading again from the last commit does not reach back to its beginning, only its commit
            // says what is missing
            var open = tc.Draw(Generators.Booleans(), "open at the checkpoint") ? _db.LongTransaction() : null;
            for (var n = tc.Draw(Generators.Integers().Min(1).Max(3), "delivered"); n > 0; n--) _db.CommitNewInsert();
            while (PumpOnce()) { }
            Flush();
            open?.Invoke(true);
            for (var n = tc.Draw(Generators.Integers().Min(1).Max(4), "transactions"); n > 0; n--) _db.NewTransaction(tc.Draw(Generators.Integers().Min(1).Max(5), "rows"), ddl: tc.Draw(Generators.Booleans(), "ddl"));
            var end = _db.LongTransaction();
            _db.CommitNewInsert();
            end(true);
            var cp = _store.Last!;
            var late = tc.Draw(Generators.SampledFrom(_db.Records.Where(l => l > cp.RestartLsn).ToList()), "restartAt");
            Sync(() => _store.SaveAsync([cp with { RestartLsn = late }], CancellationToken.None));
            _capture = NewCapture();
        }

        /// <summary>A pump lands while a checkpoint save is in flight: acknowledgements advance between the save and the release.</summary>
        [Rule]
        public void CheckpointWhilePumping(TestCase tc)
        {
            _db.CommitNewInsert();
            PumpOnce();                      // something acknowledged that is not checkpointed yet
            _store.Arm();
            var flush = Task.Run(FlushAsync);
            var entered = Sync(() => Task.WhenAny(_store.Entered, flush)) == _store.Entered;
            _db.CommitNewInsert();
            PumpOnce();                      // acknowledgements advance while the save is in flight
            _store.Open();
            try { Sync(() => flush); }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { }
            if (entered) Cover("checkpoint-while-pumping");
        }

        [Rule(Weight = 4)]
        public void Pump(TestCase tc) => PumpOnce();

        private bool PumpOnce()
        {
            Cover("pump");
            try { return Sync(() => _capture.PumpAsync(_db, _encoder, Sink, _settings, CancellationToken.None, _db, new GapRecorder(this))); }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                Cover("pump-fault:" + ex.GetType().Name);
                _lastFault = ex.Message;
                // every layout is derivable in this model: needing a rewind to find one is a defect even if the retry succeeds
                Assert.False(ex is CatalogHistoryException or UndecodableChangeException && !_expectUndecodable, $"{ex.GetType().Name}: {ex.Message}");
                _capture.SetFault(ex);
                return true;
            }
        }

        [Rule(Weight = 2)]
        public void Checkpoint(TestCase tc) => Flush();

        private Task FlushAsync() => CheckpointCoordinator.FlushAsync([_capture], _store, _db, _delayed ? Interval : TimeSpan.Zero, null, CancellationToken.None, _remote.ReceiptOf, _now);
        private void Flush() => Sync(FlushAsync);

        [Rule]
        public void LogBackup(TestCase tc) => _db.Truncate();

        /// <summary>Other tables are busy and the log is backed up while the miner does not read: its position may be truncated, harmlessly.</summary>
        [Rule]
        public void UntrackedActivityAndLogBackup(TestCase tc) { _db.UntrackedTransaction(); _db.Truncate(); }

        [Rule]
        public void ForeignRelease(TestCase tc)
        {
            if (tc.Draw(Generators.Integers().Min(0).Max(2), "rarely") != 0) return;
            if (tc.Draw(Generators.Booleans(), "afterMoreChanges")) _db.CommitNewInsert();
            _db.ForeignRelease();
            Cover("foreign-release");
            if (tc.Draw(Generators.Booleans(), "truncate")) { _db.UntrackedTransaction(); _db.Truncate(); }
        }

        /// <summary>The durable position lies at a long transaction that is rolled back while the miner is down; the log is then truncated up
        /// to the held-back transaction. The position is gone, but nothing is missing.</summary>
        [Rule]
        public void PositionTruncatedHarmlessly(TestCase tc)
        {
            var end = _db.LongTransaction();
            for (var i = 0; i < 2; i++) { _db.CommitNewInsert(); PumpOnce(); Flush(); }
            Crash(tc);
            end(tc.Draw(Generators.Booleans(), "longTransactionCommits"));
            _db.UntrackedTransaction();
            // also against an engine that would not keep the long transaction's beginning: it must be reported, never published in part
            _db.ForgetsLongTransactions = tc.Draw(Generators.Integers().Min(0).Max(3), "engineForgets") == 0;
            if (_db.ForgetsLongTransactions) Cover("engine-forgets");
            _db.Truncate();
            _db.ForgetsLongTransactions = false;
            if (tc.Draw(Generators.Booleans(), "moreChanges")) _db.CommitNewInsert();
        }

        /// <summary>The log comes to begin inside the block the position is in: nothing is missing when the block was read to its end, and captured
        /// changes may be when it was not. (Found by a review.)</summary>
        [Rule]
        public void LogStartInTheBlockOfThePosition(TestCase tc)
        {
            var sw = Stopwatch.StartNew();
            while (PumpOnce() && sw.Elapsed < TimeSpan.FromMilliseconds(200)) { }
            _db.CloseBlock();
            _db.CommitNewInsert();
            if (tc.Draw(Generators.Booleans(), "moreInTheBlock")) _db.CommitNewInsert();
            _db.Begin(); // seven records: one block, the last of them keeps the log
            for (var i = tc.Draw(Generators.Integers().Min(1).Max(3), "reads"); i > 0; i--) PumpOnce();
            Flush();
            if (tc.Draw(Generators.Booleans(), "crash")) Crash(tc);
            _db.ForeignRelease();
            _db.Truncate();
        }

        public void FaultWhileACheckpointIsSaved()
        {
            _db.CommitNewInsert();
            PumpOnce();
            Flush();
            _db.CommitNewInsert();
            PumpOnce();                      // acknowledged, not checkpointed yet
            _store.Arm();
            var flush = Task.Run(FlushAsync);
            Sync(() => _store.Entered);
            _remote.Script([SinkOutcome.FailWithoutStore]);
            _db.CommitNewInsert();
            PumpOnce();                      // faults and rewinds to the first checkpoint
            _store.Open();
            Sync(() => flush);               // the second one is durable
            Flush();
        }

        public int PhantomsFound { get; private set; }

        /// <summary>Two transactions and the beginning of a third in one block: the first is read, the second is not, and the third is what the log begins
        /// with from then on.</summary>
        public void GoneFromTheBlockOfThePosition()
        {
            _db.CommitNewInsert();
            var first = _db.LastReplicatedCommit;
            _db.CommitNewInsert();
            _db.Begin();
            PumpOnce();
            Flush();
            Assert.Equal(first, _capture.Durable!.RestartLsn); // the premise
            Assert.False(_capture.Durable.Settled);
            _db.ForeignRelease();
            _db.Truncate();
            _capture = NewCapture();
        }

        /// <summary>The power fails right after a look: what was looked at then is not stored yet.</summary>
        public void PowerFailsRightAfterALook(int looksBefore)
        {
            for (var i = 0; i < looksBefore; i++)
            {
                _db.CommitNewInsert();
                PumpOnce();
                _remote.Sync();
                _now += Interval;
                Flush();
            }
            _db.CommitNewInsert();
            PumpOnce();
            _now += Interval;
            Flush(); // saved, then looked at
            Assert.Equal(1, _remote.LoseTail(1));
            _powerFailures++;
            _capture = NewCapture();
        }

        /// <summary>The stored position is one of the version before receipts; then the destination loses what it got after the upgrade.</summary>
        public void LostAfterAnUpgrade()
        {
            for (var i = 0; i < 3; i++) _db.CommitNewInsert();
            PumpOnce();
            Flush();
            _remote.Sync();
            var old = Sync(() => _store.LoadAsync("db", CancellationToken.None))! with { Receipt = null, Safe = null, Settled = false };
            Sync(() => _store.SaveAsync([old], CancellationToken.None));
            _capture = NewCapture();
            for (var i = 0; i < 3; i++) _db.CommitNewInsert();
            PumpOnce();
            Flush();
            Assert.True(_capture.Durable!.LastCommitLsn > old.LastCommitLsn);
            Assert.Equal(old.LastCommitLsn, _capture.Durable.Safe!.CommitLsn);
            Assert.Equal(3, _remote.LoseTail(3));
            _powerFailures++;
            _capture = NewCapture();
        }

        /// <summary>Rows are deleted while the miner is down, and that part of the log is released and truncated by somebody else.</summary>
        public void DeletesGoneInAGap(int rows, int deleted)
        {
            for (var i = 0; i < rows; i++) _db.CommitNewInsert();
            var sw = Stopwatch.StartNew();
            while (PumpOnce() && sw.Elapsed < TimeSpan.FromSeconds(5)) { }
            Flush();
            for (var i = 0; i < deleted; i++) _db.MutateCommittedRow(delete: true);
            _db.ForeignRelease();
            _db.UntrackedTransaction();
            _db.Truncate();
            _db.CommitNewInsert();
            _capture = NewCapture();
        }

        /// <summary>Before anything was safe, somebody else releases the log and then the destination loses what it had acknowledged.</summary>
        public void LostAndTruncatedInTheFirstSeconds()
        {
            _db.CommitNewInsert();
            PumpOnce();
            Flush();
            Assert.NotNull(_capture.Durable);
            _db.ForeignRelease();
            _db.UntrackedTransaction();
            _db.Truncate();
            Assert.True(_remote.LoseTail(12) > 0);
            _powerFailures++;
            _capture = NewCapture();
        }

        public void TruncateBeforeTheFirstRead(bool releasedByOthers)
        {
            Flush(); // the start point is durable; nothing was read
            _db.CommitNewInsert();
            if (releasedByOthers) _db.ForeignRelease();
            _db.UntrackedTransaction();
            _db.Truncate();
            _db.CommitNewInsert();
        }

        public void ReleaseWaits(TimeSpan interval)
        {
            var durable = new List<(Lsn Commit, DateTime At)>();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var releases = 0;
            var last = default(Lsn);
            while (sw.Elapsed < 6 * interval)
            {
                _db.CommitNewInsert();
                PumpOnce();
                Sync(() => CheckpointCoordinator.SaveAsync([_capture], _store, CancellationToken.None));
                durable.Add((_capture.Durable!.LastCommitLsn, DateTime.UtcNow));
                Sync(() => CheckpointCoordinator.ReleaseAsync([_capture], _db, interval, null, CancellationToken.None));
                if (_db.ReleasedThrough != last)
                {
                    last = _db.ReleasedThrough;
                    releases++;
                    var age = DateTime.UtcNow - durable.First(d => d.Commit >= last).At;
                    Assert.True(age >= interval, $"released {age.TotalMilliseconds:F0} ms after its checkpoint");
                }
                Thread.Sleep(5);
            }
            Assert.True(releases >= 3, $"{releases} releases"); // it does not wait for ever either, while changes keep coming
            var newest = _capture.Releasable!.Value.Commit;
            for (var i = 0; i < 4; i++)
            {
                Thread.Sleep(interval + TimeSpan.FromMilliseconds(20));
                Sync(() => CheckpointCoordinator.SaveAsync([_capture], _store, CancellationToken.None));
                Sync(() => CheckpointCoordinator.ReleaseAsync([_capture], _db, interval, null, CancellationToken.None));
            }
            Assert.Equal(newest, _db.ReleasedThrough); // nor when none come any more
        }

        public void RestartBehindALongTransaction(int pumps)
        {
            _ = _db.LongTransaction(); // stays open: the restart point
            for (var i = 0; i < 12; i++) _db.CommitNewInsert();
            while (PumpOnce()) { }
            Flush();
            _capture = NewCapture();
            for (var i = 0; i < pumps; i++) { PumpOnce(); Flush(); }
        }

        public void CutATransactionAndRestart(int pumpsBeforeRestart)
        {
            var end = _db.LongTransaction();
            for (var i = 0; i < 2; i++) { _db.CommitNewInsert(); PumpOnce(); Flush(); }
            _capture = NewCapture();
            for (var i = 0; i < 12; i++) _db.CommitNewInsert(); // more than the pumps below read
            end(true);
            _db.ForgetsLongTransactions = true;
            _db.Truncate();
            _db.ForgetsLongTransactions = false;
            for (var i = 0; i < pumpsBeforeRestart; i++) PumpOnce(); // the first one notices the truncation
            Flush();
            _capture = NewCapture();
        }

        public int Gaps => _gaps.Count;
        private readonly List<CaptureGap> _gaps = [];
        private int _powerFailures;

        private sealed class GapRecorder(PipelineMachine m) : IGapLog
        {
            public Task RecordAsync(CaptureGap gap, bool resync, CancellationToken ct)
            {
                Cover("gap");
                if (gap.Continuity == LogContinuity.Broken) Assert.True(m._db.ForeignReleases > 0, $"false alarm: {gap}");
                m._gaps.Add(gap);
                return Task.CompletedTask;
            }
        }

        /// <summary>A change whose transaction lost records to a recorded gap: it may be missing (and nothing is said about it).</summary>
        private bool InGap(Lsn commit) => _gaps.Any(g => commit > g.AfterCommit && _db.BeginOf(commit) < g.ResumedAt);

        /// <summary>Request a historical load; commits may land while its chunks are read.</summary>
        [Rule]
        public void RequestSnapshot(TestCase tc)
        {
            var chunk = tc.Draw(Generators.Integers().Min(1).Max(4), "chunkSize");
            ArmChunkReadNoise(tc);
            if (!_capture.Live) return;
            _capture.RequestSnapshot(NewSnapshot($"s{++_snapshotCount}", chunk));
            Cover("snapshot-requested");
        }

        private static TableSnapshot NewSnapshot(string id, int chunkSize) =>
            new(SimulatedLog.Layout, id, chunkSize) { RetryDelay = TimeSpan.FromMilliseconds(1), MaxConsecutiveFailures = 4 };

        /// <summary>Chunk reads fail for a while (fewer times in a row than the load tolerates).</summary>
        [Rule]
        public void ChunkReadsFail(TestCase tc)
        {
            _db.FailChunkReads = tc.Draw(Generators.Integers().Min(1).Max(3), "chunkReadFailures");
            Cover("chunk-read-fails");
        }

        /// <summary>Choose what commits while chunks are read (drawn here: the hook runs later, on the pump's thread).</summary>
        [Rule]
        public void ArmChunkReadNoise(TestCase tc)
        {
            var insert = tc.Draw(Generators.Booleans(), "insertDuringChunkRead");
            var mutate = tc.Draw(Generators.Booleans(), "mutateChunkRowDuringChunkRead");
            var delete = tc.Draw(Generators.Booleans(), "deleteInsteadOfUpdate");
            _db.DuringChunkRead = !insert && !mutate ? null : after =>
            {
                if (mutate) _db.MutateCommittedRow(delete, after);
                if (insert) _db.CommitNewInsert();
            };
        }

        [Rule]
        public void RemoteFails(TestCase tc)
        {
            Cover(_useSpool ? "remote-fails(spool)" : "remote-fails(direct)");
            var n = tc.Draw(Generators.Integers().Min(1).Max(3), "failures");
            for (var i = 0; i < n; i++)
                _remote.Script([tc.Draw(Generators.SampledFrom(new[] { SinkOutcome.FailAfterStore, SinkOutcome.FailWithoutStore, SinkOutcome.FailHalfStored, SinkOutcome.Throw }), "outcome")]);
        }

        [Rule]
        public void Crash(TestCase tc)
        {
            Cover("crash");
            if (_db.OpenCount > 0) Cover("crash-with-open-txn");
            if (_db.LastDdl > _capture.AcknowledgedCommit) Cover("crash-behind-ddl");
            if (_capture.OpenTransactions > 0) Cover("crash-with-buffered-txn");
            if (Sync(() => _store.LoadAsync("db", CancellationToken.None)) is not null) Cover("crash-with-checkpoint");
            if (_useSpool)
            {
                var (path, durable, current) = Sync(() => _spool!.SimulateCrashAsync());
                if (path is not null && File.Exists(path) && tc.Draw(Generators.Booleans(), "appendGarbage"))
                {
                    using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
                    fs.Seek(0, SeekOrigin.End);
                    fs.Write(tc.Draw(Generators.Binary().MinSize(1).MaxSize(30), "garbage"));
                }
                OpenSpool();
            }
            _remote.NewProducer();
            _capture = NewCapture(); // everything in memory is gone; resume from the durable checkpoint
        }

        [Rule]
        public void SettleNow(TestCase tc) => Settle();

        [Invariant]
        public void Delivered_events_are_committed_changes_in_commit_order(TestCase tc) => CheckDelivered(complete: false);

        [Invariant]
        public void Log_is_never_released_beyond_the_durable_checkpoint(TestCase tc) => Assert.Null(_db.ReleaseViolation);

        [Invariant]
        public void The_durable_position_never_moves_back(TestCase tc) => Assert.Null(_store.Regression);

        private List<(Lsn Commit, int Seq, string Op, int Id, int? Before, int? After)> FirstDeliveries()
        {
            var seen = new HashSet<string>();
            var result = new List<(Lsn, int, string, int, int?, int?)>();
            foreach (var m in _remote.Stored)
            {
                if (!seen.Add(m.MessageId)) continue;
                using var doc = JsonDocument.Parse(m.Payload);
                var root = doc.RootElement;
                if (root.GetProperty("op").GetString() is "r" or "l") continue; // snapshot rows are checked by the replica comparison
                var src = root.GetProperty("source");
                int? V(string image) => root.GetProperty(image).ValueKind == JsonValueKind.Null ? null : root.GetProperty(image).GetProperty("v").GetInt32();
                var id = (root.GetProperty("after").ValueKind != JsonValueKind.Null ? root.GetProperty("after") : root.GetProperty("before")).GetProperty("id").GetInt32();
                result.Add((Lsn.Parse(src.GetProperty("commit_lsn").GetString()!), src.GetProperty("seq").GetInt32(), root.GetProperty("op").GetString()!, id, V("before"), V("after")));
            }
            return result;
        }

        private void CheckDelivered(bool complete)
        {
            var delivered = FirstDeliveries();
            var expected = _db.Expected.ToDictionary(e => (e.CommitLsn, e.Seq));
            (Lsn, int)? prev = null;
            foreach (var d in delivered)
            {
                if (_gaps.Count > 0 && InGap(d.Commit)) continue; // delivered before its transaction was cut by a gap
                Assert.True(expected.TryGetValue((d.Commit, d.Seq), out var e), $"delivered an event that was never committed: {d}");
                Assert.Equal((e.Op, e.Id, e.Before, e.After), (d.Op, d.Id, d.Before, d.After));
                if (prev is { } p) Assert.True(p.Item1 < d.Commit || (p.Item1 == d.Commit && p.Item2 < d.Seq), $"out of order: {d.Commit}/{d.Seq} after {p}");
                prev = (d.Commit, d.Seq);
            }
            if (complete)
            {
                // Earliest: everything. Latest: everything after the first durable position, and no gap once delivery started.
                bool Required(Lsn commit) => _start == StartPosition.Earliest
                    || (_store.First is { } f && commit > f.LastCommitLsn)
                    || (delivered.Count > 0 && commit >= delivered[0].Commit);
                var missing = _db.Expected.Where(e => Required(e.CommitLsn) && !InGap(e.CommitLsn) && !_db.GoneBeforeStart.Contains(e.CommitLsn) && !delivered.Any(d => d.Commit == e.CommitLsn && d.Seq == e.Seq)).ToList();
                Assert.True(missing.Count == 0, $"{missing.Count} committed change(s) never delivered, first: {missing.FirstOrDefault()} (last fault: {_lastFault})");
            }
        }

        /// <summary>Heal the remote and let the pipeline catch up; then every committed change must have been delivered.</summary>
        public void Settle()
        {
            Cover("settle");
            Cover(_db.Expected.Count == 0 ? "settle-with-0-events" : _db.Expected.Count < 5 ? "settle-with-1-4-events" : "settle-with-5+-events");
            _db.DuringChunkRead = null; // quiesce: no new commits while waiting for everything to be delivered
            _db.DuringCatalogRead = null;
            _remote.Heal();
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(10))
            {
                var more = PumpOnce();
                Flush();
                var spoolDrained = !_useSpool || _spool!.DeliveredSeq >= _spool.LastSeq;
                if (!more && _capture.State != CaptureState.Faulted && spoolDrained && _capture.AcknowledgedCommit >= LastCommittedLsn()) break;
                if (!more) Thread.Sleep(1);
            }
            CheckDelivered(complete: true);
            if (_gaps.Count > 0 || _start == StartPosition.Latest || _snapshotCount > 0) CheckReplica();
        }

        /// <summary>
        /// Applying every delivered event once (stream changes and snapshot rows, in first-delivery order) must reproduce the
        /// committed table: nothing missing (a gap in the historical load) and nothing stale (a snapshot row overwriting a newer change).
        /// A load's markers say which rows should be there: at the end of a load that arrived whole, a row that was not mentioned since its beginning
        /// does not exist. Without a gap there is none; behind one, the deletes that were never conveyed are found that way.
        /// </summary>
        private void CheckReplica()
        {
            // make sure one complete historical load ran after the start point (needed when history predates the capture, or was released and
            // truncated by someone else before there was a position to notice it by; a load requested earlier may have died in a crash)
            if (_start == StartPosition.Latest || _db.GoneBeforeStart.Count > 0 || _gaps.Count > 0)
            {
                _db.DuringChunkRead = null;
                var s = NewSnapshot($"final{++_snapshotCount}", 3);
                var sw = Stopwatch.StartNew();
                while (!_capture.Live && sw.Elapsed < TimeSpan.FromSeconds(3)) PumpOnce();
                _capture.RequestSnapshot(s);
                _remote.Script(Enumerable.Repeat(SinkOutcome.FailWithoutStore, _finalFailures)); // chunks are lost in flight
                while (!s.Done && sw.Elapsed < TimeSpan.FromSeconds(30)) { var more = PumpOnce(); Flush(); if (!more) Thread.Sleep(1); } // a sleep takes 16 ms
                Assert.True(s.Done, $"historical load did not finish: live {_capture.Live}, {_capture.State} {_capture.Fault}, faults {_capture.ConsecutiveFaults}, queued {_capture.QueuedSnapshots}, active {_capture.ActiveSnapshot is not null}, holding {_capture.HoldingForBaseline}, last fault {_lastFault}");
                Assert.False(s.Failed, $"historical load failed: {s.Error}");
                Cover("snapshot-finished");
                while (sw.Elapsed < TimeSpan.FromSeconds(35) && (_useSpool && _spool!.DeliveredSeq < _spool.LastSeq || _remote.Pending > 0)) Thread.Sleep(2);
            }
            var replica = new SortedDictionary<int, int>();
            var seen = new HashSet<string>();
            var loads = new Dictionary<string, (HashSet<int> Mentioned, HashSet<int> Loaded)>();
            var ended = new HashSet<string>();
            var lossless = _gaps.Count == 0 && _powerFailures == 0;
            foreach (var m in _remote.Stored)
            {
                if (!seen.Add(m.MessageId)) continue;
                using var doc = JsonDocument.Parse(m.Payload);
                var root = doc.RootElement;
                var op = root.GetProperty("op").GetString();
                var load = root.GetProperty("source").TryGetProperty("snapshot", out var x) ? x.GetString() : null;
                if (op == "l")
                {
                    var marker = root.GetProperty("load");
                    Assert.Equal(load, marker.GetProperty("id").GetString());
                    if (marker.GetProperty("phase").GetString() == "begin") { loads[load!] = ([], []); continue; }
                    if (!loads.Remove(load!, out var whole)) { Assert.True(!lossless || ended.Contains(load!), $"load {load} ends without having begun"); continue; }
                    ended.Add(load!);
                    if (marker.GetProperty("rows").GetInt64() > whole.Loaded.Count) { Assert.False(lossless, $"load {load}: {marker.GetProperty("rows")} rows were loaded, {whole.Loaded.Count} arrived"); Cover("load-arrived-in-part"); continue; }
                    var gone = replica.Keys.Where(k => !whole.Mentioned.Contains(k)).ToList();
                    Assert.True(!lossless || gone.Count == 0, $"load {load} mentions rows {string.Join(",", gone)} neither as loaded nor as changed");
                    PhantomsFound += gone.Count;
                    foreach (var k in gone) replica.Remove(k);
                    Cover("load-arrived-whole");
                    continue;
                }
                var image = root.GetProperty(op == "d" ? "before" : "after");
                var id = image.GetProperty("id").GetInt32();
                if (op == "d") { replica.Remove(id); continue; }
                replica[id] = image.GetProperty("v").GetInt32();
                foreach (var (_, l) in loads) l.Mentioned.Add(id);
                if (op != "r") continue;
                // behind the end: a chunk that was stored, not acknowledged and read again
                Assert.True(!lossless || loads.ContainsKey(load!) || ended.Contains(load!), $"a row of load {load} before its beginning");
                if (loads.TryGetValue(load!, out var its)) its.Loaded.Add(id);
            }
            Assert.Equal(string.Join(",", _db.Committed.Select(kv => $"{kv.Key}={kv.Value}")), string.Join(",", replica.Select(kv => $"{kv.Key}={kv.Value}")));
        }

        /// <summary>Also of transactions of which nothing is left: what is not acknowledged is not released either, and holds log for ever.</summary>
        private Lsn LastCommittedLsn() => _db.LastReplicatedCommitNot(InGap);

        public void Dispose()
        {
            _remote.Shutdown();
            if (_spool is not null) try { Sync(async () => await _spool.DisposeAsync()); } catch (InvalidOperationException) { }
            try { if (Directory.Exists(_spoolDir)) Directory.Delete(_spoolDir, recursive: true); } catch (IOException) { }
        }
    }
}
