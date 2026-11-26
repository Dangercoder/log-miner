using Hegel;
using Hegel.Xunit;
using LogMiner.Log;
using LogMiner.Transactions;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>
/// Stateful model test: random interleavings of transactions (inserts/updates/deletes on tracked and untracked
/// allocation units, savepoints, partial rollbacks, aborts, commits) are turned into the log records SQL Server
/// would write, including compensation records undone strictly backwards. The assembler must emit exactly the
/// committed, non-undone tracked changes, in commit order, and report the right restart LSN.
/// </summary>
public class TransactionAssemblerProperties
{
    private const long TrackedA = 101, TrackedB = 102, Untracked = 999;

    [HegelTest(TestCases = 300)]
    public void Emits_exactly_the_committed_effects(TestCase tc) => Stateful.Run(new Machine(), tc);

    /// <summary>Whatever is replicated is counted, and is a change, something known to repeat one, or a record that stops the stream if its rowset is
    /// captured. Nothing replicated disappears.</summary>
    [HegelTest(TestCases = 1000)]
    public void No_replicated_record_is_passed_over(TestCase tc)
    {
        var assembler = new TransactionAssembler(_ => true);
        var committed = new List<CommittedTransaction>();
        var n = tc.Draw(Generators.Integers().Min(0).Max(8), "records");
        var replicated = 0;
        var expected = new List<ChangeKind>();
        for (var i = 1; i <= n; i++)
        {
            var r = new LogRecord
            {
                Lsn = new Lsn(1, 1, (ushort)i), TransactionId = 7, AllocUnitId = 1, PartitionId = 1,
                Operation = tc.Draw(Generators.SampledFrom(Enum.GetValues<LogOperation>().Except([LogOperation.CommitXact, LogOperation.AbortXact]).ToArray()), "operation"),
                Context = tc.Draw(Generators.SampledFrom(Enum.GetValues<LogContext>()), "context"),
                Flags = tc.Draw(Generators.Booleans(), "replicated") ? LogRecordBits.Replicated : LogRecordBits.None,
                RowLog0 = tc.Draw(Generators.Booleans(), "image") ? [1] : null, RowLog1 = [2],
            };
            assembler.Process(r, committed);
            if (!r.IsReplicated) continue;
            replicated++;
            var row = r.Context is LogContext.Heap or LogContext.Clustered or LogContext.MarkAsGhost;
            var lob = r.Context is LogContext.TextMix or LogContext.TextTree;
            if (r.Operation is LogOperation.ReplCommand or LogOperation.ReplicationInfo or LogOperation.BeginXact) continue;
            if (row && r.Operation is LogOperation.InsertRows && r.RowLog0 is not null) expected.Add(ChangeKind.Insert);
            else if (row && r.Operation is LogOperation.DeleteRows && r.RowLog0 is not null) expected.Add(ChangeKind.Delete);
            else if (row && r.Operation is LogOperation.ModifyRow && r.RowLog0 is not null) expected.Add(ChangeKind.Update);
            else if (row && r.Operation is LogOperation.ModifyRow or LogOperation.ModifyColumns && row) expected.Add(ChangeKind.PartialUpdate);
            else if (!lob) expected.Add(ChangeKind.Unknown);
        }
        var logged = replicated + tc.Draw(Generators.Integers().Min(0).Max(1), "notReceived");
        assembler.Process(new LogRecord { Lsn = new Lsn(1, 2, 1), Operation = LogOperation.CommitXact, Context = LogContext.Null, TransactionId = 7, TransactionBegin = new Lsn(1, 1, 0), Flags = logged > 0 ? LogRecordBits.Replicated : LogRecordBits.None, ReplicatedRecords = logged }, committed);
        if (logged == 0) return;
        var t = Assert.Single(committed);
        Assert.Equal((replicated, logged, replicated == logged), (t.RecordsSeen, t.RecordsLogged, t.Complete));
        Assert.Equal(expected, t.Changes.Where(c => c.Kind != ChangeKind.PartialUpdate || expected.Contains(ChangeKind.PartialUpdate)).Select(c => c.Kind).Where((k, i) => true).ToList() is var got && got.Count == expected.Count ? got : Kinds(t, expected));
    }

    // records that are not replicated may be changes, too (tables without CDC): only the replicated ones are compared
    private static List<ChangeKind> Kinds(CommittedTransaction t, List<ChangeKind> expected)
    {
        var kinds = t.Changes.Select(c => c.Kind).ToList();
        var at = 0;
        foreach (var k in kinds) if (at < expected.Count && expected[at] == k) at++;
        return at == expected.Count ? expected : kinds;
    }

    private sealed class Machine
    {
        /// <summary>Hidden: a row change that is not logged for replication (DDL rewriting rows, LOB work) - the miner never sees it, only its undo.</summary>
        private sealed record Op(LogOperation Kind, long AllocUnit, byte[] Payload, Lsn Lsn, Lsn Previous, bool Hidden);
        private sealed class Txn
        {
            public required ulong Xid;
            public required Lsn Begin;
            /// <summary>The transaction's newest record (head of its backward chain).</summary>
            public required Lsn Last;
            public readonly List<Op> Ops = [];
            public readonly Stack<int> Savepoints = new();
            public int Replicated;
        }

        private readonly TransactionAssembler _assembler = new(au => au is TrackedA or TrackedB);
        private readonly List<CommittedTransaction> _emitted = [];
        private readonly List<(ulong Xid, Lsn Commit, List<Op> Ops)> _expected = [];
        private readonly Dictionary<ulong, Txn> _open = [];
        private uint _block = 1;
        private ushort _slot;
        private ulong _nextXid = 0x1000;
        private int _payload;

        private Lsn NextLsn()
        {
            if (++_slot == 50) { _block++; _slot = 1; }
            return new Lsn(0x2A, _block, _slot);
        }

        private void Feed(LogRecord r) => _assembler.Process(r, _emitted);

        private Txn PickOrBegin(TestCase tc)
        {
            if (_open.Count > 0 && tc.Draw(Generators.Booleans(), "existing"))
            {
                var xid = tc.Draw(Generators.SampledFrom(_open.Keys.OrderBy(k => k).ToList()), "xid");
                return _open[xid];
            }
            var begin = NextLsn();
            var t = new Txn { Xid = _nextXid++, Begin = begin, Last = begin };
            Feed(new LogRecord { Lsn = t.Begin, Operation = LogOperation.BeginXact, Context = LogContext.Null, TransactionId = t.Xid });
            _open[t.Xid] = t;
            return t;
        }

        [Rule(Weight = 4)]
        public void Change(TestCase tc)
        {
            var t = PickOrBegin(tc);
            var kind = tc.Draw(Generators.SampledFrom(new[] { LogOperation.InsertRows, LogOperation.ModifyRow, LogOperation.DeleteRows }), "kind");
            var au = tc.Draw(Generators.SampledFrom(new[] { TrackedA, TrackedB, Untracked }), "allocUnit");
            var payload = BitConverter.GetBytes(++_payload);
            var lsn = NextLsn();
            var hidden = au != Untracked && tc.Draw(Generators.Integers().Min(0).Max(5), "hidden") == 0;
            t.Ops.Add(new Op(kind, au, payload, lsn, t.Last, hidden));
            t.Last = lsn;
            if (hidden) return;
            if (au != Untracked) { _firstTracked.TryAdd(t.Xid, lsn); t.Replicated++; }
            Feed(new LogRecord
            {
                Lsn = lsn, Operation = kind, TransactionId = t.Xid, AllocUnitId = au, PartitionId = au,
                Context = kind == LogOperation.DeleteRows ? LogContext.MarkAsGhost : LogContext.Clustered,
                Flags = au == Untracked ? LogRecordBits.None : LogRecordBits.Replicated,
                RowLog0 = payload, RowLog1 = kind == LogOperation.ModifyRow ? payload : null,
            });
        }

        [Rule]
        public void Savepoint(TestCase tc)
        {
            var t = PickOrBegin(tc);
            t.Savepoints.Push(t.Ops.Count);
        }

        [Rule]
        public void RollbackToSavepoint(TestCase tc)
        {
            var t = PickOrBegin(tc);
            var keep = t.Savepoints.Count > 0 ? t.Savepoints.Pop() : 0;
            Undo(t, keep);
        }

        [Rule]
        public void Commit(TestCase tc)
        {
            var t = PickOrBegin(tc);
            var lsn = NextLsn();
            Feed(new LogRecord { Lsn = lsn, Operation = LogOperation.CommitXact, Context = LogContext.Null, TransactionId = t.Xid, TransactionBegin = t.Begin, Flags = t.Replicated > 0 ? LogRecordBits.Replicated : LogRecordBits.None, ReplicatedRecords = t.Replicated });
            _open.Remove(t.Xid);
            _firstTracked.Remove(t.Xid);
            var tracked = t.Ops.Where(o => o.AllocUnit != Untracked && !o.Hidden).ToList();
            if (t.Replicated > 0) _expected.Add((t.Xid, lsn, tracked)); // also when nothing is left of it: it holds log
        }

        [Rule]
        public void Abort(TestCase tc)
        {
            var t = PickOrBegin(tc);
            Undo(t, 0);
            Feed(new LogRecord { Lsn = NextLsn(), Operation = LogOperation.AbortXact, Context = LogContext.Null, TransactionId = t.Xid });
            _open.Remove(t.Xid);
            _firstTracked.Remove(t.Xid);
        }

        /// <summary>SQL Server undoes backwards, writing one compensation record per undone row change; each names the record before the
        /// one it undoes.</summary>
        private void Undo(Txn t, int keep)
        {
            for (var i = t.Ops.Count - 1; i >= keep; i--)
            {
                var op = t.Ops[i];
                var inverse = op.Kind switch { LogOperation.InsertRows => LogOperation.DeleteRows, LogOperation.DeleteRows => LogOperation.InsertRows, _ => LogOperation.ModifyRow };
                t.Last = NextLsn();
                Feed(new LogRecord
                {
                    Lsn = t.Last, Operation = inverse, TransactionId = t.Xid, AllocUnitId = op.AllocUnit, PartitionId = op.AllocUnit,
                    Context = inverse == LogOperation.DeleteRows ? LogContext.MarkAsGhost : LogContext.Clustered,
                    Flags = LogRecordBits.Compensation, UndoneAfter = op.Previous,
                });
            }
            t.Ops.RemoveRange(keep, t.Ops.Count - keep);
            while (t.Savepoints.Count > 0 && t.Savepoints.Peek() > keep) t.Savepoints.Pop();
        }

        [Invariant]
        public void Output_matches_model(TestCase tc)
        {
            Assert.Equal(_expected.Count, _emitted.Count);
            for (var i = 0; i < _expected.Count; i++)
            {
                var (xid, commit, ops) = _expected[i];
                var got = _emitted[i];
                Assert.Equal(xid, got.TransactionId);
                Assert.Equal(commit, got.CommitLsn);
                Assert.True(got.Complete);
                Assert.Equal(ops.Select(o => o.Lsn), got.Changes.Select(c => c.Lsn));
                Assert.Equal(ops.Select(o => Convert.ToHexString(o.Payload)), got.Changes.Select(c => Convert.ToHexString(c.After ?? c.Before!)));
            }
        }

        [Invariant]
        public void Restart_lsn_is_the_oldest_open_tracked_change(TestCase tc)
        {
            // The assembler opens a buffer at the first tracked record of a transaction and keeps it until commit/abort,
            // even if a partial rollback later cancels every change (conservative, never too late).
            Lsn? oldest = _firstTracked.Count == 0 ? null : _firstTracked.Values.Min();
            Assert.Equal(oldest, _assembler.RestartLsn);
        }

        private readonly Dictionary<ulong, Lsn> _firstTracked = [];

    }
}
