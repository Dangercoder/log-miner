using System.Collections.Concurrent;
using System.Text;
using Hegel;
using Hegel.Xunit;
using Xunit;

namespace LogMiner.Verify.Tests;

/// <summary>
/// The verifier against streams of simulated tables (<see cref="Sim"/>): a correct stream never differs, however it is replayed, repeated,
/// interleaved and cut at its head; a stream with one message dropped, altered or out of place differs whenever anybody could see it
/// (<see cref="Oracle"/>), at the latest when the next complete load ends; and after that the same stream gives nothing more.
/// </summary>
public class StreamVerifierProperties
{
    /// <summary>How often the interesting situations occur in generated histories (diagnostics, and <see cref="The_generator_reaches_the_interesting_histories"/>).</summary>
    public static readonly ConcurrentDictionary<string, int> Coverage = new();
    private static void Cover(string what, int n = 1) => Coverage.AddOrUpdate(what, n, (_, old) => old + n);
    private static void Cover(Sim sim) { foreach (var s in sim.Seen) Cover(s.Key, s.Value); }

    internal static Tape DrawTape(TestCase tc, int max, string label)
    {
        var size = tc.Draw(Generators.Integers().Min(0).Max(max), label + ".size");
        return new Tape(tc.Draw(Generators.Lists(Generators.Integers().Min(0).Max(999_999)).MinSize(size).MaxSize(size), label));
    }

    private static void Feed(StreamVerifier v, IReadOnlyList<Ev> stream, int from, int to)
    {
        for (var i = from; i < to; i++) v.Add(stream[i].Subject, [new KeyValuePair<string, string>("Nats-Msg-Id", stream[i].Id)], stream[i].Payload, (ulong)i + 1);
    }

    private static string Explain(StreamVerifier v, IReadOnlyList<Ev> stream, int from = 0) =>
        v.Report().ToText() + "\nstream:\n" + string.Join("\n", stream.Skip(from).Select((e, i) => $"{from + i + 1}: {e}"));

    /// <summary>The state the verifier holds is the table: the same keys, and every value it knows.</summary>
    private static void AssertStateIsTheTable(StreamVerifier v, Sim sim, IReadOnlyList<Ev> stream)
    {
        foreach (var t in sim.Dbs.SelectMany(d => d.Tables).Where(t => t.Keyed))
        {
            var state = v.Dump(t.Db.Server, t.Db.Name, t.Schema, t.Name);
            var table = t.Rows.Values.ToDictionary(r => StreamVerifier.KeyText(Encoding.UTF8.GetBytes("{" + string.Join(",", t.KeyCols.Select(k => Sim.Quote(k) + ":" + r[k])) + "}")), r => r, StringComparer.Ordinal);
            Assert.True(state.Keys.Order(StringComparer.Ordinal).SequenceEqual(table.Keys.Order(StringComparer.Ordinal)),
                $"{t.Db.Name}/{t.Name}: the state has [{string.Join(" ", state.Keys.Order(StringComparer.Ordinal))}], the table [{string.Join(" ", table.Keys.Order(StringComparer.Ordinal))}]\n{Explain(v, stream)}");
            foreach (var (key, cells) in state)
                foreach (var cell in cells.Split(';', StringSplitOptions.RemoveEmptyEntries))
                {
                    var (column, has) = (cell[..cell.IndexOf('=')], cell[(cell.IndexOf('=') + 1)..]);
                    if (has == "(not known)" || !table[key].TryGetValue(column, out var json)) continue; // unavailable when last seen; dropped since
                    var should = StreamVerifier.CellText(Encoding.UTF8.GetBytes(json));
                    // a column that was retyped keeps its old kind in rows nothing was published about since
                    static char Kind(string text) => text.StartsWith('"') || text.StartsWith("(string", StringComparison.Ordinal) ? 's' : text is "null" ? 'n' : text is "true" or "false" ? 'b' : '1';
                    if (has != should && (Kind(has) == Kind(should) || Kind(has) == 'n' || Kind(should) == 'n'))
                        Assert.Fail($"{t.Db.Name}/{t.Name} {key}.{column}: the state has {has}, the table {should}\n{Explain(v, stream)}");
                }
        }
    }

    [HegelTest(TestCases = 3000)]
    public void A_correct_stream_never_differs(TestCase tc) => NoFalseAlarm(tc);

    private static void NoFalseAlarm(TestCase tc)
    {
        var tape = DrawTape(tc, 2000, "tape");
        var sim = new Sim(tape) { RowsCarryPositions = tc.Draw(Generators.Booleans(), "rows carry positions") };
        // a stream whose beginning is gone (its limits): the verifier comes in at any point no later replay reaches back across
        var from = 0;
        if (tc.Draw(Generators.Booleans(), "the head of the stream is gone"))
        {
            sim.Run(tape.Next(30));
            sim.Forget();
            from = sim.Out.Count;
            Cover("stream-without-its-head");
        }
        sim.Run();
        var v = new StreamVerifier();
        Feed(v, sim.Out, from, sim.Out.Count);
        Assert.True(v.Differences == 0 && v.Unreadable == 0, Explain(v, sim.Out, from));

        // two loads of everything: the first makes every state complete, the second is compared with it row by row
        var fed = sim.Out.Count;
        sim.Settle();
        sim.Settle();
        Feed(v, sim.Out, fed, sim.Out.Count);
        Cover(sim);
        Cover("messages", sim.Out.Count);
        Cover("histories");
        var report = v.Report();
        Assert.True(report.Clean, Explain(v, sim.Out, from));
        AssertStateIsTheTable(v, sim, sim.Out);
        foreach (var t in sim.Dbs.SelectMany(d => d.Tables))
        {
            var r = report.Tables.Single(r => r.Server == t.Db.Server && r.Database == t.Db.Name && r.Schema == t.Schema && r.Table == t.Name);
            // a table without a key that never had a row is verified as what it is: empty
            if (t.Keyed) Assert.True(r.Verdict == Verdict.Verified && r.RowsInState == t.Rows.Count && r.LoadsCompared >= 1 && r.Baseline is not null, Explain(v, sim.Out, from));
            else Assert.True(r.Verdict == (r.WithoutKey > 0 ? Verdict.CannotBeVerified : Verdict.Verified), Explain(v, sim.Out, from));
            Cover(r.ReplaysSkipped > 0 ? "table-with-replays" : "table-without-replays");
            if (r.Retyped > 0) Cover("retyped-value-seen");
            if (r.LoadsStartedOver > 0) Cover("table-with-load-started-over");
        }
        if (report.Notes.Count > 0) Cover("incarnation-noted");
    }

    private static readonly string[] Numbers = ["0", "1", "2", "3", "-1", "7", "2147483647", "9007199254740992", "9007199254740993", "9223372036854775807", "5"];
    private static readonly string[] Strings = ["\"\"", "\"a\"", "\"A\"", "\"å\"", "\"abc\"", "\"\\ud83d\"", "\"\\ude00\"", "\"1\"", "\"0.10\"", "\"12.50\"", "\"AA==\"", "\"the quick brown fox jumps over the lazy dog\"", "\"the quick brown fox jumps over the lazy dot\"", "\"x\""];

    /// <summary>Another value for a column. Never one of another JSON kind (number for string): that is what a retyped column looks like, and by
    /// the rules it is not a difference. NULL is a value of every kind.</summary>
    private static string Other(TestCase tc, string json)
    {
        var now = Oracle.Canonical(json);
        var pool = json.StartsWith('"') ? Strings : json is "true" or "false" ? ["true", "false"] : json is "null" || json.StartsWith('{') ? [.. Numbers, .. Strings, "true"] : Numbers;
        var others = pool.Append("null").Where(x => Oracle.Canonical(x) != now).ToList();
        return tc.Draw(Generators.SampledFrom(others), "altered to");
    }

    /// <summary>The stream with one message dropped, altered, or exchanged with the next. Returns what was done.</summary>
    private static string Damage(TestCase tc, List<Ev> stream)
    {
        // anywhere: a number drawn from a range tends to be small, a large one modulo the length does not
        var at = tc.Draw(Generators.Integers().Min(0).Max(999_999), "damaged message") % stream.Count;
        var how = tc.Draw(Generators.Integers().Min(0).Max(5), "damage");
        var e = stream[at];
        var images = new List<bool>(); // false: before, true: after
        foreach (var after in new[] { false, true })
            if ((after ? e.After : e.Before) is { } i && e.Key is not null && i.Any(c => e.Key.All(k => k.Name != c.Name))) images.Add(after);
        if (how >= 3 && images.Count > 0)
        {
            // a value of a column that is not of the key
            var copy = e.Clone();
            var image = tc.Draw(Generators.SampledFrom(images), "in the after image") ? copy.After! : copy.Before!;
            var columns = Enumerable.Range(0, image.Count).Where(i => e.Key!.All(k => k.Name != image[i].Name)).ToList();
            var column = tc.Draw(Generators.SampledFrom(columns), "altered column");
            image[column] = (image[column].Name, Other(tc, image[column].Json));
            stream[at] = copy;
            return $"altered-{e.Op}";
        }
        if (how == 2 && at + 1 < stream.Count)
        {
            (stream[at], stream[at + 1]) = (stream[at + 1], stream[at]);
            return "swapped";
        }
        stream.RemoveAt(at);
        return $"dropped-{e.Op}";
    }

    /// <summary>
    /// Any one message of a correct stream is dropped, altered or exchanged with its neighbour. If anybody who follows the stream can see that
    /// (<see cref="Oracle"/>), the verifier reports a difference, at the latest when the next complete load ends.
    /// <para>What nobody can see, and so is not asked for (each of these is a correct stream of some table, or says the same as before):</para>
    /// <list type="bullet">
    /// <item>a message that is delivered again anyway, or was itself a repeat: a change at or below the high-water mark is passed over, so one
    /// copy of it is enough; a repeat that comes in order heals the loss;</item>
    /// <item>an update that is lost and then overwritten while no before image says what was there: the next change's before image has the
    /// lost columns as unavailable (or the update changed nothing a later image shows), or the row's database got a new incarnation, or the
    /// column was dropped before anything showed it again;</item>
    /// <item>a lost update of a column that was retyped since the stream last showed the row: what the stream had is of the old JSON kind, the
    /// next image of the new one, and by the rules that is a retyped value, not a difference;</item>
    /// <item>in a table without a complete load, a change of a row whose key another row moved onto earlier in the same transaction: the
    /// row that was there may be one the stream never showed;</item>
    /// <item>changes of a row the stream had said nothing about yet in a table without a complete load: "only once the key's state is known";</item>
    /// <item>a value altered into the value the table has, or in an image nothing is compared with (a row of a load for a key that is new);</item>
    /// <item>a value altered to another JSON kind: that is a retyped column by the rules, so the damage never does that;</item>
    /// <item>markers: a lost begin or end loses a comparison, not an event; rows of a load exchanged with rows of other keys;</item>
    /// <item>messages of different databases, tables or rows exchanged: every order of those is a correct stream. (Two changes of one database
    /// exchanged are a lost change: the first is then below the high-water mark.)</item>
    /// <item>tables without a key.</item>
    /// </list>
    /// </summary>
    [HegelTest(TestCases = 3000)]
    public void Nothing_is_missed(TestCase tc) => NothingIsMissed(tc);

    private static void NothingIsMissed(TestCase tc)
    {
        var sim = new Sim(DrawTape(tc, 1500, "tape"));
        sim.Run();
        if (sim.Out.Count == 0) { Cover("empty-stream"); return; }
        var stream = sim.Out.ToList();
        var original = stream.ToList();
        var damage = Damage(tc, stream);
        // (two copies of one message exchanged are the stream as it was)
        var damaged = Enumerable.Range(0, original.Count).Where(i => i >= stream.Count || !ReferenceEquals(original[i], stream[i])).DefaultIfEmpty(0).First();
        var why = !original[damaged].Table.Keyed ? "no key" : original.Count(e => e.Id == original[damaged].Id) > 1 ? "delivered more than once" : original[damaged].Op == 'l' ? "marker" : "other";
        // the miner goes on, correctly, and nothing it sends again reaches back across the damage
        var cut = sim.Out.Count;
        sim.Forget();
        sim.Use(DrawTape(tc, 100, "afterwards"));
        sim.Run();
        var loads = sim.Settle() - cut + stream.Count;
        stream.AddRange(sim.Out.Skip(cut));

        var oracle = new Oracle();
        for (var i = 0; i < loads; i++) oracle.Add(stream[i]);
        oracle.CompareWithTables(sim.Dbs.SelectMany(d => d.Tables));

        var v = new StreamVerifier();
        Feed(v, stream, 0, stream.Count);
        Cover(sim);
        Cover(oracle.Seen is null ? "harmless" : oracle.ByChain ? "seen-by-chain" : "seen-by-load");
        Cover((oracle.Seen is null ? "harmless: " : "seen: ") + damage);
        if (oracle.Seen is null) Cover($"harmless ({why}): {damage}");
        // One more thing that cannot be seen, by the rules about columns: a row of a load that was read before a column was dropped and is
        // published after changes without it. For the verifier that column is gone (a column of that name in a later image is another one,
        // or a column dropped and added again would differ in every row), so a damaged value in it is damage to nothing.
        if (damage == "altered-r" && stream[damaged].After!.Zip(original[damaged].After!).First(c => c.First.Json != c.Second.Json).First.Name is var column
            && stream.Take(damaged).LastOrDefault(e => e.IsChange && e.Table == stream[damaged].Table && e.Incarnation == stream[damaged].Incarnation) is { } change
            && (change.After ?? change.Before!).All(c => c.Name != column))
        {
            Cover("altered in a dropped column of a row read earlier");
            return;
        }
        if (oracle.Seen is not null) Assert.True(v.Differences > 0, $"{damage}; anybody can see: {oracle.Seen}\nbut the verifier says:\n{Explain(v, stream)}");
        if (oracle.Seen is null && v.Differences > 0) Cover("reported-beyond-the-oracle: " + damage);

        // REPAIR: after the loads the state is the table again, and the same stream, continued correctly, gives nothing more
        var (reported, fed) = (v.Differences, stream.Count);
        AssertStateIsTheTable(v, sim, stream);
        cut = sim.Out.Count;
        sim.Use(DrawTape(tc, 200, "later"));
        sim.Run();
        sim.Settle();
        stream.AddRange(sim.Out.Skip(cut));
        Feed(v, stream, fed, stream.Count);
        Assert.True(v.Differences == reported, $"{damage}: {v.Differences - reported} more difference(s) after the loads that followed it\n{Explain(v, stream)}");
        AssertStateIsTheTable(v, sim, stream);
    }

    /// <summary>The properties are only as good as the histories they reach: each of these must occur in a normal run.</summary>
    [Fact]
    public void The_generator_reaches_the_interesting_histories()
    {
        Coverage.Clear();
        Property.Test(NoFalseAlarm, new Settings { TestCases = 500 });
        Property.Test(NothingIsMissed, new Settings { TestCases = 3000 });
        string[] required =
        [
            "insert", "insert-without-key", "update", "delete", "key-changed", "keys-swapped", "keys-shifted", "transaction-of-several", "unavailable", "new-vlf",
            "load-begins", "load-row", "load-ends", "chunk-row-dropped", "load-starts-over", "chunk-published-again", "restart-during-load", "replay", "restart-inside-transaction",
            "load-message-redelivered", "column-added", "column-dropped", "column-retyped", "column-retyped-back", "incarnation", "incarnation-noted",
            "stream-without-its-head", "table-with-replays", "retyped-value-seen", "table-with-load-started-over",
            "seen-by-chain", "seen-by-load", "harmless", "loaded-first",
            "seen: dropped-c", "seen: dropped-u", "seen: dropped-d", "seen: dropped-r", "seen: altered-c", "seen: altered-u", "seen: altered-d", "seen: altered-r", "seen: swapped",
        ];
        var report = string.Join(" | ", Coverage.OrderBy(k => k.Key, StringComparer.Ordinal).Select(k => $"{k.Key}={k.Value}"));
        var missing = required.Where(r => !Coverage.ContainsKey(r)).ToList();
        Assert.True(missing.Count == 0, $"never reached: {string.Join(", ", missing)}; coverage: {report}");
        File.WriteAllText(Path.Combine(AppContext.BaseDirectory, "coverage.txt"), report); // for whoever wants to look
    }
}
