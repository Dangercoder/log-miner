using System.Globalization;
using System.Text;
using System.Text.Json;

namespace LogMiner.Verify;

/// <summary>How the stream and the table disagree. Each is one row at one place in the stream.</summary>
public enum DifferenceKind
{
    /// <summary>The before image of an update or delete is not what the stream said the row was: a change to it is missing (or was altered).</summary>
    BeforeImage,
    /// <summary>An insert (or a key change) onto a key that is there: its delete is missing.</summary>
    RowAlreadyThere,
    /// <summary>An update or delete of a row that a complete state does not have: its insert is missing.</summary>
    RowNotThere,
    /// <summary>A row of a historical load is not what the stream said it was: an update is missing.</summary>
    LoadRowDiffers,
    /// <summary>A row of a historical load that a complete state does not have: an insert is missing.</summary>
    LoadRowNotInStream,
    /// <summary>A row that a finished load neither read nor saw changed: the table does not have it, a delete is missing.</summary>
    RowNotInLoad,
    /// <summary>A load announced another number of rows than arrived: rows of the load itself are missing.</summary>
    LoadRowCount,
}

public enum Verdict
{
    /// <summary>At least one historical load was compared row by row with a complete state, and nothing ever differed.</summary>
    Verified,
    NotVerifiedYet,
    Different,
    /// <summary>No key: rows cannot be told apart.</summary>
    CannotBeVerified,
}

public sealed record ColumnDifference(string Column, string Expected, string Got);

/// <param name="Position">commit_lsn/seq of the change, or the lsn of a load's row.</param>
/// <param name="Sequence">The stream sequence number of the message that showed it (0 when the caller gave none).</param>
public sealed record Difference(string Table, DifferenceKind Kind, string? Key, IReadOnlyList<ColumnDifference> Columns, string Position, ulong Sequence, string What)
{
    public override string ToString()
    {
        var sb = new StringBuilder().Append(CultureInfo.InvariantCulture, $"{Table} {Kind}");
        if (Key is not null) sb.Append(' ').Append(Key);
        sb.Append(": ").Append(What);
        foreach (var c in Columns) sb.Append(CultureInfo.InvariantCulture, $"; {c.Column}: expected {c.Expected}, got {c.Got}");
        sb.Append(CultureInfo.InvariantCulture, $" (at {Position}");
        if (Sequence != 0) sb.Append(CultureInfo.InvariantCulture, $", stream sequence {Sequence}");
        return sb.Append(')').ToString();
    }
}

public sealed class TableReport
{
    public required string Server { get; init; }
    public required string Database { get; init; }
    public required string Schema { get; init; }
    public required string Table { get; init; }
    public required string Incarnation { get; init; }
    public string Name => $"{Server}/{Database}/{Schema}.{Table}";
    public required Verdict Verdict { get; init; }
    /// <summary>Why it is not verified, or what the verification rests on.</summary>
    public required string Reason { get; init; }
    /// <summary>Where the state became complete for the first time ("first complete load at ..."), or null.</summary>
    public string? Baseline { get; init; }
    public long RowsInState { get; init; }
    public long StateBytes { get; init; }
    /// <summary>Messages of the table, repeats included.</summary>
    public long Events { get; init; }
    public long Changes { get; init; }
    public long LoadRows { get; init; }
    /// <summary>Loads that ended; <see cref="LoadsCompared"/> of them against a complete state.</summary>
    public long Loads { get; init; }
    public long LoadsCompared { get; init; }
    public long LoadsStartedOver { get; init; }
    public long ReplaysSkipped { get; init; }
    /// <summary>Column values whose JSON kind changed (a type change): counted, not differences.</summary>
    public long Retyped { get; init; }
    /// <summary>Events without a key.</summary>
    public long WithoutKey { get; init; }
    /// <summary>Changes that could not be checked: a key value that is not in the before image.</summary>
    public long NotCheckable { get; init; }
    /// <summary>Rows displaced by another with their key in a transaction that had not ended when the stream did.</summary>
    public long Undecided { get; init; }
    public long Resets { get; init; }
    public long Differences { get; init; }
    public required IReadOnlyDictionary<DifferenceKind, long> ByKind { get; init; }
    public required IReadOnlyList<Difference> Examples { get; init; }
}

public sealed class VerificationReport
{
    public long Messages { get; init; }
    public ulong FirstSequence { get; init; }
    public ulong LastSequence { get; init; }
    public DateTimeOffset? FirstTime { get; init; }
    /// <summary>Messages that are not an envelope, or lack what a change must have. Each could be a lost event.</summary>
    public long Unreadable { get; init; }
    public required IReadOnlyList<string> UnreadableExamples { get; init; }
    public long UnknownOperations { get; init; }
    /// <summary>What happened to whole databases: incarnations that changed.</summary>
    public required IReadOnlyList<string> Notes { get; init; }
    public required IReadOnlyList<TableReport> Tables { get; init; }

    public long Differences => Tables.Sum(t => t.Differences);
    public long StateBytes => Tables.Sum(t => t.StateBytes);
    public long Rows => Tables.Sum(t => t.RowsInState);
    /// <summary>Nothing differs and every message could be read.</summary>
    public bool Clean => Differences == 0 && Unreadable == 0;
    public int Count(Verdict v) => Tables.Count(t => t.Verdict == v);

    public string ToText()
    {
        var c = CultureInfo.InvariantCulture;
        var sb = new StringBuilder();
        foreach (var t in Tables)
        {
            var verdict = t.Verdict switch { Verdict.Verified => "verified", Verdict.Different => "DIFFERENT", Verdict.CannotBeVerified => "cannot be verified", _ => "not verified yet" };
            sb.Append(c, $"{t.Name}: {verdict} - {t.Reason}\n");
            sb.Append(c, $"  baseline: {t.Baseline ?? "none"}\n");
            sb.Append(c, $"  rows in state {t.RowsInState} ({t.StateBytes} bytes), events {t.Events} (changes {t.Changes}, load rows {t.LoadRows}), loads {t.Loads} ({t.LoadsCompared} compared, {t.LoadsStartedOver} started over), replays skipped {t.ReplaysSkipped}");
            if (t.Retyped > 0) sb.Append(c, $", retyped values {t.Retyped}");
            if (t.WithoutKey > 0) sb.Append(c, $", events without a key {t.WithoutKey}");
            if (t.NotCheckable > 0) sb.Append(c, $", changes that could not be checked {t.NotCheckable}");
            if (t.Undecided > 0) sb.Append(c, $", undecided at the end {t.Undecided}");
            if (t.Resets > 0) sb.Append(c, $", state reset {t.Resets} time(s)");
            sb.Append('\n');
            if (t.Differences == 0) continue;
            sb.Append(c, $"  differences {t.Differences}: {string.Join(", ", t.ByKind.Where(k => k.Value > 0).Select(k => $"{k.Key} {k.Value}"))}\n");
            foreach (var d in t.Examples) sb.Append("    ").Append(d).Append('\n');
        }
        foreach (var n in Notes) sb.Append(n).Append('\n');
        if (Unreadable > 0)
        {
            sb.Append(c, $"UNREADABLE: {Unreadable} message(s) are not change events as LogMiner publishes them; what they were cannot be verified\n");
            foreach (var u in UnreadableExamples) sb.Append("    ").Append(u).Append('\n');
        }
        if (UnknownOperations > 0) sb.Append(c, $"{UnknownOperations} message(s) with an operation this verifier does not know were passed over\n");
        sb.Append(c, $"{Messages} messages");
        if (LastSequence != 0) sb.Append(c, $" (stream sequence {FirstSequence} to {LastSequence})");
        sb.Append(c, $", {Tables.Count} tables: {Count(Verdict.Verified)} verified, {Count(Verdict.NotVerifiedYet)} not verified yet, {Count(Verdict.CannotBeVerified)} cannot be verified, {Count(Verdict.Different)} DIFFERENT; {Rows} rows in {StateBytes} bytes of state\n");
        return sb.ToString();
    }

    public string ToJson()
    {
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            w.WriteStartObject();
            w.WriteBoolean("clean", Clean);
            w.WriteNumber("messages", Messages);
            w.WriteNumber("first_sequence", FirstSequence);
            w.WriteNumber("last_sequence", LastSequence);
            if (FirstTime is { } first) w.WriteString("first_time", first);
            w.WriteNumber("differences", Differences);
            w.WriteNumber("unreadable", Unreadable);
            w.WriteNumber("unknown_operations", UnknownOperations);
            w.WriteNumber("verified", Count(Verdict.Verified));
            w.WriteNumber("not_verified_yet", Count(Verdict.NotVerifiedYet));
            w.WriteNumber("cannot_be_verified", Count(Verdict.CannotBeVerified));
            w.WriteNumber("different", Count(Verdict.Different));
            w.WriteNumber("rows", Rows);
            w.WriteNumber("state_bytes", StateBytes);
            w.WriteStartArray("notes");
            foreach (var n in Notes) w.WriteStringValue(n);
            foreach (var u in UnreadableExamples) w.WriteStringValue("unreadable: " + u);
            w.WriteEndArray();
            w.WriteStartArray("tables");
            foreach (var t in Tables)
            {
                w.WriteStartObject();
                w.WriteString("server", t.Server);
                w.WriteString("db", t.Database);
                w.WriteString("schema", t.Schema);
                w.WriteString("table", t.Table);
                w.WriteString("incarnation", t.Incarnation);
                w.WriteString("verdict", t.Verdict switch { Verdict.Verified => "verified", Verdict.Different => "different", Verdict.CannotBeVerified => "cannot_be_verified", _ => "not_verified_yet" });
                w.WriteString("reason", t.Reason);
                if (t.Baseline is null) w.WriteNull("baseline"); else w.WriteString("baseline", t.Baseline);
                w.WriteNumber("rows", t.RowsInState);
                w.WriteNumber("state_bytes", t.StateBytes);
                w.WriteNumber("events", t.Events);
                w.WriteNumber("changes", t.Changes);
                w.WriteNumber("load_rows", t.LoadRows);
                w.WriteNumber("loads", t.Loads);
                w.WriteNumber("loads_compared", t.LoadsCompared);
                w.WriteNumber("loads_started_over", t.LoadsStartedOver);
                w.WriteNumber("replays_skipped", t.ReplaysSkipped);
                w.WriteNumber("retyped", t.Retyped);
                w.WriteNumber("without_key", t.WithoutKey);
                w.WriteNumber("not_checkable", t.NotCheckable);
                w.WriteNumber("undecided", t.Undecided);
                w.WriteNumber("resets", t.Resets);
                w.WriteNumber("differences", t.Differences);
                w.WriteStartObject("by_kind");
                foreach (var k in t.ByKind) if (k.Value > 0) w.WriteNumber(k.Key.ToString(), k.Value);
                w.WriteEndObject();
                w.WriteStartArray("examples");
                foreach (var d in t.Examples)
                {
                    w.WriteStartObject();
                    w.WriteString("kind", d.Kind.ToString());
                    if (d.Key is null) w.WriteNull("key"); else w.WriteString("key", d.Key);
                    w.WriteString("what", d.What);
                    w.WriteString("position", d.Position);
                    w.WriteNumber("sequence", d.Sequence);
                    w.WriteStartArray("columns");
                    foreach (var col in d.Columns)
                    {
                        w.WriteStartObject();
                        w.WriteString("column", col.Column);
                        w.WriteString("expected", col.Expected);
                        w.WriteString("got", col.Got);
                        w.WriteEndObject();
                    }
                    w.WriteEndArray();
                    w.WriteEndObject();
                }
                w.WriteEndArray();
                w.WriteEndObject();
            }
            w.WriteEndArray();
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.GetBuffer(), 0, (int)stream.Length);
    }
}
