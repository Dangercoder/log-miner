using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Hegel;
using Hegel.Xunit;
using Xunit;

namespace LogMiner.Verify.Tests;

/// <summary>The verifier on streams written by hand: what it says, in which words, and the corners the generated histories only graze.</summary>
public class StreamVerifierTests
{
    private int _block = 0x10;
    private ulong _sequence;
    private readonly StreamVerifier _v = new();

    private static string Source(string table, string db, string incarnation, string rest) =>
        $"\"source\":{{\"server\":\"srv\",\"db\":\"{db}\",\"incarnation\":\"{incarnation}\",\"schema\":\"dbo\",\"table\":\"{table}\",\"lsn\":\"0000002A:00000001:0001\"{rest}}}";

    private void Add(string json, string? id = null) => _v.Add("cdc.srv.db.dbo.t", Encoding.UTF8.GetBytes(json), id, ++_sequence);

    /// <summary>One change in a transaction of its own.</summary>
    private void Change(string op, string key, string? before, string? after, string table = "t", string db = "db", string incarnation = "i1", int? block = null, int seq = 0) =>
        Add($"{{\"op\":\"{op}\",\"key\":{key},\"before\":{before ?? "null"},\"after\":{after ?? "null"},{Source(table, db, incarnation, $",\"commit_lsn\":\"0000002A:{block ?? ++_block:X8}:0001\",\"seq\":{seq}")}}}");

    private void Row(string key, string row, string load = "L1", string table = "t", string? id = null, string rest = "") =>
        Add($"{{\"op\":\"r\",\"key\":{key},\"before\":null,\"after\":{row},{Source(table, "db", "i1", $",\"snapshot\":\"{load}\"{rest}")}}}", id);

    private void Marker(string phase, string load = "L1", long? rows = null, string table = "t") =>
        Add($"{{\"op\":\"l\",\"key\":null,\"before\":null,\"after\":null,\"load\":{{\"id\":\"{load}\",\"phase\":\"{phase}\"{(rows is null ? "" : $",\"rows\":{rows}")}}},{Source(table, "db", "i1", $",\"snapshot\":\"{load}\"")}}}");

    private TableReport T(string table = "t") => _v.Report().Tables.Single(t => t.Table == table && t.Database == "db");

    [Fact]
    public void A_lost_update_shows_with_the_next_change_of_the_row_and_once()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":10,\"w\":\"x\"}");
        // v 10 -> 20 was lost
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":20,\"w\":\"x\"}", "{\"id\":1,\"v\":30,\"w\":\"x\"}");
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":30,\"w\":\"x\"}", "{\"id\":1,\"v\":40,\"w\":\"x\"}");
        var t = T();
        Assert.Equal(Verdict.Different, t.Verdict);
        Assert.Equal(1, t.Differences);
        var d = Assert.Single(t.Examples);
        Assert.Equal((DifferenceKind.BeforeImage, "{\"id\":1}", 2UL, "commit 0000002A:00000012:0001 seq 0"), (d.Kind, d.Key, d.Sequence, d.Position));
        Assert.Equal(new ColumnDifference("v", "10", "20"), Assert.Single(d.Columns));
        Assert.Contains("DIFFERENT", _v.Report().ToText(), StringComparison.Ordinal);
        Assert.Contains("v: expected 10, got 20", _v.Report().ToText(), StringComparison.Ordinal);
    }

    [Fact]
    public void Keys_that_differ_in_case_type_or_half_a_surrogate_pair_are_different_rows()
    {
        string[] keys = ["\"a\"", "\"A\"", "1", "\"1\"", "1.0", "\"\\ud83d\"", "\"\\ude00\"", "\"\\ud83d\\ude00\"", "9007199254740992", "9007199254740993", "null", "\"\"", "\"null\"", "true", "\"å\"", "\"a\\u0000\""];
        foreach (var k in keys) Change("c", $"{{\"id\":{k}}}", null, $"{{\"id\":{k},\"v\":1}}");
        Assert.Equal((0L, (long)keys.Length), (T().Differences, T().RowsInState));
        // and each is found again under its own key only
        foreach (var k in keys) Change("d", $"{{\"id\":{k}}}", $"{{\"id\":{k},\"v\":1}}", null);
        Assert.Equal((0L, 0L), (T().Differences, T().RowsInState));
    }

    [Fact]
    public void A_key_is_what_its_text_means_not_how_it_is_spelled()
    {
        Change("c", "{\"id\":\"å😀\"}", null, "{\"id\":\"å😀\",\"v\":\"å\"}");
        Change("u", "{\"id\":\"\\u00e5\\uD83D\\uDE00\"}", "{\"id\":\"\\u00E5\\ud83d\\ude00\",\"v\":\"\\u00e5\"}", "{\"id\":\"å😀\",\"v\":\"b\\/\"}");
        Change("d", "{\"id\":\"å😀\"}", "{\"id\":\"å😀\",\"v\":\"b/\"}", null);
        Assert.Equal((0L, 0L), (T().Differences, T().RowsInState));
    }

    [Theory]
    [InlineData("9007199254740992", "9007199254740993")] // the same double
    [InlineData("\"12.50\"", "\"12.5\"")] // a decimal is its text
    [InlineData("\"\\ud83d\"", "\"\\ude00\"")]
    [InlineData("\"\\ud83d\"", "\"\\ufffd\"")] // what half a pair becomes when it is decoded carelessly
    [InlineData("\"3q2+7w==\"", "\"3q2+7W==\"")]
    [InlineData("\"the quick brown fox jumps over the lazy dog\"", "\"the quick brown fox jumps over the lazy dot\"")]
    [InlineData("null", "0")]
    [InlineData("null", "\"\"")]
    [InlineData("true", "false")]
    [InlineData("1", "1.0")]
    [InlineData("{\"undecodable\":\"AAE=\"}", "{\"undecodable\":\"AAI=\"}")]
    public void Values_that_look_alike_are_different(string was, string claimed)
    {
        Change("c", "{\"id\":1}", null, $"{{\"id\":1,\"v\":{was}}}");
        Change("u", "{\"id\":1}", $"{{\"id\":1,\"v\":{claimed}}}", $"{{\"id\":1,\"v\":{was}}}");
        Change("u", "{\"id\":1}", $"{{\"id\":1,\"v\":{was}}}", $"{{\"id\":1,\"v\":{was}}}");
        Assert.Equal(1, T().Differences);
    }

    [Fact]
    public void A_load_is_compared_row_by_row_and_what_it_does_not_read_is_not_in_the_table()
    {
        Assert.Empty(_v.Report().Tables);
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":1}");
        Assert.Equal(Verdict.NotVerifiedYet, T().Verdict);
        Assert.Contains("does not reach back", T().Reason, StringComparison.Ordinal);
        Assert.Null(T().Baseline);
        Marker("begin");
        Assert.Contains("has begun", T().Reason, StringComparison.Ordinal);
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}");
        Row("{\"id\":2}", "{\"id\":2,\"v\":2}"); // was there before the stream began
        Row("{\"id\":3}", "{\"id\":3,\"v\":3}");
        Marker("end", rows: 3);
        Assert.Equal((Verdict.NotVerifiedYet, 0L, 3L, 1L, 0L), (T().Verdict, T().Differences, T().RowsInState, T().Loads, T().LoadsCompared));
        Assert.StartsWith("first complete load at stream sequence 6", T().Baseline, StringComparison.Ordinal);
        Assert.Contains("baseline", T().Reason, StringComparison.Ordinal);

        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":2}", "{\"id\":2,\"v\":20}");
        Marker("begin", "L2");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}", "L2");
        Row("{\"id\":2}", "{\"id\":2,\"v\":20}", "L2");
        Change("d", "{\"id\":3}", "{\"id\":3,\"v\":3}", null); // before its part was read: the change is in the stream instead
        Marker("end", "L2", 2);
        Assert.Equal((Verdict.Verified, 0L, 2L, 2L, 1L), (T().Verdict, T().Differences, T().RowsInState, T().Loads, T().LoadsCompared));

        // the stream lost: an update of 1, the delete of 2, the insert of 4
        Marker("begin", "L3");
        Row("{\"id\":1}", "{\"id\":1,\"v\":100}", "L3");
        Row("{\"id\":4}", "{\"id\":4,\"v\":4}", "L3");
        Marker("end", "L3", 2);
        var t = T();
        Assert.Equal(Verdict.Different, t.Verdict);
        Assert.Equal((1L, 1L, 1L, 3L), (t.ByKind[DifferenceKind.LoadRowDiffers], t.ByKind[DifferenceKind.LoadRowNotInStream], t.ByKind[DifferenceKind.RowNotInLoad], t.Differences));
        Assert.Equal("{\"id\":2}", t.Examples.Single(e => e.Kind == DifferenceKind.RowNotInLoad).Key);
        Assert.Equal(new ColumnDifference("v", "1", "100"), t.Examples.Single(e => e.Kind == DifferenceKind.LoadRowDiffers).Columns.Single());

        // repaired: the next load finds nothing, and the changes chain on
        Change("u", "{\"id\":4}", "{\"id\":4,\"v\":4}", "{\"id\":4,\"v\":5}");
        Marker("begin", "L4");
        Row("{\"id\":1}", "{\"id\":1,\"v\":100}", "L4");
        Row("{\"id\":4}", "{\"id\":4,\"v\":5}", "L4");
        Marker("end", "L4", 2);
        Assert.Equal((3L, 2L, 3L), (T().Differences, T().RowsInState, T().LoadsCompared));
    }

    [Fact]
    public void A_complete_state_knows_which_rows_are_not_there()
    {
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":1}", "{\"id\":1,\"v\":2}"); // not known yet: learned, not judged
        Change("d", "{\"id\":7}", "{\"id\":7,\"v\":1}", null);
        Assert.Equal(0, T().Differences);
        Marker("begin");
        Row("{\"id\":1}", "{\"id\":1,\"v\":2}");
        Marker("end", rows: 1);
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":1}", "{\"id\":2,\"v\":2}");
        Change("d", "{\"id\":3}", "{\"id\":3,\"v\":1}", null);
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":9}");
        Change("c", "{\"id\":8}", null, "{\"id\":8,\"v\":9}"); // settles the transaction before
        Assert.Equal((2L, 1L), (T().ByKind[DifferenceKind.RowNotThere], T().ByKind[DifferenceKind.RowAlreadyThere]));
    }

    [Fact]
    public void Rows_of_a_load_that_do_not_arrive_are_missing_rows_not_missing_deletes()
    {
        Marker("begin");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}");
        Row("{\"id\":2}", "{\"id\":2,\"v\":2}");
        Marker("end", rows: 2);
        Marker("begin", "L2");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}", "L2");
        Marker("end", "L2", 2);
        var t = T();
        Assert.Equal((1L, 1L, 2L, 0L), (t.Differences, t.ByKind[DifferenceKind.LoadRowCount], t.RowsInState, t.LoadsCompared));
        Assert.Contains("announced 2 row(s), 1 arrived", t.Examples.Single().What, StringComparison.Ordinal);
    }

    [Fact]
    public void A_load_that_starts_over_counts_from_its_last_begin()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":1}");
        Change("c", "{\"id\":2}", null, "{\"id\":2,\"v\":1}");
        Marker("begin");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}");
        Change("d", "{\"id\":1}", "{\"id\":1,\"v\":1}", null);
        Marker("begin");
        Row("{\"id\":2}", "{\"id\":2,\"v\":1}");
        Marker("end", rows: 1);
        Marker("end", rows: 1); // of nothing that began
        Assert.Equal((0L, 1L, 1L, 1L), (T().Differences, T().RowsInState, T().Loads, T().LoadsStartedOver));
    }

    [Fact]
    public void A_value_that_is_not_available_matches_anything_and_is_not_known_afterwards()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":1,\"big\":\"x\"}");
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":1,\"big\":{\"unavailable\":true}}", "{\"id\":1,\"v\":2,\"big\":{\"unavailable\":true}}");
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":2,\"big\":\"whatever it was\"}", "{\"id\":1,\"v\":3,\"big\":\"y\"}");
        Assert.Equal(0, T().Differences);
        // something else in braces is a value like any other
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":3,\"big\":\"y\"}", "{\"id\":1,\"v\":3,\"big\":{\"unavailable\":false}}");
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":3,\"big\":{\"unavailable\":true,\"x\":1}}", "{\"id\":1,\"v\":3,\"big\":\"y\"}");
        Assert.Equal(1, T().Differences);
    }

    [Fact]
    public void Columns_that_come_and_go_and_change_their_type_are_not_differences()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":5,\"gone\":\"x\"}");
        Change("c", "{\"id\":2}", null, "{\"id\":2,\"v\":6,\"gone\":\"x\"}");
        // dropped, added, int -> varchar: no events
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":\"5\",\"new\":0}", "{\"id\":1,\"v\":\"five\",\"new\":1}");
        Assert.Equal((0L, 1L), (T().Differences, T().Retyped));
        // the column that went comes back under its name, with other values: another column
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":\"6\",\"new\":0,\"gone\":null}", "{\"id\":2,\"v\":\"6\",\"new\":0,\"gone\":7}");
        Assert.Equal((0L, 2L), (T().Differences, T().Retyped));
        // but NULL is a value of every type
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":null,\"new\":0,\"gone\":7}", "{\"id\":2,\"v\":null,\"new\":0,\"gone\":7}");
        Assert.Equal(1, T().Differences);
    }

    [Fact]
    public void A_new_incarnation_resets_its_database_and_says_so()
    {
        Marker("begin");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}");
        Marker("end", rows: 1);
        Marker("begin", "L2");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}", "L2");
        Marker("end", "L2", 1);
        Change("c", "{\"id\":5}", null, "{\"id\":5}", db: "other", block: 0x500);
        Assert.Equal(Verdict.Verified, T().Verdict);
        // restored: positions start over, the row is another
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":77}", "{\"id\":1,\"v\":78}", incarnation: "i2", block: 2);
        Change("c", "{\"id\":6}", null, "{\"id\":6}", db: "other", block: 0x501);
        var report = _v.Report();
        Assert.Equal((Verdict.NotVerifiedYet, 0L, 1L, 1L), (T().Verdict, T().Differences, T().Resets, T().RowsInState));
        Assert.Contains("incarnation changed", T().Reason, StringComparison.Ordinal);
        Assert.Contains("srv/db: incarnation changed from i1 to i2 at stream sequence 8", Assert.Single(report.Notes), StringComparison.Ordinal);
        Assert.Equal(2, report.Tables.Single(t => t.Database == "other").RowsInState);
    }

    [Fact]
    public void A_table_without_a_key_cannot_be_verified_and_is_counted()
    {
        Change("c", "null", null, "{\"v\":1}");
        Change("d", "null", "{\"v\":1}", null);
        Marker("begin");
        Row("null", "{\"v\":2}");
        Marker("end", rows: 1);
        Assert.Equal((Verdict.CannotBeVerified, 3L, 0L), (T().Verdict, T().WithoutKey, T().Differences));
        Assert.Contains("cannot be verified", _v.Report().ToText(), StringComparison.Ordinal);
    }

    [Fact]
    public void Other_key_columns_are_another_table()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"code\":\"a\"}");
        Change("c", "{\"code\":\"b\"}", null, "{\"id\":2,\"code\":\"b\"}");
        Change("u", "{\"code\":\"a\"}", "{\"id\":1,\"code\":\"a\"}", "{\"id\":1,\"code\":\"a\"}");
        Assert.Equal((0L, 2L, 1L), (T().Differences, T().RowsInState, T().Resets));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("[1,2]")]
    [InlineData("{\"op\":\"c\",\"key\":{\"id\":1},\"before\":null,\"after\":{\"id\":1}")] // cut
    [InlineData("{\"op\":\"c\",\"key\":{\"id\":1},\"before\":null,\"after\":{\"id\":1}}")] // no source
    [InlineData("{\"op\":\"c\",\"key\":{\"id\":1},\"before\":null,\"after\":{\"id\":1},\"source\":{\"server\":\"s\",\"db\":\"d\",\"schema\":\"dbo\",\"table\":\"t\"}}")] // no position
    [InlineData("{\"op\":\"c\",\"key\":{\"id\":1},\"before\":null,\"after\":null,\"source\":{\"server\":\"s\",\"db\":\"d\",\"schema\":\"dbo\",\"table\":\"t\",\"commit_lsn\":\"0000002A:00000001:0001\",\"seq\":0}}")]
    [InlineData("{\"op\":\"c\",\"key\":{\"id\":1},\"before\":null,\"after\":{\"id\":1},\"source\":{\"server\":\"s\",\"db\":\"d\",\"schema\":\"dbo\",\"table\":\"t\",\"commit_lsn\":\"2A:1:1\",\"seq\":0}}")]
    [InlineData("{\"op\":\"l\",\"key\":null,\"load\":{\"phase\":\"middle\",\"id\":\"x\"},\"source\":{\"server\":\"s\",\"db\":\"d\",\"schema\":\"dbo\",\"table\":\"t\"}}")]
    [InlineData("{\"op\":\"c\",\"key\":{\"id\":1},\"before\":null,\"after\":{\"id\":1},\"source\":{\"server\":\"s\",\"db\":\"d\",\"schema\":\"dbo\",\"table\":\"t\",\"commit_lsn\":\"0000002A:00000001:0001\",\"seq\":0}} trailing")]
    public void A_message_that_cannot_be_read_is_never_passed_over_silently(string payload)
    {
        Add(payload);
        var report = _v.Report();
        Assert.Equal((1L, false), (report.Unreadable, report.Clean));
        Assert.Contains("UNREADABLE", report.ToText(), StringComparison.Ordinal);
        Assert.Single(report.UnreadableExamples);
    }

    [Fact]
    public void An_operation_that_is_not_known_is_counted_and_harmless()
    {
        Add($"{{\"op\":\"x\",\"key\":null,\"before\":null,\"after\":null,{Source("t", "db", "i1", "")}}}");
        Assert.Equal((1L, true), (_v.Report().UnknownOperations, _v.Report().Clean));
    }

    [Fact]
    public void Rows_of_a_load_never_touch_the_high_water_mark()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":1}", block: 0x100);
        Marker("begin");
        // as they used to be published: with a commit_lsn and seq of their own, here far above and far below every change
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}", rest: ",\"commit_lsn\":\"FFFFFFFF:00000001:0001\",\"seq\":0");
        Row("{\"id\":2}", "{\"id\":2,\"v\":2}", rest: ",\"commit_lsn\":\"00000001:00000001:0001\",\"seq\":0");
        Marker("end", rows: 2);
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":2}", "{\"id\":2,\"v\":3}", block: 0x101);
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":3}", "{\"id\":2,\"v\":4}", block: 0x101); // the same again
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":3}", "{\"id\":2,\"v\":4}", block: 0x101, seq: 1);
        Assert.Equal((0L, 2L, 1L, 3L), (T().Differences, T().RowsInState, T().ReplaysSkipped, T().Changes));
    }

    [Fact]
    public void Positions_order_by_all_their_parts()
    {
        void At(string commit, int seq, int v) => Add($"{{\"op\":\"u\",\"key\":{{\"id\":1}},\"before\":{{\"id\":1,\"v\":{v - 1}}},\"after\":{{\"id\":1,\"v\":{v}}},{Source("t", "db", "i1", $",\"commit_lsn\":\"{commit}\",\"seq\":{seq}")}}}");
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":0}", block: 1);
        At("0000002A:000000FF:0001", 0, 1);
        At("0000002A:000000FF:0001", 2, 2);
        At("0000002A:000000FF:000A", 0, 3); // the slot counts
        At("0000002A:00000100:0001", 1, 4);
        At("0000002B:00000001:0001", 0, 5); // a new virtual log file: its blocks start low
        At("0000002B:00000001:0001", 0, 99); // repeats, each at or below the mark
        At("0000002A:FFFFFFFF:FFFF", 9, 99);
        At("0000002B:00000000:FFFF", 9, 99);
        At("0x0000002B:00000002:0001", 0, 6);
        Assert.Equal((0L, 3L, 7L), (T().Differences, T().ReplaysSkipped, T().Changes));
    }

    [Fact]
    public void A_repeated_row_of_a_load_is_told_by_its_message_id()
    {
        Marker("begin");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}", id: "snap:1");
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":1}", "{\"id\":1,\"v\":2}");
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}", id: "snap:1"); // delivered again, behind the change
        Marker("end", rows: 1);
        Assert.Equal((0L, 1L), (T().Differences, T().ReplaysSkipped));
        // without an id nothing tells: the row is taken for what the table has now
        Row("{\"id\":1}", "{\"id\":1,\"v\":1}");
        Assert.Equal(1, T().Differences);
    }

    [Fact]
    public void Two_rows_that_exchange_their_keys_in_one_transaction_are_not_a_missing_delete()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":\"one\"}");
        Change("c", "{\"id\":2}", null, "{\"id\":2,\"v\":\"two\"}");
        Change("u", "{\"id\":2}", "{\"id\":1,\"v\":\"one\"}", "{\"id\":2,\"v\":\"one\"}", block: 0x50, seq: 0);
        Change("u", "{\"id\":1}", "{\"id\":2,\"v\":\"two\"}", "{\"id\":1,\"v\":\"two\"}", block: 0x50, seq: 1);
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":\"two\"}", "{\"id\":1,\"v\":\"2\"}", block: 0x51);
        Change("u", "{\"id\":2}", "{\"id\":2,\"v\":\"one\"}", "{\"id\":2,\"v\":\"1\"}", block: 0x52);
        Assert.Equal((0L, 2L, 0L), (T().Differences, T().RowsInState, T().Undecided));
        // but a row that moves onto another and stays there: that one was deleted without a word
        Change("u", "{\"id\":2}", "{\"id\":1,\"v\":\"2\"}", "{\"id\":2,\"v\":\"2\"}", block: 0x53);
        Assert.Equal((0L, 1L), (T().Differences, T().Undecided)); // not before its transaction is over
        Change("c", "{\"id\":9}", null, "{\"id\":9,\"v\":\"\"}", block: 0x54);
        Assert.Equal((1L, 0L, 1L), (T().Differences, T().Undecided, T().ByKind[DifferenceKind.RowAlreadyThere]));
    }

    [Fact]
    public void A_key_too_long_to_keep_still_tells_its_rows_apart()
    {
        var (a, b) = (new string('k', 5000) + "a", new string('k', 5000) + "b");
        Change("c", $"{{\"id\":\"{a}\"}}", null, $"{{\"id\":\"{a}\",\"v\":1}}");
        Change("c", $"{{\"id\":\"{b}\"}}", null, $"{{\"id\":\"{b}\",\"v\":2}}");
        Change("u", $"{{\"id\":\"{a}\"}}", $"{{\"id\":\"{a}\",\"v\":2}}", $"{{\"id\":\"{a}\",\"v\":3}}");
        Assert.Equal((1L, 2L), (T().Differences, T().RowsInState));
        Assert.Contains("too long", T().Examples.Single().Key, StringComparison.Ordinal);
    }

    /// <summary>A missing delete that the last transaction of the stream shows is said when a load's row or end comes, not only with the next
    /// change (which may never come).</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_row_of_a_load_and_the_end_of_one_end_the_transaction_before(bool row)
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":1}");
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":2}");
        Marker("begin", table: "other");
        Assert.Equal((0L, 1L), (T().Differences, T().Undecided)); // a begin may come in the middle of a transaction
        if (row) Row("{\"id\":5}", "{\"id\":5}", table: "other");
        else Marker("end", rows: 0, table: "other");
        Assert.Equal((1L, 0L), (T().ByKind[DifferenceKind.RowAlreadyThere], T().Undecided));
    }

    [Fact]
    public void The_report_as_JSON_says_the_same()
    {
        Change("c", "{\"id\":1}", null, "{\"id\":1,\"v\":10}");
        Change("u", "{\"id\":1}", "{\"id\":1,\"v\":20}", "{\"id\":1,\"v\":30}");
        Add("garbage");
        using var json = JsonDocument.Parse(_v.Report().ToJson());
        var root = json.RootElement;
        Assert.Equal((false, 3, 1, 1, 1), (root.GetProperty("clean").GetBoolean(), root.GetProperty("messages").GetInt32(), root.GetProperty("differences").GetInt32(), root.GetProperty("unreadable").GetInt32(), root.GetProperty("different").GetInt32()));
        var table = root.GetProperty("tables")[0];
        Assert.Equal(("different", "t", 1, 1), (table.GetProperty("verdict").GetString(), table.GetProperty("table").GetString(), table.GetProperty("rows").GetInt32(), table.GetProperty("by_kind").GetProperty("BeforeImage").GetInt32()));
        var example = table.GetProperty("examples")[0];
        Assert.Equal(("{\"id\":1}", "v", "10", "20", 2), (example.GetProperty("key").GetString(), example.GetProperty("columns")[0].GetProperty("column").GetString(),
            example.GetProperty("columns")[0].GetProperty("expected").GetString(), example.GetProperty("columns")[0].GetProperty("got").GetString(), example.GetProperty("sequence").GetInt32()));
    }

    [Fact]
    public void Differences_are_told_as_they_are_found_and_examples_are_limited()
    {
        var told = new List<Difference>();
        var v = new StreamVerifier(new VerifierOptions { Examples = 2 }) { OnDifference = told.Add };
        for (var i = 0; i < 6; i++)
            v.Add("s", Encoding.UTF8.GetBytes($"{{\"op\":\"u\",\"key\":{{\"id\":1}},\"before\":{{\"id\":1,\"v\":{i * 2}}},\"after\":{{\"id\":1,\"v\":{i * 2 + 1}}},{Source("t", "db", "i1", $",\"commit_lsn\":\"0000002A:{i + 1:X8}:0001\",\"seq\":0")}}}"));
        Assert.Equal((5, 5L, 2), (told.Count, v.Differences, v.Report().Tables[0].Examples.Count));
    }

    // ---- the parts ------------------------------------------------------------------------------------------------------------------

    /// <summary>Any UTF-16 text, well formed or not, spelled with any mixture of escapes, is the same canonical bytes; and those are the text.</summary>
    [HegelTest(TestCases = 2000)]
    public void A_string_is_what_its_escapes_mean(TestCase tc)
    {
        var units = tc.Draw(Generators.Lists(Generators.SampledFrom(new[] { 'a', '"', '\\', '/', '\n', '\u0001', 'å', '\u07ff', '\u0800', '日', '\ud83d', '\ude00', '\udbff', '\udfff', '\ufffd', '\uffff' })).MaxSize(12), "text");
        var text = new string([.. units]);
        string Spell()
        {
            var sb = new StringBuilder("\"");
            for (var i = 0; i < text.Length; i++)
            {
                var c = text[i];
                var pair = char.IsHighSurrogate(c) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]);
                var escape = c is '"' or '\\' || c < ' ' || (char.IsSurrogate(c) && !pair) || tc.Draw(Generators.Booleans(), "escaped");
                if (pair && !escape) { sb.Append(c).Append(text[++i]); continue; }
                if (!escape) sb.Append(c);
                else if (c == '\n' && tc.Draw(Generators.Booleans(), "short")) sb.Append("\\n");
                else if (c is '"' or '\\' or '/' && tc.Draw(Generators.Booleans(), "short")) sb.Append('\\').Append(c);
                else sb.Append("\\u").Append(((int)c).ToString(tc.Draw(Generators.Booleans(), "upper") ? "X4" : "x4", CultureInfo.InvariantCulture));
            }
            return sb.Append('"').ToString();
        }
        byte[] Canonical(string json)
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            var r = Canon.Reader(bytes);
            r.Read();
            var scratch = new byte[4];
            return Canon.Text(ref r, ref scratch).ToArray();
        }
        var (one, two) = (Canonical(Spell()), Canonical(Spell()));
        Assert.Equal(one, two);
        Assert.Equal(text, Canon.Decode(one));
        // and it is UTF-8 wherever the text is well formed
        if (!text.Any(char.IsSurrogate)) Assert.Equal(Encoding.UTF8.GetBytes(text), one);
    }

    /// <summary>The row store against a dictionary: rows of any size set, replaced by others of another size, removed, swept.</summary>
    [HegelTest(TestCases = 1500)]
    public void The_row_store_is_a_dictionary(TestCase tc)
    {
        var store = new RowStore();
        var model = new Dictionary<string, (int Shape, uint Stamp, byte[] Cells)>(StringComparer.Ordinal);
        var tape = StreamVerifierProperties.DrawTape(tc, 600, "tape");
        var wide = tape.Chance(3) ? 3000 : 3; // enough to fill arrays and have them reclaimed
        var written = 0;
        while (!tape.Empty)
        {
            var name = "k" + tape.Next(tape.Chance(3) ? 400 : 12) + new string('x', tape.Next(3) * 7);
            var key = Encoding.UTF8.GetBytes(name);
            var hash = RowStore.Hash(key);
            switch (tape.Next(8))
            {
                case 0 or 1:
                    Assert.Equal(model.Remove(name), store.Remove(key, hash));
                    break;
                case 2:
                    for (var slot = 0; slot < store.Slots; slot++)
                        if (store.AddressAt(slot) is >= 0 and var a && store.Stamp(a) % 3 == 0) { Assert.True(model.Remove(Encoding.UTF8.GetString(store.Key(a)))); store.RemoveAt(slot); }
                    store.Trim();
                    break;
                default:
                    var cells = new byte[tape.Next(wide) * Canon.Cell];
                    for (var i = 0; i < cells.Length; i += 97) cells[i] = (byte)(i + ++written); // every row is its own
                    var (shape, stamp) = (tape.Next(1000), (uint)tape.Next(1000));
                    store.Put(key, hash, shape, stamp, cells);
                    model[name] = (shape, stamp, cells);
                    break;
            }
            Assert.Equal(model.Count, store.Count);
        }
        foreach (var (name, row) in model)
        {
            var key = Encoding.UTF8.GetBytes(name);
            var a = store.Find(key, RowStore.Hash(key));
            Assert.True(a >= 0, name);
            Assert.Equal((row.Shape, row.Stamp), (store.Shape(a), store.Stamp(a)));
            Assert.True(store.Key(a).SequenceEqual(key) && store.Cells(a).SequenceEqual(row.Cells), name);
        }
        var seen = 0;
        for (var slot = 0; slot < store.Slots; slot++) if (store.AddressAt(slot) >= 0) seen++;
        Assert.Equal(model.Count, seen);
        Assert.Equal(-1, store.Find("nobody"u8, RowStore.Hash("nobody"u8)));
        // what is held is in proportion to what is alive
        var alive = model.Sum(r => 12L + Encoding.UTF8.GetByteCount(r.Key) + r.Value.Cells.Length);
        Assert.True(store.Bytes <= 4 * alive + (1 << 17) + 64L * model.Count + 4096, $"{store.Bytes} bytes held for {alive} alive in {model.Count} rows");
    }

    /// <summary>A million rows of a narrow table (an int key, four columns): the memory they take and how fast they come. Numbers go to
    /// perf.txt beside the tests.</summary>
    [Fact]
    public void A_million_narrow_rows_take_less_than_200_bytes_each()
    {
        const int Rows = 1_000_000;
        static byte[] Message(string op, int id, int version, int n) => Encoding.UTF8.GetBytes(
            $"{{\"op\":\"{op}\",\"key\":{{\"id\":{id}}},\"before\":{(op == "u" ? Image(id, version - 1) : "null")},\"after\":{Image(id, version)},\"source\":{{\"server\":\"srv\",\"db\":\"db\",\"incarnation\":\"06E43102AAAA\",\"schema\":\"dbo\",\"table\":\"orders\",\"lsn\":\"0000002A:{n:X8}:0001\",{(op == "r" ? "\"snapshot\":\"L1\"" : $"\"commit_lsn\":\"0000002A:{n:X8}:0001\",\"xid\":\"0000:0000002a\",\"seq\":0,\"txn_size\":1")}}},\"ts_ms\":1795600000000}}");
        static string Image(int id, int version) => $"{{\"id\":{id},\"customer\":{id % 50021},\"status\":\"{(version % 2 == 0 ? "open" : "shipped")}\",\"amount\":\"{id % 1000}.{version % 100:D2}\",\"placed\":\"2026-11-25T10:{id % 60:D2}:{version % 60:D2}.0000000\"}}";

        var loads = Enumerable.Range(0, Rows).Select(i => Message("r", i, 0, 1)).ToList();
        var updates = Enumerable.Range(0, Rows).Select(i => Message("u", i * 7 % Rows, 1, i + 2)).ToList();
        var bytes = loads.Sum(m => (long)m.Length) + updates.Sum(m => (long)m.Length);
        GC.Collect();
        var before = GC.GetTotalMemory(forceFullCollection: true);
        var v = new StreamVerifier();
        var clock = Stopwatch.StartNew();
        foreach (var m in loads) v.Add("cdc.srv.db.dbo.orders", m);
        var loaded = clock.Elapsed;
        foreach (var m in updates) v.Add("cdc.srv.db.dbo.orders", m);
        var all = clock.Elapsed;
        var held = GC.GetTotalMemory(forceFullCollection: true) - before;
        var report = v.Report();
        GC.KeepAlive(v);
        GC.KeepAlive(loads);
        GC.KeepAlive(updates);
        Assert.Equal((0L, (long)Rows, 2L * Rows), (report.Differences, report.Rows, report.Messages));
        var line = string.Create(CultureInfo.InvariantCulture, $"{Rows} rows of 5 columns: {held / (double)Rows:F1} bytes per row on the heap ({report.StateBytes / (double)Rows:F1} in the row store); {Rows / loaded.TotalSeconds:F0} load rows/s, {Rows / (all - loaded).TotalSeconds:F0} updates/s, {bytes / all.TotalSeconds / 1e6:F0} MB/s of JSON ({bytes / (2 * Rows)} bytes per message)");
        File.WriteAllText(Path.Combine(AppContext.BaseDirectory, "perf.txt"), line);
        Assert.True(held < 200L * Rows, line);
        Assert.True(report.StateBytes < 200L * Rows, line);
    }
}
