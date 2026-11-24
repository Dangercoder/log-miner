using Hegel;
using Hegel.Xunit;
using Xunit;

namespace LogMiner.Core.Tests;

public class LsnProperties
{
    private static readonly Generator<Lsn> Lsns = Generators.Composite(tc => new Lsn(
        (uint)tc.Draw(Generators.Longs().Min(0).Max(uint.MaxValue), "vlf"),
        (uint)tc.Draw(Generators.Longs().Min(0).Max(uint.MaxValue), "block"),
        (ushort)tc.Draw(Generators.Integers().Min(0).Max(ushort.MaxValue), "slot")));

    [HegelTest]
    public void Text_form_round_trips(TestCase tc)
    {
        var lsn = tc.Draw(Lsns, "lsn");
        Assert.Equal(lsn, Lsn.Parse(lsn.ToString()));
        Assert.Equal(lsn, Lsn.Parse("0x" + lsn));
    }

    [HegelTest]
    public void Binary_form_round_trips(TestCase tc)
    {
        var lsn = tc.Draw(Lsns, "lsn");
        Assert.Equal(lsn, Lsn.FromBinary(lsn.ToBinary()));
    }

    [HegelTest]
    public void Ordering_matches_big_endian_binary_ordering(TestCase tc)
    {
        var a = tc.Draw(Lsns, "a");
        var b = tc.Draw(Lsns, "b");
        var bytes = a.ToBinary().AsSpan().SequenceCompareTo(b.ToBinary());
        Assert.Equal(Math.Sign(bytes), Math.Sign(a.CompareTo(b)));
    }

    [HegelTest]
    public void Decimal_form_uses_engine_component_values(TestCase tc)
    {
        var lsn = tc.Draw(Lsns, "lsn");
        var parts = lsn.ToDecimalString().Split(':');
        Assert.Equal(lsn.Vlf, uint.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(lsn.Block, uint.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(lsn.Slot, ushort.Parse(parts[2], System.Globalization.CultureInfo.InvariantCulture));
    }

    [HegelTest]
    public void Checkpoints_round_trip_through_their_text_form(TestCase tc)
    {
        var cp = new LogMiner.Pipeline.Checkpoint("db", "0011-2026", tc.Draw(Lsns, "restart"), tc.Draw(Lsns, "commit"), tc.Draw(Lsns, "begin"),
            DateTimeOffset.FromUnixTimeMilliseconds(tc.Draw(Generators.Longs().Min(0).Max(4_000_000_000_000), "at")), tc.Draw(Generators.Booleans(), "held"),
            tc.Draw(Lsns, "resumed"), tc.Draw(Generators.Booleans(), "in gap"));
        var text = cp.Serialize();
        Assert.Equal(cp, LogMiner.Pipeline.Checkpoint.Deserialize("db", text));
        // files of earlier versions: without ResumedAt, and without Held
        var parts = text.Split('|');
        Assert.Equal(cp with { ResumedAt = default, ResumedInGap = false }, LogMiner.Pipeline.Checkpoint.Deserialize("db", string.Join('|', parts[..6])));
        Assert.Equal(cp with { ResumedAt = default, ResumedInGap = false, Held = false }, LogMiner.Pipeline.Checkpoint.Deserialize("db", string.Join('|', parts[..5])));
        Assert.Throws<FormatException>(() => LogMiner.Pipeline.Checkpoint.Deserialize("db", string.Join('|', parts[..7])));
    }

    [Fact]
    public void Parses_the_fn_dblog_sample_from_the_experiments()
    {
        var lsn = Lsn.Parse("0000002A:000006C8:0003");
        Assert.Equal(new Lsn(0x2A, 0x6C8, 3), lsn);
        Assert.Equal("42:1736:3", lsn.ToDecimalString());
        Assert.Equal(Convert.FromHexString("0000002A000006C80003"), lsn.ToBinary());
    }
}
