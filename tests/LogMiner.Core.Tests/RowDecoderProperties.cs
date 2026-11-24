using System.Buffers.Binary;
using System.Text;
using Hegel;
using Hegel.Xunit;
using LogMiner.Rows;
using LogMiner.Schema;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>
/// Round-trip properties for <see cref="RowDecoder"/> against an independent encoder of the FixedVar record format.
/// The real-engine oracle (rows written by SQL Server itself) lives in the integration tests.
/// </summary>
public class RowDecoderProperties
{
    private static readonly SqlType[] Types =
    [
        SqlType.TinyInt, SqlType.SmallInt, SqlType.Int, SqlType.BigInt, SqlType.Bit, SqlType.Decimal, SqlType.Money, SqlType.SmallMoney,
        SqlType.Float, SqlType.Real, SqlType.Date, SqlType.Time, SqlType.DateTime2, SqlType.DateTimeOffset, SqlType.DateTime, SqlType.SmallDateTime,
        SqlType.Char, SqlType.VarChar, SqlType.NChar, SqlType.NVarChar, SqlType.Binary, SqlType.VarBinary, SqlType.UniqueIdentifier,
    ];

    private sealed record Spec(SqlType Type, short Length, byte Precision, byte Scale, bool Nullable);

    private static readonly Generator<Spec> Specs = Generators.Composite(tc =>
    {
        var type = tc.Draw(Generators.SampledFrom(Types), "type");
        var nullable = tc.Draw(Generators.Booleans(), "nullable");
        byte precision = 0, scale = 0;
        short length;
        switch (type)
        {
            case SqlType.Decimal:
                precision = (byte)tc.Draw(Generators.Integers().Min(1).Max(38), "precision");
                scale = (byte)tc.Draw(Generators.Integers().Min(0).Max(precision), "scale");
                length = (short)ValueDecoder.DecimalLength(precision);
                break;
            case SqlType.Time or SqlType.DateTime2 or SqlType.DateTimeOffset:
                scale = (byte)tc.Draw(Generators.Integers().Min(0).Max(7), "scale");
                length = (short)(ValueDecoder.TimeLength(scale) + type switch { SqlType.Time => 0, SqlType.DateTime2 => 3, _ => 5 });
                break;
            case SqlType.Char or SqlType.VarChar or SqlType.Binary or SqlType.VarBinary:
                length = (short)tc.Draw(Generators.Integers().Min(1).Max(300), "length");
                break;
            case SqlType.NChar or SqlType.NVarChar:
                length = (short)(2 * tc.Draw(Generators.Integers().Min(1).Max(150), "chars"));
                break;
            default:
                length = (short)FixedLength(type);
                break;
        }
        return new Spec(type, length, precision, scale, nullable);
    });

    private static int FixedLength(SqlType t) => t switch
    {
        SqlType.TinyInt or SqlType.Bit => 1, SqlType.SmallInt => 2, SqlType.Int or SqlType.Real or SqlType.SmallMoney or SqlType.SmallDateTime => 4,
        SqlType.BigInt or SqlType.Float or SqlType.Money or SqlType.DateTime => 8, SqlType.Date => 3, SqlType.UniqueIdentifier => 16, _ => 0,
    };

    private static bool IsVariable(SqlType t) => t is SqlType.VarChar or SqlType.NVarChar or SqlType.VarBinary;

    /// <summary>Assigns physical positions the way SQL Server does for appended columns: fixed columns packed from offset 4, bit columns share bytes, variable columns numbered 1..n.</summary>
    private static TableLayout BuildLayout(IReadOnlyList<Spec> specs, IReadOnlyList<object?> defaults)
    {
        var cols = new List<ColumnLayout>();
        short fixedPos = 4, varIndex = 0;
        var bitByte = -1;
        var bitPos = 8;
        for (var i = 0; i < specs.Count; i++)
        {
            var s = specs[i];
            short leaf;
            byte bit = 0;
            if (s.Type == SqlType.Bit)
            {
                if (bitPos == 8) { bitByte = fixedPos; fixedPos += 1; bitPos = 0; }
                leaf = (short)bitByte;
                bit = (byte)bitPos++;
            }
            else if (IsVariable(s.Type)) leaf = (short)-(++varIndex);
            else { leaf = fixedPos; fixedPos += s.Length; }
            cols.Add(new ColumnLayout
            {
                ColumnId = i + 1, PartitionColumnId = i + 1, Name = $"c{i + 1}", Type = s.Type, MaxLength = s.Length, MaxInRowLength = s.Length,
                Precision = s.Precision, Scale = s.Scale, CodePage = 1252, LeafOffset = leaf, LeafNullBit = (short)(i + 1), LeafBitPosition = bit,
                IsNullable = s.Nullable, KeyOrdinal = i == 0 ? 1 : 0, DefaultValue = defaults[i],
            });
        }
        return new TableLayout(5, "db", 1, "dbo", "t", 1, DataCompression.None, cols);
    }

    /// <summary>Draws a value for a column and returns (expected decoded value, stored bytes).</summary>
    private static (object Value, byte[] Bytes) DrawValue(TestCase tc, ColumnLayout c, string label)
    {
        switch (c.Type)
        {
            case SqlType.TinyInt: { var v = (byte)tc.Draw(Generators.Integers().Min(0).Max(255), label); return (v, [v]); }
            case SqlType.SmallInt: { var v = (short)tc.Draw(Generators.Integers().Min(short.MinValue).Max(short.MaxValue), label); var b = new byte[2]; BinaryPrimitives.WriteInt16LittleEndian(b, v); return (v, b); }
            case SqlType.Int: { var v = tc.Draw(Generators.Integers(), label); var b = new byte[4]; BinaryPrimitives.WriteInt32LittleEndian(b, v); return (v, b); }
            case SqlType.BigInt: { var v = tc.Draw(Generators.Longs(), label); var b = new byte[8]; BinaryPrimitives.WriteInt64LittleEndian(b, v); return (v, b); }
            case SqlType.Real: { var v = (float)tc.Draw(Generators.Doubles().Min(-1e30).Max(1e30).AllowNan(false), label); var b = new byte[4]; BinaryPrimitives.WriteSingleLittleEndian(b, v); return (v, b); }
            case SqlType.Float: { var v = tc.Draw(Generators.Doubles().AllowNan(false).AllowInfinity(false), label); var b = new byte[8]; BinaryPrimitives.WriteDoubleLittleEndian(b, v); return (v, b); }
            case SqlType.Decimal:
            {
                var maxDigits = Math.Min((int)c.Precision, 28);
                var mantissa = tc.Draw(Generators.Longs().Min(0).Max(maxDigits >= 18 ? long.MaxValue : (long)Math.Pow(10, maxDigits) - 1), label);
                var negative = mantissa != 0 && tc.Draw(Generators.Booleans(), label + ".neg");
                var b = new byte[ValueDecoder.DecimalLength(c.Precision)];
                b[0] = negative ? (byte)0 : (byte)1;
                Span<byte> mag = stackalloc byte[16];
                BinaryPrimitives.WriteUInt64LittleEndian(mag, (ulong)mantissa);
                mag[..(b.Length - 1)].CopyTo(b.AsSpan(1));
                if (c.Scale <= 28)
                    return (new decimal((int)(uint)mantissa, (int)(uint)((ulong)mantissa >> 32), 0, negative, c.Scale), b);
                var digits = mantissa.ToString(System.Globalization.CultureInfo.InvariantCulture).PadLeft(c.Scale + 1, '0');
                return ((negative ? "-" : "") + digits[..^c.Scale] + "." + digits[^c.Scale..], b);
            }
            case SqlType.Money: { var units = tc.Draw(Generators.Longs(), label); var b = new byte[8]; BinaryPrimitives.WriteInt64LittleEndian(b, units); return (units / 10000m, b); }
            case SqlType.SmallMoney: { var units = tc.Draw(Generators.Integers(), label); var b = new byte[4]; BinaryPrimitives.WriteInt32LittleEndian(b, units); return (units / 10000m, b); }
            case SqlType.Date: { var day = tc.Draw(Generators.Integers().Min(0).Max(DateOnly.MaxValue.DayNumber), label); return (DateOnly.FromDayNumber(day), U24(day)); }
            case SqlType.Time: { var (ticks, b) = DrawTime(tc, c.Scale, label); return (TimeSpan.FromTicks(ticks), b); }
            case SqlType.DateTime2:
            {
                var (ticks, tb) = DrawTime(tc, c.Scale, label);
                var day = tc.Draw(Generators.Integers().Min(0).Max(DateOnly.MaxValue.DayNumber), label + ".day");
                return (DateOnly.FromDayNumber(day).ToDateTime(TimeOnly.MinValue).AddTicks(ticks), [.. tb, .. U24(day)]);
            }
            case SqlType.DateTimeOffset:
            {
                var (ticks, tb) = DrawTime(tc, c.Scale, label);
                var day = tc.Draw(Generators.Integers().Min(1).Max(DateOnly.MaxValue.DayNumber - 1), label + ".day");
                var offset = (short)tc.Draw(Generators.Integers().Min(-840).Max(840), label + ".offset");
                var utc = DateOnly.FromDayNumber(day).ToDateTime(TimeOnly.MinValue).AddTicks(ticks);
                var ob = new byte[2]; BinaryPrimitives.WriteInt16LittleEndian(ob, offset);
                var o = TimeSpan.FromMinutes(offset);
                return (new DateTimeOffset(utc + o, o), [.. tb, .. U24(day), .. ob]);
            }
            case SqlType.DateTime:
            {
                var days = tc.Draw(Generators.Integers().Min(-53690).Max(2958463), label);
                var t300 = tc.Draw(Generators.Integers().Min(0).Max(25919999), label + ".t");
                var b = new byte[8]; BinaryPrimitives.WriteInt32LittleEndian(b, t300); BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(4), days);
                var ms = (long)Math.Round(t300 * 10.0 / 3.0, MidpointRounding.AwayFromZero);
                return (new DateTime(1900, 1, 1).AddDays(days).AddMilliseconds(ms), b);
            }
            case SqlType.SmallDateTime:
            {
                var days = (ushort)tc.Draw(Generators.Integers().Min(0).Max(65535), label);
                var minutes = (ushort)tc.Draw(Generators.Integers().Min(0).Max(1439), label + ".m");
                var b = new byte[4]; BinaryPrimitives.WriteUInt16LittleEndian(b, minutes); BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(2), days);
                return (new DateTime(1900, 1, 1).AddDays(days).AddMinutes(minutes), b);
            }
            case SqlType.Char or SqlType.VarChar:
            {
                var maxLen = c.MaxLength;
                var s = tc.Draw(Generators.Text().Codepoints(0x20, 0x7E).MaxSize(maxLen), label);
                if (c.Type == SqlType.Char) s = s.PadRight(maxLen);
                return (s, Encoding.Latin1.GetBytes(s));
            }
            case SqlType.NChar or SqlType.NVarChar:
            {
                var maxChars = c.MaxLength / 2;
                var s = tc.Draw(Generators.Text().Codepoints(0x20, 0xD7FF).MaxSize(maxChars), label);
                if (c.Type == SqlType.NChar) s = s.PadRight(maxChars);
                return (s, Encoding.Unicode.GetBytes(s));
            }
            case SqlType.Binary or SqlType.VarBinary:
            {
                var b = tc.Draw(Generators.Binary().MinSize(c.Type == SqlType.Binary ? c.MaxLength : 0).MaxSize(c.MaxLength), label);
                return (b, b);
            }
            case SqlType.UniqueIdentifier: { var g = tc.Draw(Generators.Uuids(), label); return (g, g.ToByteArray()); }
            default: throw new InvalidOperationException(c.Type.ToString());
        }
    }

    private static (long Ticks, byte[] Bytes) DrawTime(TestCase tc, byte scale, string label)
    {
        long unitsPerDay = 864_000_000_000L;
        for (var s = scale; s < 7; s++) unitsPerDay /= 10;
        var units = tc.Draw(Generators.Longs().Min(0).Max(unitsPerDay - 1), label);
        var len = ValueDecoder.TimeLength(scale);
        var b = new byte[len];
        for (var i = 0; i < len; i++) b[i] = (byte)(units >> (8 * i));
        var ticks = units;
        for (var s = scale; s < 7; s++) ticks *= 10;
        return (ticks, b);
    }

    private static byte[] U24(int v) => [(byte)v, (byte)(v >> 8), (byte)(v >> 16)];

    /// <summary>Encodes a row containing the first <paramref name="present"/> columns of <paramref name="layout"/>.</summary>
    private static byte[] Encode(TableLayout layout, int present, object?[] values, byte[]?[] stored, bool trimTrailingNullVars)
    {
        var cols = layout.Columns.Take(present).ToArray();
        var fixedEnd = 4;
        foreach (var c in cols.Where(c => c.IsFixed || c.Type == SqlType.Bit))
            fixedEnd = Math.Max(fixedEnd, c.LeafOffset + (c.Type == SqlType.Bit ? 1 : c.MaxInRowLength));
        var vars = cols.Where(c => !c.IsFixed && c.Type != SqlType.Bit).OrderBy(c => -c.LeafOffset).ToList();
        var varCount = vars.Count;
        if (trimTrailingNullVars) // SQL Server drops trailing zero-length variable columns (NULL or empty)
            while (varCount > 0 && (stored[vars[varCount - 1].ColumnId - 1]?.Length ?? 0) == 0) varCount--;

        var row = new List<byte>();
        var statusA = (byte)(0x10 | (varCount > 0 ? 0x20 : 0));
        row.AddRange([statusA, 0, (byte)fixedEnd, (byte)(fixedEnd >> 8)]);
        row.AddRange(new byte[fixedEnd - 4]);
        var bitmap = new byte[(present + 7) / 8];
        foreach (var c in cols)
        {
            var i = c.ColumnId - 1;
            if (values[i] is null) { bitmap[i / 8] |= (byte)(1 << (i % 8)); continue; }
            if (c.Type == SqlType.Bit) { if ((bool)values[i]!) row[c.LeafOffset] |= (byte)(1 << c.LeafBitPosition); }
            else if (c.IsFixed) for (var k = 0; k < stored[i]!.Length; k++) row[c.LeafOffset + k] = stored[i]![k];
        }
        row.AddRange([(byte)present, (byte)(present >> 8)]);
        row.AddRange(bitmap);
        if (varCount > 0)
        {
            row.AddRange([(byte)varCount, (byte)(varCount >> 8)]);
            var offsetsAt = row.Count;
            row.AddRange(new byte[2 * varCount]);
            for (var v = 0; v < varCount; v++)
            {
                var data = stored[vars[v].ColumnId - 1] ?? [];
                row.AddRange(data);
                row[offsetsAt + 2 * v] = (byte)row.Count;
                row[offsetsAt + 2 * v + 1] = (byte)(row.Count >> 8);
            }
        }
        return row.ToArray();
    }

    [HegelTest(TestCases = 300)]
    public void Decodes_what_the_format_encodes(TestCase tc)
    {
        var specs = tc.Draw(Generators.Lists(Specs).MinSize(1).MaxSize(14), "specs");
        // a suffix of columns may have been added after the row was written (metadata-only ALTER TABLE ADD)
        var present = tc.Draw(Generators.Integers().Min(1).Max(specs.Count), "present");
        var defaults = new object?[specs.Count];
        var probe = BuildLayout(specs, defaults);
        for (var i = present; i < specs.Count; i++)
            if (probe.Columns[i].Type != SqlType.Bit && tc.Draw(Generators.Booleans(), $"default{i}"))
                defaults[i] = DrawValue(tc, probe.Columns[i], $"defaultValue{i}").Value;
        var layout = BuildLayout(specs, defaults);

        var values = new object?[specs.Count];
        var stored = new byte[]?[specs.Count];
        for (var i = 0; i < present; i++)
        {
            var c = layout.Columns[i];
            if (c.IsNullable && tc.Draw(Generators.Booleans(), $"null{i}")) continue;
            if (c.Type == SqlType.Bit) { values[i] = tc.Draw(Generators.Booleans(), $"bit{i}"); continue; }
            (values[i], stored[i]) = DrawValue(tc, c, $"v{i}");
        }
        var trim = tc.Draw(Generators.Booleans(), "trimTrailingNullVars");
        var record = Encode(layout, present, values, stored, trim);

        // a column that cannot be NULL, is not in the row, and has no known default: the layout is not the row's
        if (Enumerable.Range(present, specs.Count - present).Any(i => !layout.Columns[i].IsNullable && defaults[i] is null))
        {
            Assert.Throws<SchemaMismatchException>(() => RowDecoder.Decode(record, layout));
            return;
        }
        var image = RowDecoder.Decode(record, layout);

        for (var i = 0; i < specs.Count; i++)
        {
            var expected = i < present ? values[i] : defaults[i];
            Assert.Equal(expected, image.Values[i]);
        }
        Assert.Equal(record.Length, RowDecoder.DeclaredLength(record));

        // rows may be followed by version information, and log records cut them off: a value is then right or missing, never another one
        var tail = tc.Draw(Generators.Binary().MaxSize(20), "tail");
        Assert.Equal(image.Values, RowDecoder.Decode([.. record, .. tail], layout).Values);
        var cut = RowDecoder.Decode(record.AsSpan(0, tc.Draw(Generators.Integers().Min(4).Max(record.Length), "cutAt")), layout, possiblyTruncated: true);
        for (var i = 0; i < specs.Count; i++)
            if (cut.Values[i] is not Unavailable) Assert.Equal(image.Values[i], cut.Values[i]);
    }

    /// <summary>One damaged byte of the row's structure: the same row, or refused - never a different one that looks right. (Values have no
    /// checksum: damage there cannot be told.)</summary>
    [HegelTest(TestCases = 500)]
    public void A_value_outside_its_type_is_refused(TestCase tc)
    {
        var (type, scale, precision, bad) = tc.Draw(Generators.SampledFrom(new (SqlType, byte, byte, byte[])[]
        {
            (SqlType.Time, 7, 0, [0x00, 0xC0, 0x69, 0x2A, 0xC9]),                        // 24:00
            (SqlType.Time, 0, 0, [0x80, 0x51, 0x01]),
            (SqlType.Date, 0, 0, [0xDB, 0xB9, 0x37]),                                    // the day after 9999-12-31
            (SqlType.DateTime, 0, 0, [0x00, 0x82, 0x8B, 0x01, 0x00, 0x00, 0x00, 0x00]),  // 25,920,000 ticks
            (SqlType.DateTime, 0, 0, [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]),
            (SqlType.DateTime, 0, 0, [0x00, 0x00, 0x00, 0x00, 0x45, 0x2E, 0xFF, 0xFF]),  // before 1753
            (SqlType.SmallDateTime, 0, 0, [0xA0, 0x05, 0x00, 0x00]),                     // 1440 minutes
            (SqlType.Decimal, 2, 5, [0x02, 0x01, 0x00, 0x00, 0x00]),                     // sign
            (SqlType.Decimal, 2, 5, [0x01, 0xA0, 0x86, 0x01, 0x00]),                     // 100,000 in 5 digits
            (SqlType.DateTimeOffset, 0, 0, [0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x49, 0x03]), // +14:01
            (SqlType.DateTimeOffset, 0, 0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC4, 0xFF]), // an hour before 0001-01-01
        }), "bad");
        var column = new ColumnLayout { ColumnId = 1, PartitionColumnId = 1, Name = "c", Type = type, MaxLength = (short)bad.Length, MaxInRowLength = (short)bad.Length, Precision = precision, Scale = scale, LeafOffset = 4, LeafNullBit = 1 };
        Assert.Throws<RowFormatException>(() => ValueDecoder.Decode(bad, column));
        Assert.Throws<RowFormatException>(() => ValueDecoder.Decode(bad.Append((byte)0).ToArray(), column));
        Assert.Throws<RowFormatException>(() => ValueDecoder.Decode(bad.AsSpan(1), column));
    }

    [HegelTest(TestCases = 500)]
    public void Arbitrary_bytes_fail_only_with_decoder_exceptions(TestCase tc)
    {
        var specs = tc.Draw(Generators.Lists(Specs).MinSize(1).MaxSize(8), "specs");
        var layout = BuildLayout(specs, new object?[specs.Count]);
        var bytes = tc.Draw(Generators.Binary().MaxSize(200), "bytes");
        var truncated = tc.Draw(Generators.Booleans(), "truncated");
        try { RowDecoder.Decode(bytes, layout, truncated); }
        catch (RowFormatException) { }
        catch (SchemaMismatchException) { }
        _ = RowDecoder.DeclaredLength(bytes);
    }

    [Fact]
    public void Decodes_the_t_cdc_insert_captured_in_experiment_01()
    {
        // dbo.t_cdc (id int PK, a int, b varchar(50), c nvarchar(100), d datetime2(3), e decimal(18,4), f varbinary(100))
        var layout = new TableLayout(5, "lab_a", 1, "dbo", "t_cdc", 1, DataCompression.None,
        [
            Col(1, "id", SqlType.Int, 4, 4, 1, key: 1),
            Col(2, "a", SqlType.Int, 4, 8, 2),
            Col(3, "b", SqlType.VarChar, 50, -1, 3),
            Col(4, "c", SqlType.NVarChar, 200, -2, 4),
            Col(5, "d", SqlType.DateTime2, 7, 12, 5, scale: 3),
            Col(6, "e", SqlType.Decimal, 9, 19, 6, precision: 18, scale: 4),
            Col(7, "f", SqlType.VarBinary, 100, -3, 7),
        ]);
        var row = Convert.FromHexString("30001C00010000000A0000007B512502664A0B0140E201000000000007000003002C0036003A0068656C6C6F7700F60072006C006400DEADBEEF");
        var image = RowDecoder.Decode(row, layout);
        Assert.Equal(1, image["id"]);
        Assert.Equal(10, image["a"]);
        Assert.Equal("hello", image["b"]);
        Assert.Equal("wörld", image["c"]);
        Assert.Equal(new DateTime(2026, 11, 23, 10, 0, 0, 123), image["d"]);
        Assert.Equal(12.3456m, image["e"]);
        Assert.Equal(Convert.FromHexString("DEADBEEF"), image["f"]);
    }

    private static ColumnLayout Col(int id, string name, SqlType t, short len, short leaf, short nullBit, int key = 0, byte precision = 0, byte scale = 0) => new()
    {
        ColumnId = id, PartitionColumnId = id, Name = name, Type = t, MaxLength = len, MaxInRowLength = len, LeafOffset = leaf, LeafNullBit = nullBit,
        KeyOrdinal = key, Precision = precision, Scale = scale, CodePage = 1252, IsNullable = key == 0,
    };
}
