using System.Buffers.Binary;
using System.Globalization;
using System.IO.Hashing;
using System.Text;
using System.Text.Json;

namespace LogMiner.Verify;

/// <summary>What a JSON value is. Part of what is compared: 1 and "1" are different values.</summary>
internal enum ValueKind : byte { Unknown = 0, Null = 1, False = 2, True = 3, Number = 4, String = 5, Object = 6, Array = 7 }

/// <summary>
/// JSON values in the form they are kept and compared in. A <b>cell</b> is nine bytes: a tag (kind, and the length when the value itself fits)
/// and either the value's canonical bytes (up to eight: most numbers, short strings - exact, and readable in a report) or a 64-bit hash of them.
/// Canonical: a number is its text as published (never a double: 9007199254740993 is not 9007199254740992), a string is what its escapes
/// mean, as UTF-8 that also holds half a surrogate pair (System.Text.Json refuses to decode those, so nothing here asks it for a string).
/// </summary>
internal static class Canon
{
    public const int Cell = 9;
    private static readonly JsonReaderOptions Strict = new() { CommentHandling = JsonCommentHandling.Disallow, AllowTrailingCommas = false, MaxDepth = 64 };

    public static Utf8JsonReader Reader(ReadOnlySpan<byte> json) => new(json, Strict);

    public static ValueKind KindOf(byte tag) => (ValueKind)(tag & 7);

    /// <summary>Kinds a column cannot move between without a type change: both booleans are one.</summary>
    public static int ClassOf(byte tag) => KindOf(tag) switch { ValueKind.True => (int)ValueKind.False, var k => (int)k };

    /// <summary>The reader stands on a value's first token and is left on its last. <paramref name="source"/> is what the reader reads.</summary>
    public static void WriteCell(scoped ref Utf8JsonReader r, ReadOnlySpan<byte> source, scoped Span<byte> cell, scoped ref byte[] scratch)
    {
        cell.Clear();
        switch (r.TokenType)
        {
            case JsonTokenType.Null: cell[0] = (byte)ValueKind.Null; break;
            case JsonTokenType.False: cell[0] = (byte)ValueKind.False; break;
            case JsonTokenType.True: cell[0] = (byte)ValueKind.True; break;
            case JsonTokenType.Number: Fill(cell, ValueKind.Number, r.ValueSpan); break;
            case JsonTokenType.String: Fill(cell, ValueKind.String, Text(ref r, ref scratch)); break;
            case JsonTokenType.StartObject:
            case JsonTokenType.StartArray:
                var kind = r.TokenType == JsonTokenType.StartObject ? ValueKind.Object : ValueKind.Array;
                if (kind == ValueKind.Object && IsUnavailable(r)) { r.Skip(); break; } // stays Unknown
                var start = (int)r.TokenStartIndex;
                r.Skip();
                Fill(cell, kind, source[start..(int)r.BytesConsumed]);
                break;
            default: throw new JsonException($"a value cannot start with {r.TokenType}");
        }
    }

    private static void Fill(Span<byte> cell, ValueKind kind, ReadOnlySpan<byte> canonical)
    {
        if (canonical.Length <= 8)
        {
            cell[0] = (byte)((int)kind | (canonical.Length + 1) << 3);
            canonical.CopyTo(cell[1..]);
        }
        else
        {
            cell[0] = (byte)kind;
            BinaryPrimitives.WriteUInt64LittleEndian(cell[1..], XxHash3.HashToUInt64(canonical));
        }
    }

    /// <summary>Exactly {"unavailable":true}: the value is not known (a LOB that is not in the log, an event cut down to fit). Anything else in
    /// braces is a value.</summary>
    private static bool IsUnavailable(Utf8JsonReader r) =>
        r.Read() && r.TokenType == JsonTokenType.PropertyName && r.ValueTextEquals("unavailable"u8) && r.Read() && r.TokenType == JsonTokenType.True
        && r.Read() && r.TokenType == JsonTokenType.EndObject;

    /// <summary>The text of the string or property name the reader stands on.</summary>
    public static ReadOnlySpan<byte> Text(scoped ref Utf8JsonReader r, scoped ref byte[] scratch)
    {
        if (!r.ValueIsEscaped) return r.ValueSpan;
        var raw = r.ValueSpan;
        if (scratch.Length < raw.Length) scratch = new byte[Math.Max(raw.Length, scratch.Length * 2)];
        return scratch.AsSpan(0, Unescape(raw, scratch));
    }

    /// <summary>What the escapes mean. A lone surrogate becomes its three bytes (as in WTF-8), so that it stays what it is and different halves
    /// stay different; a pair becomes the four bytes of its character, which is what the same character is when it is not escaped.</summary>
    public static int Unescape(ReadOnlySpan<byte> raw, Span<byte> to)
    {
        var n = 0;
        for (var i = 0; i < raw.Length; i++)
        {
            if (raw[i] != (byte)'\\') { to[n++] = raw[i]; continue; }
            var c = raw[++i];
            if (c != (byte)'u')
            {
                to[n++] = c switch { (byte)'b' => 8, (byte)'f' => 12, (byte)'n' => 10, (byte)'r' => 13, (byte)'t' => 9, _ => c };
                continue;
            }
            var unit = Hex4(raw.Slice(i + 1, 4));
            i += 4;
            var code = unit;
            if (char.IsHighSurrogate((char)unit) && i + 6 < raw.Length && raw[i + 1] == (byte)'\\' && raw[i + 2] == (byte)'u' && Hex4(raw.Slice(i + 3, 4)) is var low && char.IsLowSurrogate((char)low))
            {
                code = char.ConvertToUtf32((char)unit, (char)low);
                i += 6;
            }
            if (code < 0x80) to[n++] = (byte)code;
            else if (code < 0x800) { to[n++] = (byte)(0xC0 | code >> 6); to[n++] = (byte)(0x80 | code & 0x3F); }
            else if (code < 0x10000) { to[n++] = (byte)(0xE0 | code >> 12); to[n++] = (byte)(0x80 | code >> 6 & 0x3F); to[n++] = (byte)(0x80 | code & 0x3F); }
            else { to[n++] = (byte)(0xF0 | code >> 18); to[n++] = (byte)(0x80 | code >> 12 & 0x3F); to[n++] = (byte)(0x80 | code >> 6 & 0x3F); to[n++] = (byte)(0x80 | code & 0x3F); }
        }
        return n;
    }

    private static int Hex4(ReadOnlySpan<byte> s)
    {
        var v = 0;
        foreach (var b in s) v = v << 4 | (b <= (byte)'9' ? b - '0' : (b | 0x20) - 'a' + 10);
        return v;
    }

    /// <summary>Appends the value the reader stands on to a key: kind, length, canonical bytes. Keys are kept whole (they are what a report
    /// names) and compared byte by byte: "a" is not "A", 1 is not "1", NULL is a key.</summary>
    public static void AppendKeyPart(scoped ref Utf8JsonReader r, ReadOnlySpan<byte> source, scoped ref byte[] key, scoped ref int length, scoped ref byte[] scratch)
    {
        ReadOnlySpan<byte> bytes;
        ValueKind kind;
        switch (r.TokenType)
        {
            case JsonTokenType.Null: kind = ValueKind.Null; bytes = default; break;
            case JsonTokenType.False: kind = ValueKind.False; bytes = default; break;
            case JsonTokenType.True: kind = ValueKind.True; bytes = default; break;
            case JsonTokenType.Number: kind = ValueKind.Number; bytes = r.ValueSpan; break;
            case JsonTokenType.String: kind = ValueKind.String; bytes = Text(ref r, ref scratch); break;
            case JsonTokenType.StartObject:
            case JsonTokenType.StartArray:
                kind = r.TokenType == JsonTokenType.StartObject ? ValueKind.Object : ValueKind.Array;
                var start = (int)r.TokenStartIndex;
                r.Skip();
                bytes = source[start..(int)r.BytesConsumed];
                break;
            default: throw new JsonException($"a value cannot start with {r.TokenType}");
        }
        if (key.Length < length + bytes.Length + 6) Array.Resize(ref key, Math.Max(key.Length * 2, length + bytes.Length + 6));
        key[length++] = (byte)kind;
        if (kind < ValueKind.Number) return;
        var n = (uint)bytes.Length;
        for (; n >= 0x80; n >>= 7) key[length++] = (byte)(n | 0x80);
        key[length++] = (byte)n;
        bytes.CopyTo(key.AsSpan(length));
        length += bytes.Length;
    }

    /// <summary>A key as the JSON object it was, for a report.</summary>
    public static string KeyText(ReadOnlySpan<byte> key, IReadOnlyList<string> names)
    {
        if (key.Length > 0 && key[0] == 0xFF) return $"(a key too long to keep, hash {Convert.ToHexString(key[1..])})";
        var sb = new StringBuilder("{");
        var at = 0;
        for (var i = 0; at < key.Length; i++)
        {
            if (i > 0) sb.Append(',');
            Quote(sb, i < names.Count ? names[i] : "?").Append(':');
            var kind = (ValueKind)key[at++];
            var bytes = ReadOnlySpan<byte>.Empty;
            if (kind >= ValueKind.Number)
            {
                var (n, shift) = (0, 0);
                for (; key[at] >= 0x80; shift += 7) n |= (key[at++] & 0x7F) << shift;
                n |= key[at++] << shift;
                bytes = key.Slice(at, n);
                at += n;
            }
            AppendValue(sb, kind, bytes);
        }
        return sb.Append('}').ToString();
    }

    /// <summary>What a cell holds, as far as it still knows: the value when it fitted, its hash otherwise.</summary>
    public static string CellText(ReadOnlySpan<byte> cell)
    {
        var kind = KindOf(cell[0]);
        var length = (cell[0] >> 3) - 1;
        if (kind == ValueKind.Unknown) return "(not known)";
        if (kind < ValueKind.Number) return kind == ValueKind.Null ? "null" : kind == ValueKind.True ? "true" : "false";
        if (length >= 0) return AppendValue(new StringBuilder(), kind, cell.Slice(1, length)).ToString();
        return $"({kind.ToString().ToLowerInvariant()}, hash {BinaryPrimitives.ReadUInt64LittleEndian(cell[1..]):x16})";
    }

    private static StringBuilder AppendValue(StringBuilder sb, ValueKind kind, ReadOnlySpan<byte> bytes) => kind switch
    {
        ValueKind.Null => sb.Append("null"),
        ValueKind.False => sb.Append("false"),
        ValueKind.True => sb.Append("true"),
        ValueKind.String => Quote(sb, Decode(bytes)),
        _ => sb.Append(Encoding.UTF8.GetString(bytes)),
    };

    /// <summary>Canonical text back to UTF-16, half surrogate pairs included.</summary>
    public static string Decode(ReadOnlySpan<byte> wtf8)
    {
        var sb = new StringBuilder(wtf8.Length);
        for (var i = 0; i < wtf8.Length;)
        {
            int b = wtf8[i], code, more;
            if (b < 0x80) (code, more) = (b, 0);
            else if (b < 0xE0) (code, more) = (b & 0x1F, 1);
            else if (b < 0xF0) (code, more) = (b & 0x0F, 2);
            else (code, more) = (b & 0x07, 3);
            i++;
            for (; more > 0 && i < wtf8.Length; more--) code = code << 6 | wtf8[i++] & 0x3F;
            if (code >= 0x10000 && code <= 0x10FFFF) sb.Append(char.ConvertFromUtf32(code));
            else sb.Append((char)code);
        }
        return sb.ToString();
    }

    public static StringBuilder Quote(StringBuilder sb, string s)
    {
        sb.Append('"');
        for (var i = 0; i < s.Length; i++)
        {
            var c = s[i];
            var pair = char.IsHighSurrogate(c) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]);
            if (pair) sb.Append(c).Append(s[++i]);
            else if (c is '"' or '\\' || c < ' ' || char.IsSurrogate(c)) sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
            else sb.Append(c);
        }
        return sb.Append('"');
    }

    /// <summary>Raw JSON for a report: whole when short.</summary>
    public static string Excerpt(ReadOnlySpan<byte> json) =>
        json.Length <= 120 ? Encoding.UTF8.GetString(json) : $"{Encoding.UTF8.GetString(json[..100])}... ({json.Length} bytes)";

    /// <summary>A name as it stands between its quotes in the message, decoded.</summary>
    public static string Name(ReadOnlySpan<byte> raw)
    {
        if (!raw.Contains((byte)'\\')) return Decode(raw);
        var buffer = new byte[raw.Length];
        return Decode(buffer.AsSpan(0, Unescape(raw, buffer)));
    }
}

/// <summary>Byte strings as dictionary keys, looked up by span.</summary>
internal sealed class Bytes : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
    public static readonly Bytes Comparer = new();
    public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);
    public int GetHashCode(byte[] obj) => GetHashCode(obj.AsSpan());
    public bool Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
    public int GetHashCode(ReadOnlySpan<byte> alternate) => (int)XxHash3.HashToUInt64(alternate);
    public byte[] Create(ReadOnlySpan<byte> alternate) => alternate.ToArray();
}
