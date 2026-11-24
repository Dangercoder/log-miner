using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using LogMiner.Events;
using LogMiner.Rows;
using LogMiner.Schema;

namespace LogMiner.Sinks;

public enum PartitionKeyMode
{
    /// <summary>Key = database: total commit order per database (default).</summary>
    Database,
    /// <summary>Key = database + table: order per table, more parallelism in partitioned sinks.</summary>
    Table,
    /// <summary>Key = database + table + primary key: order per row, maximum parallelism.</summary>
    RowKey,
}

public sealed class EncoderOptions
{
    /// <summary>Logical source name (appears in events and destinations). Defaults to the machine name.</summary>
    public string ServerName { get; init; } = Environment.MachineName;
    /// <summary>Destination template. Tokens: {server} {db} {schema} {table} {op}. Each token is sanitised to [A-Za-z0-9_-].</summary>
    public string DestinationTemplate { get; init; } = "cdc.{server}.{db}.{schema}.{table}";
    public PartitionKeyMode PartitionKey { get; init; } = PartitionKeyMode.Database;
}

/// <summary>Turns change events into transport-neutral <see cref="SinkMessage"/>s (JSON envelope, routing, idempotency key).</summary>
public sealed class EventEncoder(EncoderOptions options)
{
    // by name, not by layout: every reload makes new layouts
    private readonly ConcurrentDictionary<(string Database, string Schema, string Table, ChangeOp Op), string> _destinations = new();

    public EncoderOptions Options { get; } = options;

    public SinkMessage Encode(ChangeEvent e)
    {
        var buffer = new ArrayBufferWriter<byte>(512);
        ChangeEventJson.Write(e, Options.ServerName, buffer);
        return new SinkMessage
        {
            Database = e.Table.Database,
            Stream = e.SnapshotId is null ? $"{e.Table.Database}#{e.Incarnation}" : null,
            Position = e.SnapshotId is null ? new SourcePosition(e.CommitLsn, e.Sequence) : null,
            Destination = _destinations.GetOrAdd((e.Table.Database, e.Table.Schema, e.Table.Table, e.Op), k => Render(k.Database, k.Schema, k.Table, k.Op)),
            Key = KeyFor(e),
            MessageId = e.MessageId(Options.ServerName),
            Payload = buffer.WrittenMemory,
            ContentType = "application/json",
            Headers =
            [
                new("lm-db", HeaderValue(e.Table.Database)),
                new("lm-table", HeaderValue($"{e.Table.Schema}.{e.Table.Table}")),
                new("lm-op", OpCode(e.Op)),
                new("lm-commit-lsn", e.CommitLsn.ToString()),
            ],
        };
    }

    public List<SinkMessage> Encode(IReadOnlyList<ChangeEvent> events, long maxPayload = long.MaxValue)
    {
        var list = new List<SinkMessage>(events.Count);
        foreach (var e in events) list.Add(Encode(e, maxPayload));
        return list;
    }

    /// <summary>An event that is too large for the destination would stop its database for good. Its largest values are marked as unavailable
    /// (as LOBs that are not in the log are), largest first, never those of the key.</summary>
    public SinkMessage Encode(ChangeEvent e, long maxPayload)
    {
        var message = Encode(e);
        if (message.Payload.Length <= maxPayload) return message;
        var (before, after) = (Copy(e.Before), Copy(e.After));
        var large = new[] { before, after }.Where(i => i is not null)
            .SelectMany(i => i!.Values.Select((v, n) => (Image: i, Index: n, Size: Size(v), Key: i.Layout.Visible[n].KeyOrdinal > 0)))
            .Where(x => x.Size > 256 && !x.Key).OrderByDescending(x => x.Size);
        foreach (var x in large)
        {
            x.Image.Values[x.Index] = Unavailable.Value;
            message = Encode(e.With(before, after));
            if (message.Payload.Length <= maxPayload) return message;
        }
        return message; // the destination will say no
    }

    private static RowImage? Copy(RowImage? image) => image is null ? null : new RowImage(image.Layout, [.. image.Values]);
    private static long Size(object? v) => v switch { string s => 6L * s.Length, byte[] b => b.Length * 4L / 3, RawText t => t.Bytes.Length * 4L / 3, _ => 0 };

    private string KeyFor(ChangeEvent e) => Options.PartitionKey switch
    {
        PartitionKeyMode.Table => $"{e.Table.Database}/{e.Table.Schema}.{e.Table.Table}",
        PartitionKeyMode.RowKey => $"{e.Table.Database}/{e.Table.Schema}.{e.Table.Table}/{RowKeyJson(e)}",
        _ => e.Table.Database,
    };

    private static string RowKeyJson(ChangeEvent e)
    {
        var image = e.After ?? e.Before;
        if (image is null || e.Table.Key.Count == 0) return "";
        var buffer = new ArrayBufferWriter<byte>(64);
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartArray();
            foreach (var k in e.Table.Key) ChangeEventJson.WriteValue(w, image.Values[e.Table.VisibleIndex[k.ColumnId]]);
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Header values cannot hold line breaks; those (and other control characters, and '%') are percent-encoded.</summary>
    public static string HeaderValue(string s)
    {
        if (!s.Any(c => c is < ' ' or '%' or '\x7f') && s.Trim(' ') == s) return s;
        var sb = new StringBuilder(s.Length + 8);
        for (var i = 0; i < s.Length; i++)
            if (s[i] is < ' ' or '%' or '\x7f' || (s[i] == ' ' && (i == 0 || i == s.Length - 1))) sb.Append('%').Append(((int)s[i]).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            else sb.Append(s[i]);
        return sb.ToString();
    }

    private string Render(string database, string schema, string table, ChangeOp op) => Options.DestinationTemplate
        .Replace("{server}", Token(Options.ServerName), StringComparison.Ordinal)
        .Replace("{db}", Token(database), StringComparison.Ordinal)
        .Replace("{schema}", Token(schema), StringComparison.Ordinal)
        .Replace("{table}", Token(table), StringComparison.Ordinal)
        .Replace("{op}", OpCode(op), StringComparison.Ordinal);

    /// <summary>Why a destination template cannot work, or null.</summary>
    public static string? TemplateProblem(string template)
    {
        var rest = template;
        foreach (var token in new[] { "{server}", "{db}", "{schema}", "{table}", "{op}" }) rest = rest.Replace(token, "x", StringComparison.Ordinal);
        if (rest.Length == 0 || rest.Split('.').Any(part => part.Length == 0)) return $"the destination template '{template}' has an empty part";
        return rest.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-') ? null
            : $"the destination template '{template}' may only contain {{server}}, {{db}}, {{schema}}, {{table}}, {{op}}, letters, digits, '_', '-' and '.'";
    }

    public static string OpCode(ChangeOp op) => op switch { ChangeOp.Create => "c", ChangeOp.Update => "u", ChangeOp.Delete => "d", _ => "r" };

    /// <summary>Keeps [A-Za-z0-9_-]; anything else (dots, spaces, wildcards, non-ASCII) becomes '_'. Safe for NATS subjects and Kafka topics.</summary>
    public static string Token(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s) sb.Append(ch is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' or '-' ? ch : '_');
        return sb.Length == 0 ? "_" : sb.ToString();
    }
}
