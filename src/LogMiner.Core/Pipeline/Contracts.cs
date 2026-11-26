using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using LogMiner.Sinks;

namespace LogMiner.Pipeline;

/// <summary>
/// Resume point of one database stream.
/// <para><see cref="RestartLsn"/>: where to re-read the log from (oldest record of any transaction that was open or not yet
/// acknowledged when the checkpoint was taken).</para>
/// <para><see cref="LastCommitLsn"/>/<see cref="LastBeginLsn"/>: the newest transaction whose events (and all earlier ones)
/// the sink acknowledged. On resume, transactions committed at or before it are skipped; it is also the position handed to
/// sp_repldone so SQL Server may truncate the log behind it (except the newest transaction, see <see cref="Held"/>).</para>
/// <para><see cref="Incarnation"/>: identity of the database (recovery fork + creation time). A checkpoint of a different
/// incarnation (database restored or re-created) is never applied - its LSNs belong to another history.</para>
/// <para><see cref="Held"/>: the transaction at <see cref="LastCommitLsn"/> has deliberately not been released with sp_repldone. It is a
/// bookmark in SQL Server's own replication state: while it is the oldest unreleased transaction, nobody else released log beyond us.</para>
/// <para><see cref="ResumedAt"/>: where reading went on after the position had been truncated (zero: never). Transactions that began before it
/// and commit after <see cref="LastCommitLsn"/> may lack their beginning and are never published; <see cref="ResumedInGap"/>: they are part of
/// a recorded gap.</para>
/// <para><see cref="Settled"/>: <see cref="RestartLsn"/> is the last record of a read that reached the end of the log on disk. What its log block holds
/// besides is not captured, so it does not matter if that is truncated.</para>
/// <para><see cref="Receipt"/>: the destination's receipt for the newest message it acknowledged (of this position or a later one). It is shown to the
/// destination before anything is sent again; when it does not have that message any more (it acknowledged what it had not on disk, and lost power),
/// the stream goes back to <see cref="Safe"/>.</para>
/// </summary>
public sealed record Checkpoint(string Database, string Incarnation, Lsn RestartLsn, Lsn LastCommitLsn, Lsn LastBeginLsn, DateTimeOffset UpdatedAt, bool Held = false,
                                Lsn ResumedAt = default, bool ResumedInGap = false, bool Settled = false, string? Receipt = null, SafePosition? Safe = null)
{
    /// <summary>The position to go on from when the destination has lost what it acknowledged since <see cref="Safe"/> (null: there is none).</summary>
    public Checkpoint? Rewound() => Safe is not { } s ? null
        : this with { RestartLsn = s.Held ? s.CommitLsn : s.RestartLsn, LastCommitLsn = s.CommitLsn, LastBeginLsn = s.BeginLsn, Held = s.Held, Settled = false, Receipt = null };

    public string Serialize() => string.Create(CultureInfo.InvariantCulture,
        $"{RestartLsn}|{LastCommitLsn}|{LastBeginLsn}|{UpdatedAt.ToUnixTimeMilliseconds()}|{Incarnation}|{(Held ? 1 : 0)}|{ResumedAt}|{(ResumedInGap ? 1 : 0)}|{(Settled ? 1 : 0)}|{Uri.EscapeDataString(Receipt ?? "")}|{Safe?.RestartLsn}|{Safe?.CommitLsn}|{Safe?.BeginLsn}|{Safe?.Held}");

    public static Checkpoint Deserialize(string database, string value)
    {
        var p = value.Split('|');
        if (p.Length is not (5 or 6 or 8 or 14)) throw new FormatException($"bad checkpoint for {database}: '{value}'"); // 5: before Held existed, 6: before ResumedAt, 8: before Settled
        return new Checkpoint(database, p[4], Lsn.Parse(p[0]), Lsn.Parse(p[1]), Lsn.Parse(p[2]),
            DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(p[3], CultureInfo.InvariantCulture)), p.Length >= 6 && p[5] == "1",
            p.Length >= 8 ? Lsn.Parse(p[6]) : default, p.Length >= 8 && p[7] == "1", p.Length == 14 && p[8] == "1", p.Length == 14 && p[9].Length > 0 ? Uri.UnescapeDataString(p[9]) : null,
            p.Length == 14 && p[10].Length > 0 ? new SafePosition(Lsn.Parse(p[10]), Lsn.Parse(p[11]), Lsn.Parse(p[12]), bool.Parse(p[13])) : null);
    }
}

/// <summary>
/// An earlier durable position whose events the destination was seen to have, one release interval after they were acknowledged. Unless nothing is held
/// back (<see cref="Held"/>), its transaction has not been released: SQL Server keeps it, and everything after it, in the log.
/// </summary>
public sealed record SafePosition(Lsn RestartLsn, Lsn CommitLsn, Lsn BeginLsn, bool Held);

public interface ICheckpointStore
{
    Task<Checkpoint?> LoadAsync(string database, CancellationToken ct);
    /// <summary>Persists all <paramref name="checkpoints"/> durably (all-or-nothing) before completing.</summary>
    Task SaveAsync(IReadOnlyCollection<Checkpoint> checkpoints, CancellationToken ct);
}

public sealed class MemoryCheckpointStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<string, Checkpoint> _map = new(StringComparer.OrdinalIgnoreCase);
    private int _saves;
    public int Saves => Volatile.Read(ref _saves);
    public Task<Checkpoint?> LoadAsync(string database, CancellationToken ct) => Task.FromResult(_map.TryGetValue(database, out var c) ? c : null);
    public Task SaveAsync(IReadOnlyCollection<Checkpoint> checkpoints, CancellationToken ct)
    {
        foreach (var c in checkpoints) _map[c.Database] = c;
        Interlocked.Increment(ref _saves);
        return Task.CompletedTask;
    }
}

/// <summary>
/// All checkpoints of the instance in one local file, replaced atomically: written to a temp file with write-through,
/// fsync'd, then renamed over the previous file. A torn or corrupt file (bad magic/length/CRC-32C) is rejected rather than
/// half-read; the previous generation is kept as a backup and used instead. Local by design: checkpointing must not depend
/// on the remote destination being reachable.
/// </summary>
public sealed class FileCheckpointStore : ICheckpointStore, IDisposable
{
    private const uint Magic = 0x31434D4C; // "LMC1"
    private readonly string _path;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private Dictionary<string, Checkpoint>? _all;

    public FileCheckpointStore(string path)
    {
        _path = path;
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
    }

    public async Task<Checkpoint?> LoadAsync(string database, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            _all ??= ReadAll();
            return _all.GetValueOrDefault(database);
        }
        finally { _gate.Release(); }
    }

    public async Task SaveAsync(IReadOnlyCollection<Checkpoint> checkpoints, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            _all ??= ReadAll();
            var next = new Dictionary<string, Checkpoint>(_all, StringComparer.OrdinalIgnoreCase);
            foreach (var c in checkpoints)
            {
                next.Remove(c.Database); // the name may differ in case
                next[c.Database] = c;
            }
            var body = new StringBuilder();
            foreach (var c in next.Values.OrderBy(c => c.Database, StringComparer.OrdinalIgnoreCase))
                body.Append(Escape(c.Database)).Append('\t').Append(c.Serialize()).Append('\n');
            var bytes = Encoding.UTF8.GetBytes(body.ToString());
            var tmp = _path + ".tmp";
            await using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1, FileOptions.WriteThrough))
            {
                var header = new byte[12];
                BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
                BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4), (uint)bytes.Length);
                BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(8), Crc32C.Compute(bytes));
                await fs.WriteAsync(header, ct);
                await fs.WriteAsync(bytes, ct);
                fs.Flush(true);
            }
            if (ReadFile(_path) is not null) File.Copy(_path, _path + ".bak", overwrite: true); // a damaged file must not replace a good backup
            File.Move(tmp, _path, overwrite: true);
            _all = next;
        }
        finally { _gate.Release(); }
    }

    /// <summary>Damaged positions are not the same as none: starting without them could skip events.</summary>
    private Dictionary<string, Checkpoint> ReadAll() =>
        ReadFile(_path) ?? ReadFile(_path + ".bak") ?? (File.Exists(_path) || File.Exists(_path + ".bak")
            ? throw new InvalidDataException($"the checkpoints in {_path} (and its backup) are damaged; restore the file, or delete both to resume from SQL Server's own replication state")
            : new Dictionary<string, Checkpoint>(StringComparer.OrdinalIgnoreCase));

    private static Dictionary<string, Checkpoint>? ReadFile(string path)
    {
        if (!File.Exists(path)) return null;
        var data = File.ReadAllBytes(path);
        if (data.Length < 12 || BinaryPrimitives.ReadUInt32LittleEndian(data) != Magic) return null;
        var length = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4));
        if (12 + length != data.Length || Crc32C.Compute(data.AsSpan(12)) != BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(8))) return null;
        var map = new Dictionary<string, Checkpoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var line in Encoding.UTF8.GetString(data, 12, (int)length).Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var tab = line.IndexOf('\t', StringComparison.Ordinal);
            var db = Unescape(line[..tab]);
            map[db] = Checkpoint.Deserialize(db, line[(tab + 1)..]);
        }
        return map;
    }

    public void Dispose() => _gate.Dispose();

    private static string Escape(string s) => s.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\t", "\\t", StringComparison.Ordinal).Replace("\n", "\\n", StringComparison.Ordinal);
    private static string Unescape(string s)
    {
        var sb = new StringBuilder(s.Length);
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] != '\\' || i + 1 == s.Length) { sb.Append(s[i]); continue; }
            var c = s[++i];
            sb.Append(c switch { 't' => '\t', 'n' => '\n', _ => c });
        }
        return sb.ToString();
    }
}
