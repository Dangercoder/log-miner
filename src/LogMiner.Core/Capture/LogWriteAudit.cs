using LogMiner.Log;

namespace LogMiner.Capture;

/// <summary>
/// Checks the pushed log flushes against an independent measure: the bytes written to each log file, as polled. What was written but not
/// pushed ("unexplained") must stay constant; when it grows, the push missed something (or it is still on its way, or the engine wrote to
/// the file without flushing log, as when it starts a new VLF) and the database has to be read. A push that stopped altogether shows up as
/// writes without any bell. Not a question of losing events - every database is read again anyway - only of finding them late.
/// </summary>
public sealed class LogWriteAudit
{
    /// <summary>Polls in a row that saw writes while nothing at all was pushed, after which the push counts as stalled.</summary>
    public const int StalledAfter = 3;
    /// <summary>Polls "unexplained" is compared with.</summary>
    public const int Window = 8;

    private sealed class Entry
    {
        public long Bytes;                                     // last polled
        public long Pushed;                                    // sum of all flushes pushed since the last poll
        public long Unexplained;                               // written minus pushed at the last poll (only differences mean something)
        public readonly long[] Before = new long[Window];      // and at the polls before it
        public int Polls;
    }

    private readonly object _gate = new();
    private readonly Dictionary<int, Entry> _entries = [];
    private long _bells, _bellsAtPoll;
    private int _silentPolls;

    /// <summary>Flushes pushed so far.</summary>
    public long Bells { get { lock (_gate) return _bells; } }
    /// <summary>Writes are going on but nothing is pushed.</summary>
    public bool Stalled { get { lock (_gate) return _silentPolls >= StalledAfter; } }

    public void Pushed(IReadOnlyCollection<LogFlush> flushes)
    {
        lock (_gate)
        {
            foreach (var f in flushes)
                if (_entries.TryGetValue(f.DatabaseId, out var e)) e.Pushed += f.WriteSize; // an unknown database is new: its first poll reports it
            _bells += flushes.Count;
        }
    }

    /// <summary>
    /// Takes a poll of (some) databases' bytes written and returns those that have to be read: every one that wrote, or with
    /// <paramref name="pushing"/> only those the push does not account for.
    /// </summary>
    /// <param name="complete">The poll covers every database: databases that are not in it are forgotten, and stalling is judged.</param>
    public List<int> Polled(IReadOnlyCollection<(int DatabaseId, long Bytes)> poll, bool pushing, bool complete = true)
    {
        var read = new List<int>();
        lock (_gate)
        {
            var wrote = false;
            foreach (var (db, bytes) in poll)
            {
                if (!_entries.TryGetValue(db, out var e) || bytes < e.Bytes) // first seen, or the file was replaced (re-created or restored)
                {
                    if (e is not null) read.Add(db);
                    _entries[db] = new Entry { Bytes = bytes };
                    continue;
                }
                var delta = bytes - e.Bytes;
                var unexplained = e.Unexplained + delta - e.Pushed;
                wrote |= delta > 0;
                // A flush still on its way at an earlier poll raised "unexplained" then and lowers it again when it arrives, which must not hide
                // one that was missed: compared with the lowest of the last polls (one of them will have had nothing on its way).
                if (delta > 0 && (!pushing || unexplained > e.Before.Min())) read.Add(db);
                (e.Bytes, e.Pushed, e.Unexplained) = (bytes, 0, unexplained);
                e.Before[e.Polls++ % Window] = unexplained;
            }
            if (!complete) return read;
            if (_entries.Count > poll.Count)
            {
                var present = poll.Select(p => p.DatabaseId).ToHashSet();
                foreach (var gone in _entries.Keys.Where(k => !present.Contains(k)).ToArray()) _entries.Remove(gone);
            }
            if (_bells != _bellsAtPoll || !pushing) _silentPolls = 0;
            else if (wrote) _silentPolls++;
            _bellsAtPoll = _bells;
        }
        return read;
    }
}
