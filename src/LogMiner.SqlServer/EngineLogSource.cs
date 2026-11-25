using System.Runtime.CompilerServices;
using LogMiner.Capture;
using LogMiner.Log;
using LogMiner.Pipeline;
using LogMiner.Schema;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// Production <see cref="ILogSource"/>: sys.fn_full_dblog on the worker's long-lived connection; layout (catalog) queries on a
/// separate pooled connection, because a schema reload can be needed while the log reader is still open on the first one.
/// </summary>
public sealed class EngineLogSource(SqlConnection readerConnection, string connectionString, CaptureScope scope) : ILogSource
{
    private DatabaseRef? _held;

    public bool Unhardened { get; private set; }

    public async IAsyncEnumerable<LogRecord> ReadAsync(DatabaseRef database, Lsn? start, int maxRows, [EnumeratorCancellation] CancellationToken ct, bool settled = false)
    {
        var reader = new EngineLogReader(readerConnection);
        Unhardened = false;
        try
        {
            await foreach (var r in reader.ReadAsync(database, start, maxRows, scope, ct, settled))
            {
                _held = database;
                yield return r;
            }
            Unhardened = reader.Unhardened;
        }
        finally { _held = null; }
    }

    public async Task VerifyAsync(DatabaseRef database, CancellationToken ct)
    {
        // a read that is open has proven it, and keeps it that way. (Entering it again, on another connection, would wait behind somebody who
        // waits for that read to end, e.g. to restore the database.)
        if (_held == database && readerConnection.State == System.Data.ConnectionState.Open) return;
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await DatabaseIdentity.VerifyAsync(conn, database, ct);
    }

    public async Task<DatabaseSchema> LoadSchemaAsync(DatabaseRef database, Lsn? from, IReadOnlySet<long> unresolvedRowsets, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        try { return await SchemaLoader.LoadAsync(conn, database, scope, from, unresolvedRowsets, ct); }
        catch (SqlException ex)
        {
            await DatabaseIdentity.ThrowIfChangedAsync(connectionString, database, ex, ct);
            throw;
        }
    }

    public async Task<(LogContinuity Continuity, string Detail)> CheckContinuityAsync(DatabaseRef database, Checkpoint position, CancellationToken ct)
    {
        if (!position.Held || position.LastBeginLsn.IsZero)
            return (LogContinuity.Unknown, "no transaction was held back at this position, so it cannot be verified that all captured changes after it were retained");
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        Lsn? oldest;
        try { oldest = await ReplDoneClient.OldestUnreleasedAsync(conn, database, ct); }
        catch (SqlException ex)
        {
            await DatabaseIdentity.ThrowIfChangedAsync(connectionString, database, ex, ct);
            throw;
        }
        return ReplDoneClient.Explain(oldest, position) is { } broken
            ? (LogContinuity.Broken, broken)
            : (LogContinuity.Intact, "the held-back transaction is still unreleased");
    }
}

/// <summary>Production <see cref="ILogReleaser"/>: sp_repldone + sp_replflush on a dedicated connection.</summary>
public sealed class SqlLogReleaser(string connectionString) : ILogReleaser, IAsyncDisposable
{
    private SqlConnection? _conn;

    public async Task ReleaseAsync(DatabaseRef database, Lsn beginLsn, Lsn commitLsn, CancellationToken ct)
    {
        if (_conn is null || _conn.State != System.Data.ConnectionState.Open)
        {
            if (_conn is not null) await _conn.DisposeAsync();
            _conn = new SqlConnection(connectionString);
            await _conn.OpenAsync(ct);
        }
        try { await ReplDoneClient.MarkDistributedAsync(_conn, database, beginLsn, commitLsn, ct); }
        catch (SqlException ex)
        {
            await DatabaseIdentity.ThrowIfChangedAsync(connectionString, database, ex, ct);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_conn is not null) await _conn.DisposeAsync();
    }
}
