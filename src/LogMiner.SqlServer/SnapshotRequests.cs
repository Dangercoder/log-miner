using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

public sealed record SnapshotRequest(long Id, string Database, string? Table, int ChunkSize);

/// <summary>
/// On-demand historical loads, requested by inserting a row into <c>logminer.dbo.snapshot_requests</c>:
/// <code>INSERT logminer.dbo.snapshot_requests (server_name, database_name, table_name) VALUES ('sql01', 'tenant_0042', 'dbo.orders');</code>
/// (<c>database_name = '*'</c> for every captured database, <c>table_name = NULL</c> for every captured table). The engine claims
/// requests, runs them interleaved with the live stream and reports progress in the same row: <c>status</c> goes
/// requested → running → done | partial (some tables failed or were skipped; see <c>error</c>) | failed. A request that was
/// running when the service stopped starts over when it comes back (snapshot rows are idempotent upserts).
/// </summary>
public sealed class SnapshotRequests(string connectionString, string serverName)
{
    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        await EnsureTableAsync(ct);
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        // requests that were running when the previous instance stopped start over (snapshot rows are idempotent upserts)
        await using var cmd = new SqlCommand("UPDATE dbo.snapshot_requests SET status = 'requested', started_at = NULL WHERE server_name = @s AND status = 'running';", conn);
        cmd.Parameters.AddWithValue("@s", serverName);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task EnsureTableAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            IF OBJECT_ID(N'dbo.snapshot_requests', N'U') IS NULL
                CREATE TABLE dbo.snapshot_requests (
                    id            bigint IDENTITY NOT NULL CONSTRAINT pk_snapshot_requests PRIMARY KEY,
                    server_name   sysname        NOT NULL,
                    database_name sysname        NOT NULL,
                    table_name    nvarchar(300)  NULL,
                    chunk_size    int            NOT NULL CONSTRAINT df_snapshot_chunk DEFAULT 5000,
                    status        varchar(20)    NOT NULL CONSTRAINT df_snapshot_status DEFAULT 'requested',
                    rows_emitted  bigint         NOT NULL CONSTRAINT df_snapshot_rows DEFAULT 0,
                    requested_at  datetime2(3)   NOT NULL CONSTRAINT df_snapshot_requested DEFAULT SYSUTCDATETIME(),
                    started_at    datetime2(3)   NULL,
                    completed_at  datetime2(3)   NULL,
                    error         nvarchar(2000) NULL);
            """, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<SnapshotRequest>> ClaimAsync(CancellationToken ct)
    {
        var list = new List<SnapshotRequest>();
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            UPDATE dbo.snapshot_requests SET status = 'running', started_at = SYSUTCDATETIME()
            OUTPUT inserted.id, inserted.database_name, inserted.table_name, CASE WHEN inserted.chunk_size BETWEEN 1 AND 100000 THEN inserted.chunk_size WHEN inserted.chunk_size > 100000 THEN 100000 ELSE 5000 END
            WHERE server_name = @s AND status = 'requested';
            """, conn);
        cmd.Parameters.AddWithValue("@s", serverName);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) list.Add(new SnapshotRequest(r.GetInt64(0), r.GetString(1), r.IsDBNull(2) ? null : r.GetString(2), r.GetInt32(3)));
        return list;
    }

    /// <summary>The destination lost what it had acknowledged of these databases lately: rows of loads that ended just before may be among it.</summary>
    public async Task RequestAgainAsync(IReadOnlyCollection<string> databases, TimeSpan completedWithin, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            UPDATE dbo.snapshot_requests SET status = 'requested', started_at = NULL, completed_at = NULL
            WHERE server_name = @s AND status IN ('done', 'partial') AND completed_at > DATEADD(second, -@within, SYSUTCDATETIME())
              AND (database_name = '*' OR database_name COLLATE DATABASE_DEFAULT IN (SELECT value COLLATE DATABASE_DEFAULT FROM OPENJSON(@dbs)));
            """, conn);
        cmd.Parameters.AddWithValue("@s", serverName);
        cmd.Parameters.AddWithValue("@within", (int)Math.Min(int.MaxValue, completedWithin.TotalSeconds));
        cmd.Parameters.Add(new SqlParameter("@dbs", System.Data.SqlDbType.NVarChar, -1) { Value = System.Text.Json.JsonSerializer.Serialize(databases) });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task ReportAsync(long id, long rowsEmitted, string? status, string? error, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            UPDATE dbo.snapshot_requests
            SET rows_emitted = @rows, status = ISNULL(@status, status), error = ISNULL(@error, error),
                completed_at = CASE WHEN @status IN ('done', 'partial', 'failed') THEN SYSUTCDATETIME() ELSE completed_at END
            WHERE id = @id;
            """, conn);
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@rows", rowsEmitted);
        cmd.Parameters.AddWithValue("@status", (object?)status ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@error", (object?)error ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
