using System.Text.Json;
using LogMiner.Capture;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// Gaps in <c>logminer.dbo.gaps</c>. With a resync, the request to load the database again is written in the same transaction, so neither
/// can be lost: the stream only goes on after both are durable.
/// </summary>
public sealed class SqlGapLog(string connectionString, string serverName) : IGapLog
{
    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            IF OBJECT_ID(N'dbo.gaps', N'U') IS NULL
                CREATE TABLE dbo.gaps (
                    id            bigint IDENTITY NOT NULL CONSTRAINT pk_gaps PRIMARY KEY,
                    server_name   sysname        NOT NULL,
                    database_name sysname        NOT NULL,
                    table_name    nvarchar(300)  NULL,      -- NULL: the whole database
                    incarnation   varchar(80)    NOT NULL,
                    after_commit  binary(10)     NOT NULL,  -- everything up to here was delivered
                    resumed_at    binary(10)     NOT NULL,  -- capture went on from here
                    continuity    varchar(20)    NOT NULL,  -- Broken: changes were lost to someone else's doing; Unknown: cannot be verified; NewTable: captured from here on
                    reason        nvarchar(2000) NOT NULL,
                    resync        bit            NOT NULL,  -- a historical load of the database was requested
                    detected_at   datetime2(3)   NOT NULL CONSTRAINT df_gaps_detected DEFAULT SYSUTCDATETIME(),
                    acknowledged  bit            NOT NULL CONSTRAINT df_gaps_ack DEFAULT 0); -- for operators
            """, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task RecordAsync(CaptureGap gap, bool resync, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;
            INSERT dbo.gaps (server_name, database_name, table_name, incarnation, after_commit, resumed_at, continuity, reason, resync)
            VALUES (@s, @d, @t, @i, @a, @r, @c, @why, @resync);
            IF @resync = 1 AND NOT EXISTS (SELECT 1 FROM dbo.snapshot_requests WHERE server_name = @s AND database_name = @d AND (table_name IS NULL OR table_name = @t) AND status = 'requested')
                INSERT dbo.snapshot_requests (server_name, database_name, table_name) VALUES (@s, @d, @t);
            COMMIT TRANSACTION WITH (DELAYED_DURABILITY = OFF);
            """, conn);
        cmd.Parameters.AddWithValue("@s", serverName);
        cmd.Parameters.AddWithValue("@d", gap.Database);
        cmd.Parameters.Add(new SqlParameter("@t", System.Data.SqlDbType.NVarChar, 300) { Value = (object?)gap.Table ?? DBNull.Value });
        cmd.Parameters.AddWithValue("@i", gap.Incarnation);
        cmd.Parameters.Add(new SqlParameter("@a", System.Data.SqlDbType.Binary, 10) { Value = gap.AfterCommit.ToBinary() });
        cmd.Parameters.Add(new SqlParameter("@r", System.Data.SqlDbType.Binary, 10) { Value = gap.ResumedAt.ToBinary() });
        cmd.Parameters.AddWithValue("@c", gap.Continuity.ToString());
        cmd.Parameters.AddWithValue("@why", gap.Reason.Length > 2000 ? gap.Reason[..2000] : gap.Reason);
        cmd.Parameters.AddWithValue("@resync", resync);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>Gaps appended to a local file (host-local mode, without a state database; there a resync has to be requested by hand).</summary>
public sealed class FileGapLog(string path) : IGapLog
{
    public async Task RecordAsync(CaptureGap gap, bool resync, CancellationToken ct)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        await using var file = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read, 1, FileOptions.WriteThrough);
        var line = JsonSerializer.Serialize(new
        {
            database = gap.Database, table = gap.Table, incarnation = gap.Incarnation, after_commit = gap.AfterCommit.ToString(), resumed_at = gap.ResumedAt.ToString(),
            continuity = gap.Continuity.ToString(), reason = gap.Reason, detected_at = gap.DetectedAt,
        });
        await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes(line + "\n"), ct);
        file.Flush(true);
    }
}
