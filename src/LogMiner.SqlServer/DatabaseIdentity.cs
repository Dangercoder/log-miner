using LogMiner.Capture;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// SQL that proves a <see cref="DatabaseRef"/>: the id still belongs to the name and to the recovery fork it had (the fork is new after every
/// restore and every revert to a database snapshot, see experiments/27; the check costs ~5 µs). Raises error 50003 otherwise.
/// </summary>
public static class DatabaseIdentity
{
    public const int ChangedError = 50003;

    private const string Fork = "EXISTS (SELECT 1 FROM sys.database_recovery_status WHERE database_id = @dbid AND (@fork IS NULL OR CONVERT(binary(16), recovery_fork_guid) = @fork))";

    /// <summary>In any database: pair it with the same check after whatever it protects.</summary>
    public const string Check = "IF DB_NAME(@dbid) COLLATE Latin1_General_BIN2 <> @dbname COLLATE Latin1_General_BIN2 OR DB_NAME(@dbid) IS NULL OR NOT " + Fork + " THROW 50003, 'the database changed', 1;";

    /// <summary>Inside the database itself (its shared lock keeps it from being renamed, dropped or restored while the batch runs).</summary>
    public const string CheckInside = "IF DB_ID() <> @dbid OR NOT " + Fork + " THROW 50003, 'the database changed', 1;";

    /// <summary>Parameter declarations of <see cref="CheckInside"/> for sp_executesql.</summary>
    public const string InsideParameters = "@dbid int, @fork binary(16)";

    public static void AddParameters(SqlCommand cmd, DatabaseRef database, bool name = true)
    {
        cmd.Parameters.Add(new SqlParameter("@dbid", System.Data.SqlDbType.Int) { Value = database.Id });
        cmd.Parameters.Add(new SqlParameter("@fork", System.Data.SqlDbType.Binary, 16) { Value = (object?)ForkOf(database) ?? DBNull.Value });
        if (name) cmd.Parameters.Add(new SqlParameter("@dbname", System.Data.SqlDbType.NVarChar, 128) { Value = database.Name });
    }

    /// <summary>The incarnation starts with the recovery fork's 32 hex digits (nothing, when the server did not report one).</summary>
    private static byte[]? ForkOf(DatabaseRef database) =>
        database.Incarnation.Length >= 32 && database.Incarnation.Take(32).All(char.IsAsciiHexDigit) ? Convert.FromHexString(database.Incarnation.AsSpan(0, 32)) : null;

    public static bool IsChanged(SqlException ex) => ex.Number == ChangedError;

    /// <summary>Something failed inside the database (e.g. the name does not exist any more): because it is not that database, or for a
    /// reason of its own?</summary>
    public static async Task ThrowIfChangedAsync(string connectionString, DatabaseRef database, SqlException error, CancellationToken ct)
    {
        if (IsChanged(error)) throw new DatabaseChangedException(database, error);
        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await VerifyAsync(conn, database, ct);
        }
        catch (SqlException) { } // the server is not there: the original error tells
    }

    public static async Task VerifyAsync(SqlConnection conn, DatabaseRef database, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(Check, conn);
        AddParameters(cmd, database);
        try { await cmd.ExecuteNonQueryAsync(ct); }
        catch (SqlException ex) when (IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
    }
}
