using LogMiner.Capture;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// The only way the miner touches a captured database. Three things are ensured for every statement:
/// <list type="bullet">
/// <item><b>The database stays what it is while the statement runs.</b> The log functions take a database id and read it from outside, without
/// keeping anybody from restoring the database or taking it offline. Done at the wrong moment, one such read fails an assertion inside SQL Server
/// (logtrunc.cpp:834) and then every session of the INSTANCE hangs for good (experiments/35 and 36: restore and offline, with an explicit start as
/// well). So the database is entered first: its shared lock is held until the statement has ended, and entering one that is being restored or
/// taken offline is refused cleanly. The read itself runs in master below that (<see cref="HeldText"/>): one cached plan for 1,200 databases (a
/// plan per database is 1.4 MB each), 16 KB per database for the call around it.</item>
/// <item><b>The miner never takes the connection of a single-user database</b>, which belongs to whoever is restoring, renaming or repairing it:
/// what is looked at first takes no lock on the database. The procedure's name is in a variable for the same reason: a name in the text opens
/// the database when the batch is compiled, before anything in it runs. In a deadlock with such work the miner is the victim.</item>
/// <item><b>It is the database it was</b> (<see cref="DatabaseRef"/>): the id still belongs to the name and to the recovery fork it had (the fork
/// is new after every restore and every revert to a snapshot, see experiments/27; ~5 µs). Error 50003 otherwise.</item>
/// </list>
/// </summary>
public static class DatabaseIdentity
{
    public const int ChangedError = 50003;

    /// <summary>In single-user mode: left alone, tried again later. Its log is kept meanwhile.</summary>
    public const int BusyError = 50005;

    private const string Parameters = "@dbid int, @fork binary(16), @dbname nvarchar(128)";

    private const string NotTheName = "DB_NAME(@dbid) IS NULL OR DB_NAME(@dbid) COLLATE Latin1_General_BIN2 <> @dbname COLLATE Latin1_General_BIN2";

    private const string Fork = "EXISTS (SELECT 1 FROM sys.database_recovery_status WHERE database_id = @dbid AND (@fork IS NULL OR CONVERT(binary(16), recovery_fork_guid) = @fork))";

    /// <summary>Under the database's lock, in any database.</summary>
    private const string Check = "IF " + NotTheName + " OR NOT " + Fork + " THROW 50003, 'the database changed', 1; ";

    private const string Enter =
        "SET DEADLOCK_PRIORITY LOW; SET LOCK_TIMEOUT 5000; " +
        "IF " + NotTheName + " THROW 50003, 'the database changed', 1; " +
        "IF CONVERT(nvarchar(20), DATABASEPROPERTYEX(@dbname, 'UserAccess')) = N'SINGLE_USER' THROW 50005, 'the database is in single-user mode: the miner stays out of it', 1; " +
        "DECLARE @enter nvarchar(300) = QUOTENAME(@dbname) + N'.sys.sp_executesql'; ";

    /// <summary>A batch that runs @q in master while the database is held. Parameters of the statement beyond the identity are appended to both
    /// lists (<c>", @start nvarchar(46)"</c> and <c>", @start"</c>).</summary>
    public static string HeldText(string declarations = "", string arguments = "") =>
        $"{Enter}EXEC @enter N'EXEC master.sys.sp_executesql @q, N''{Parameters}{declarations}'', @dbid, @fork, @dbname{arguments}', N'@q nvarchar(max), {Parameters}{declarations}', @q, @dbid, @fork, @dbname{arguments};";

    private static readonly string HeldAlone = HeldText();

    /// <param name="conn">A connection of its own: the batch sets its lock timeout and deadlock priority.</param>
    /// <param name="database">The database to hold.</param>
    /// <param name="statement">Runs in master (the database is @dbid).</param>
    /// <param name="heldText">From <see cref="HeldText"/>, when the statement has parameters of its own.</param>
    public static SqlCommand Held(SqlConnection conn, DatabaseRef database, string statement, string? heldText = null)
    {
        var cmd = new SqlCommand(heldText ?? HeldAlone, conn);
        cmd.Parameters.Add(new SqlParameter("@q", System.Data.SqlDbType.NVarChar, -1) { Value = Check + statement });
        AddParameters(cmd, database);
        return cmd;
    }

    /// <summary>Put before a statement that names the database (in text run by sp_executesql, so that it is compiled behind this): leaves a single-user
    /// database alone. For reads of user tables, which hold the database themselves.</summary>
    internal const string Guard =
        "SET DEADLOCK_PRIORITY LOW; IF CONVERT(nvarchar(20), DATABASEPROPERTYEX(@db, 'UserAccess')) = N'SINGLE_USER' THROW 50005, 'the database is in single-user mode: the miner stays out of it', 1; ";

    /// <summary>A statement in the database itself (catalog views, sp_repldone): a plan per database, so not for what runs often, and large statements say
    /// OPTION (RECOMPILE) (experiments/39). The identity is checked from master, with one plan.</summary>
    public static SqlCommand Inside(SqlConnection conn, DatabaseRef database, string statement, string declarations = "", string arguments = "")
    {
        var cmd = new SqlCommand($"{Enter}EXEC @enter @q, N'@check nvarchar(max), {Parameters}{declarations}', @check, @dbid, @fork, @dbname{arguments};", conn);
        cmd.Parameters.Add(new SqlParameter("@q", System.Data.SqlDbType.NVarChar, -1) { Value = $"EXEC master.sys.sp_executesql @check, N'{Parameters}', @dbid, @fork, @dbname; " + statement });
        cmd.Parameters.Add(new SqlParameter("@check", System.Data.SqlDbType.NVarChar, -1) { Value = Check });
        AddParameters(cmd, database);
        return cmd;
    }

    private static void AddParameters(SqlCommand cmd, DatabaseRef database)
    {
        cmd.Parameters.Add(new SqlParameter("@dbid", System.Data.SqlDbType.Int) { Value = database.Id });
        cmd.Parameters.Add(new SqlParameter("@fork", System.Data.SqlDbType.Binary, 16) { Value = (object?)ForkOf(database) ?? DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@dbname", System.Data.SqlDbType.NVarChar, 128) { Value = database.Name });
    }

    /// <summary>The incarnation starts with the recovery fork's 32 hex digits (nothing, when it is not known yet).</summary>
    private static byte[]? ForkOf(DatabaseRef database) =>
        database.Incarnation.Length >= 32 && database.Incarnation.Take(32).All(char.IsAsciiHexDigit) ? Convert.FromHexString(database.Incarnation.AsSpan(0, 32)) : null;

    public static bool IsChanged(SqlException ex) => ex.Number == ChangedError;

    /// <summary>The recovery fork of a database that is known by id and name only, as 32 hex digits.</summary>
    public static async Task<string?> RecoveryForkAsync(SqlConnection conn, int id, string name, CancellationToken ct)
    {
        var database = new DatabaseRef(id, name, "");
        await using var cmd = Held(conn, database, "SELECT CONVERT(char(32), CONVERT(binary(16), recovery_fork_guid), 2) FROM sys.database_recovery_status WHERE database_id = @dbid;");
        try { return await cmd.ExecuteScalarAsync(ct) as string; }
        catch (SqlException ex) when (IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
    }

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
        catch (SqlException) { } // it cannot be entered now, or the server is not there: the original error tells
    }

    public static async Task VerifyAsync(SqlConnection conn, DatabaseRef database, CancellationToken ct)
    {
        await using var cmd = Held(conn, database, "");
        try { await cmd.ExecuteNonQueryAsync(ct); }
        catch (SqlException ex) when (IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
    }
}
