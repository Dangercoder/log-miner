using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace LogMiner.Lab;

/// <summary>
/// Experiment and benchmark harness. Not part of the shipped service.
///   lab sql &lt;file.sql&gt; [--db name] [--maxbin N]   run a script (GO-separated), print every result set
/// </summary>
internal static partial class Program
{
    internal static string ConnectionString(string db = "master") =>
        Environment.GetEnvironmentVariable("LOGMINER_LAB_CS")
        ?? $"Data Source=localhost;Initial Catalog={db};Integrated Security=true;TrustServerCertificate=true;Application Name=logminer-lab";

    private static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("usage: lab sql <file.sql> [--db name] [--maxbin N]");
            return 2;
        }
        try
        {
            return args[0] switch
            {
                "sql" => await RunSqlFile(args[1], Opt(args, "--db") ?? "master", int.Parse(Opt(args, "--maxbin") ?? "4096", CultureInfo.InvariantCulture),
                                          int.Parse(Opt(args, "--timeout") ?? "600", CultureInfo.InvariantCulture)),
                "doorbell" => await Bench.Doorbell(args),
                "readprobe" => await Bench.ReadProbe(args),
                "bells" => await Bench2.Bells(args),
                "liveness" => await Bench2.Liveness(args),
                "pdh" => await PdhBench.Run(args),
                "e2e" => await E2EBench.Run(args),
                "xe" => await XeBench.Run(args),
                "xedump" => await XeDump.Run(args),
                "xerace" => await XeRace.Run(args),
                "xecost" => await XeCost.Run(args),
                "tick" => await TickBench.Run(args),
                "xeneighbours" => await XeNeighbours.Run(args),
                "load" => await LoadGen.Run(args),
                "service" => await ServiceBench.Run(args),
                _ => Fail($"unknown command {args[0]}"),
            };
        }
        catch (SqlException ex)
        {
            Console.Error.WriteLine($"SQL error {ex.Number} (line {ex.LineNumber}): {ex.Message}");
            return 1;
        }
    }

    internal static string? Opt(string[] args, string name)
    {
        var i = Array.IndexOf(args, name);
        return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
    }

    internal static async Task ExecAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static int Fail(string message)
    {
        Console.Error.WriteLine(message);
        return 2;
    }

    [GeneratedRegex(@"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)]
    private static partial Regex GoSeparator();

    private static async Task<int> RunSqlFile(string path, string db, int maxBin, int timeoutSeconds)
    {
        var script = await File.ReadAllTextAsync(path);
        await using var conn = new SqlConnection(ConnectionString(db));
        conn.InfoMessage += (_, e) => Console.WriteLine($"-- {e.Message}");
        conn.FireInfoMessageEventOnUserErrors = false;
        await conn.OpenAsync();
        foreach (var batch in GoSeparator().Split(script).Where(b => !string.IsNullOrWhiteSpace(b)))
        {
            var sw = Stopwatch.StartNew();
            await using var cmd = new SqlCommand(batch, conn) { CommandTimeout = timeoutSeconds };
            await using var reader = await cmd.ExecuteReaderAsync();
            do
            {
                if (reader.FieldCount == 0) continue;
                PrintResultSet(reader, maxBin);
            } while (await reader.NextResultAsync());
            if (sw.ElapsedMilliseconds > 200) Console.WriteLine($"-- batch took {sw.ElapsedMilliseconds} ms");
        }
        return 0;
    }

    private static void PrintResultSet(SqlDataReader reader, int maxBin)
    {
        var names = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        Console.WriteLine("== " + string.Join(" | ", names));
        while (reader.Read())
        {
            var sb = new StringBuilder();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (i > 0) sb.Append(" | ");
                sb.Append(Format(reader.GetValue(i), maxBin));
            }
            Console.WriteLine(sb.ToString());
        }
    }

    internal static string Format(object value, int maxBin) => value switch
    {
        DBNull => "NULL",
        byte[] b => b.Length > maxBin ? $"0x{Convert.ToHexString(b, 0, maxBin)}…({b.Length}B)" : $"0x{Convert.ToHexString(b)}",
        DateTime d => d.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };
}
