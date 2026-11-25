using System.Diagnostics;
using System.Globalization;
using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace LogMiner.Verify;

/// <summary>
/// logminer-verify: reads a JetStream stream from its first message with an ordered, ephemeral consumer (the only thing it creates on the
/// server; it acknowledges nothing and publishes nothing) and says whether the stream matches its tables.
/// Exit code 0: no difference; 1: differences (or messages that cannot be read); 2: usage, or the stream cannot be read; 3 (--strict only): no
/// difference, but a table is not verified.
/// </summary>
internal static class Program
{
    private const string Usage = """
        logminer-verify --stream <name> [--url nats://127.0.0.1:4222] [--subject cdc.>] [--creds <file>] [--follow] [--json] [--strict]
                        [--examples 5] [--replay-window 100000] [--ignore-load-rows]

          Reads the stream from its first message and compares it with its tables: every change against the row the stream had (chain), and
          every historical load, row by row, against the state the stream has built.
          --follow            keep running; report differences as they come; Ctrl+C prints the report and exits with 0
          --json              the report as JSON on standard output
          --strict            exit code 3 when there is no difference but a table is not verified (no complete load to compare with)
          --examples          examples kept per table and kind of difference
          --replay-window     message ids of load rows remembered per database to pass over repeats (0: none)
          --ignore-load-rows  do not compare the number of rows a load announces with the rows that arrived
          exit code: 0 no difference, 1 differences, 2 usage or connection
        """;

    private static async Task<int> Main(string[] args)
    {
        string url = "nats://127.0.0.1:4222", subject = "cdc.>";
        string? stream = null, creds = null;
        bool follow = false, json = false, strict = false, loadRows = true;
        int examples = 5, window = 100_000;
        try
        {
            for (var i = 0; i < args.Length; i++)
            {
                string Value() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{args[i]} needs a value");
                switch (args[i])
                {
                    case "--url": url = Value(); break;
                    case "--stream": stream = Value(); break;
                    case "--subject": subject = Value(); break;
                    case "--creds": creds = Value(); break;
                    case "--follow": follow = true; break;
                    case "--json": json = true; break;
                    case "--strict": strict = true; break;
                    case "--ignore-load-rows": loadRows = false; break;
                    case "--examples": examples = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                    case "--replay-window": window = int.Parse(Value(), CultureInfo.InvariantCulture); break;
                    case "--help" or "-h" or "-?": Console.Out.WriteLine(Usage); return 0;
                    default: throw new ArgumentException($"unknown argument {args[i]}");
                }
            }
            if (stream is null) throw new ArgumentException("--stream is required");
            if (creds is not null && !File.Exists(creds)) throw new ArgumentException($"credentials file {creds} not found");
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or OverflowException)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine(Usage);
            return 2;
        }

        using var stop = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
        var verifier = new StreamVerifier(new VerifierOptions { Examples = examples, ReplayWindow = window, CheckLoadRows = loadRows });
        // while following, a difference is said when it is found; the report comes at the end
        if (follow) verifier.OnDifference = d => Console.Error.WriteLine($"DIFFERENT {d}");
        var notes = new List<string>();
        try
        {
            await ReadAsync(verifier, url, stream, subject, creds, follow, notes, stop.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
            if (!follow) { Console.Error.WriteLine("interrupted before the end of the stream: nothing is said about it"); return 2; }
        }
        catch (Exception ex) when (ex is NatsException or TimeoutException or IOException or System.Net.Sockets.SocketException or OperationCanceledException)
        {
            Console.Error.WriteLine($"cannot read stream {stream} at {url}: {ex.GetType().Name}: {ex.Message}");
            return 2;
        }

        var report = verifier.Report();
        if (json) Console.Out.WriteLine(report.ToJson());
        else
        {
            foreach (var n in notes) Console.Out.WriteLine(n);
            Console.Out.Write(report.ToText());
            Console.Out.WriteLine(!report.Clean ? "RESULT: DIFFERENT - the stream does not match its tables"
                : report.Tables.Count == 0 ? "RESULT: nothing to verify"
                : report.Count(Verdict.NotVerifiedYet) + report.Count(Verdict.CannotBeVerified) > 0 ? "RESULT: no difference; not every table is verified (see above)"
                : "RESULT: verified - every table was compared with a historical load and nothing differs");
        }
        if (follow) return 0;
        return !report.Clean ? 1 : strict && report.Count(Verdict.NotVerifiedYet) + report.Count(Verdict.CannotBeVerified) > 0 ? 3 : 0;
    }

    private static async Task ReadAsync(StreamVerifier verifier, string url, string streamName, string subject, string? creds, bool follow, List<string> notes, CancellationToken ct)
    {
        var nats = new NatsConnection(NatsOpts.Default with
        {
            Url = url,
            Name = $"logminer-verify@{Environment.MachineName}",
            AuthOpts = creds is null ? NatsAuthOpts.Default : NatsAuthOpts.Default with { CredsFile = creds },
            HeaderEncoding = Encoding.UTF8,
            // one reader that is never faster than the server: wait, never drop
            SubPendingChannelFullMode = System.Threading.Channels.BoundedChannelFullMode.Wait,
        });
        await using (nats.ConfigureAwait(false))
        {
            await nats.ConnectAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
            var js = new NatsJSContext(nats);
            var stream = await js.GetStreamAsync(streamName, cancellationToken: ct).ConfigureAwait(false);
            var state = stream.Info.State;
            notes.Add(string.Create(CultureInfo.InvariantCulture, $"stream {streamName} at {url}: {state.Messages} message(s), sequence {state.FirstSeq} to {state.LastSeq}, the first of {state.FirstTs}; subject {subject}"));
            if (state.FirstSeq > 1) notes.Add(string.Create(CultureInfo.InvariantCulture, $"the stream no longer holds its first {state.FirstSeq - 1} message(s) (limits or a purge): a table is verified from its first complete load that is still in the stream"));
            if (state.NumDeleted > 0) notes.Add(string.Create(CultureInfo.InvariantCulture, $"{state.NumDeleted} message(s) inside the stream are gone (deleted, or removed by a limit per subject)"));
            foreach (var n in notes) Console.Error.WriteLine(n);

            var consumer = await js.CreateOrderedConsumerAsync(streamName, new NatsJSOrderedConsumerOpts { FilterSubjects = [subject], DeliverPolicy = ConsumerConfigDeliverPolicy.All }, ct).ConfigureAwait(false);
            var clock = Stopwatch.StartNew();
            var said = TimeSpan.Zero;
            ulong pending = 0;
            var empty = 0;
            while (true)
            {
                // what is there now, without waiting: an empty answer is the end of the stream as the server sees it
                var got = 0;
                await foreach (var m in consumer.FetchNoWaitAsync<NatsMemoryOwner<byte>>(new NatsJSFetchOpts { MaxMsgs = 4096 }, cancellationToken: ct).ConfigureAwait(false))
                {
                    using var data = m.Data;
                    got++;
                    string? id = null;
                    if (m.Headers is { } headers && headers.TryGetValue("Nats-Msg-Id", out var ids) && ids.Count > 0) id = ids[0];
                    verifier.Add(m.Subject, data.Span, id, m.Metadata?.Sequence.Stream ?? 0, m.Metadata?.Timestamp ?? default);
                    pending = m.Metadata?.NumPending ?? 0;
                }
                if (clock.Elapsed - said > TimeSpan.FromSeconds(10))
                {
                    said = clock.Elapsed;
                    Console.Error.WriteLine(string.Create(CultureInfo.InvariantCulture, $"{verifier.Messages} messages read, {verifier.Differences} difference(s) so far"));
                }
                if (got > 0) { empty = 0; continue; }
                // the server said more was to come and then gave nothing: asked again, and never taken for the end ("verified" over a part)
                if (pending > 0 && ++empty <= 20) { await Task.Delay(250, ct).ConfigureAwait(false); continue; }
                if (pending > 0) throw new TimeoutException(string.Create(CultureInfo.InvariantCulture, $"the server holds {pending} more message(s) behind sequence {verifier.Report().LastSequence} and does not deliver them"));
                if (!follow) return;
                await Task.Delay(250, ct).ConfigureAwait(false);
            }
        }
    }
}
