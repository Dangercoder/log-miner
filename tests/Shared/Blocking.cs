namespace LogMiner.Tests;

/// <summary>
/// The only place where a test blocks on asynchronous code. Hegel (0.4.1) takes the body of a property, and the rules of a stateful one, as
/// synchronous methods, with no asynchronous overload; an async lambda would become async void, whose assertions run after the property
/// has already passed. The work runs on the pool, without the test framework's synchronization context: xUnit's lets only so many threads
/// in, so a continuation posted to it could wait for the very thread that is blocked here.
/// Production code never does this.
/// </summary>
internal static class Blocking
{
    public static T Run<T>(Func<Task<T>> work) => Task.Run(work).GetAwaiter().GetResult();
    public static void Run(Func<Task> work) => Task.Run(work).GetAwaiter().GetResult();
}
