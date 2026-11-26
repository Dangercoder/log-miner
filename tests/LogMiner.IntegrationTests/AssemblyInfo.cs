// The integration tests share one SQL Server instance (msdb job metadata, CPU for the concurrent-write tests): run them serially.
[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]
