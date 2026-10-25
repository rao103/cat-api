using Microsoft.Extensions.Configuration;
using Amazon;
using Amazon.SQS;
using Azure.Identity;
using ProductionIngestor.Adapters.Storage;
using ProductionIngestor.Configuration;
using ProductionIngestor.Core.Constants;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Resolvers;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Process-based, opt-in live end-to-end coverage for one bounded ProductionIngestor.Worker
/// refresh run. See docs/superpowers/specs/2026-09-18-production-ingestor-e2e-design.md for the
/// approved scope and boundaries. This test never runs as part of an ordinary solution test pass:
/// it requires <see cref="LiveTestGate.OptInVariable"/>="true" plus the windfarm id, date, and
/// SQS-observation-window confirmation as explicit environment variables.
/// </summary>
public sealed class ProductionWorkerEndToEndTests
{
    [Theory]
    [InlineData(null, "2026-09-01", "true")]
    [InlineData("0", "2026-09-01", "true")]
    [InlineData("-3", "2026-09-01", "true")]
    [InlineData("3", null, "true")]
    [InlineData("3", "not-a-date", "true")]
    [InlineData("3", "2026-09-01", null)]
    [InlineData("3", "2026-09-01", "false")]
    public void OptedInWithMalformedInputsFailsBeforeContactingAnyProvider(
        string? windfarmId, string? date, string? sqsConfirmed)
    {
        WithEnvironment(LiveTestGate.WindfarmIdVariable, windfarmId,
            () => WithEnvironment(LiveTestGate.DateVariable, date,
                () => WithEnvironment(LiveTestGate.SqsObservationConfirmedVariable, sqsConfirmed,
                    () => Assert.Throws<ArgumentException>(() => LiveTestGate.ParseAndValidate()))));
    }

    [LiveFact]
    public async Task RunsBoundedRefreshAndVerifiesSourceBlobSqsAndAudit()
    {
        var request = LiveTestGate.ParseAndValidate();
        var runId = $"e2e-{Guid.NewGuid():N}";
        using var overallCancellation = new CancellationTokenSource(request.Timeout);

        var configuration = WorkerConfigurationLoader.Load();
        var workerContentRoot = WorkerConfigurationLoader.ResolveWorkerContentRoot();

        var sourceOptions = new SourceOptions
        {
            SourceSystemName = configuration["Source:SourceSystemName"] ?? "production",
            BlobContainerName = configuration["Source:BlobContainerName"]
                ?? throw new InvalidOperationException("Source:BlobContainerName must be configured."),
            BlobConnectionString = configuration["Source:BlobConnectionString"] ?? string.Empty,
            BlobServiceUri = configuration["Source:BlobServiceUri"],
            ControlPlaneConnectionString = configuration["Source:ControlPlaneConnectionString"] ?? string.Empty,
        };
        var blobSettings = BlobConnectionSettingsResolver.Resolve(sourceOptions);
        var objectStore = blobSettings.UsesConnectionString
            ? new AzureBlobRawObjectStore(blobSettings.ConnectionString!, blobSettings.ContainerName)
            : new AzureBlobRawObjectStore(blobSettings.ServiceUri!, new DefaultAzureCredential(), blobSettings.ContainerName);

        var controlPlaneOptions = new ControlPlaneOptions();
        var controlPlaneSection = configuration.GetSection(ControlPlaneOptions.SectionName);
        if (controlPlaneSection.Exists())
            controlPlaneSection.Bind(controlPlaneOptions);
        else
        {
            controlPlaneOptions.Authentication = Constant.ConnectionString;
            controlPlaneOptions.ConnectionString = sourceOptions.ControlPlaneConnectionString;
        }
        // appsettings.json ships ControlPlane:RootCertificate empty by design; the real path is only
        // ever supplied via env var (EKS ConfigMap, or "./dbconnect.ps1 debug-env" for local runs).
        // Failing here with a clear message beats letting ControlPlaneOptionsValidator's low-level
        // ArgumentException surface without pointing the developer at the fix.
        if (string.Equals(controlPlaneOptions.Authentication, Constant.AwsIam, StringComparison.OrdinalIgnoreCase)
            && string.IsNullOrWhiteSpace(controlPlaneOptions.RootCertificate))
        {
            throw new InvalidOperationException(
                "ControlPlane:RootCertificate must be configured for this live test. Run " +
                "'./dbconnect.ps1 debug-env' (see localdbconnection/README.md) or set " +
                "ControlPlane__RootCertificate to a trusted RDS CA bundle path before opting in.");
        }
        using var postgresConnectionFactory = new PostgresConnectionFactory(controlPlaneOptions);
        var auditVerifier = new AuditVerifier(postgresConnectionFactory);

        var sqsOptions = new SqsOptions();
        configuration.GetSection(SqsOptions.SectionName).Bind(sqsOptions);
        using var sqsClient = new AmazonSQSClient(new AmazonSQSConfig { RegionEndpoint = RegionEndpoint.GetBySystemName(sqsOptions.Region) });
        var sqsObservation = new SqsObservation(sqsClient, sqsOptions.RawFanOut.QueueUrl);

        using var expectedSnapshotReader = new ExpectedSnapshotReader(configuration);

        // Assertion 1/7 (before): baseline checkpoint and a nonempty pre-run source snapshot.
        var cursorBefore = await auditVerifier.ReadCursorSnapshotAsync(request.WindfarmId, "Production", overallCancellation.Token);
        var baselineRunId = await auditVerifier.ReadMaxRunIdAsync(request.WindfarmId, "Production", overallCancellation.Token);
        var expectedBefore = await expectedSnapshotReader.ReadSnapshotAsync(request.WindfarmId, request.ProductionDate, overallCancellation.Token);
        Assert.True(expectedBefore.Count > 0,
            $"No source rows for windfarm {request.WindfarmId} on {request.ProductionDate:yyyy-MM-dd}; an empty artifact cannot verify the data-flow path.");

        // Assertion 3: launch the actual Worker for the bounded refresh.
        var runResult = await WorkerProcessRunner.RunRefreshAsync(
            workerContentRoot, request.WindfarmId, request.ProductionDate, runId,
            environmentOverrides: new Dictionary<string, string>(), request.Timeout, overallCancellation.Token);

        Assert.False(runResult.TimedOut,
            $"Worker did not exit within {request.Timeout}; it may have left an expiring lease or an incomplete run. " +
            "This state was not forcibly modified. Standard output/error follow.\n" +
            $"stdout:\n{runResult.StandardOutput}\nstderr:\n{runResult.StandardError}");
        Assert.Equal(0, runResult.ExitCode);

        // Assertion 4: observe (never delete) the expected SQS notification.
        var observedMessage = await sqsObservation.ObserveAsync(runId, TimeSpan.FromSeconds(90), overallCancellation.Token);
        Assert.NotNull(observedMessage);
        Assert.Equal("Production", observedMessage!.SourceType);
        Assert.Equal(request.WindfarmId, observedMessage.WindfarmId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(request.ProductionDate.ToString("yyyy-MM-dd"), observedMessage.DataDate);
        var blobPaths = observedMessage.BlobPath is not null
            ? [observedMessage.BlobPath]
            : observedMessage.ShardPaths ?? [];
        Assert.NotEmpty(blobPaths);

        // Assertion 5: download and compare Parquet output against the expected snapshot.
        var actualRows = await ParquetDownloadVerifier.DownloadAndParseAsync(objectStore, blobPaths, overallCancellation.Token);
        AssertSnapshotsMatch(expectedBefore, actualRows);

        // Assertion 6: correlate the PostgreSQL audit row using farm/source/output-prefix.
        var completedRun = await auditVerifier.FindCompletedRunAsync(
            request.WindfarmId, "Production", observedMessage.ExtractionToken, baselineRunId, overallCancellation.Token);
        Assert.NotNull(completedRun);
        Assert.Equal("Completed", completedRun!.Status);
        Assert.NotNull(completedRun.CompletedAtUtc);

        var cursorAfter = await auditVerifier.ReadCursorSnapshotAsync(request.WindfarmId, "Production", overallCancellation.Token);
        Assert.Null(cursorAfter?.LeaseOwner);
        if (cursorBefore is not null)
        {
            Assert.Equal(cursorBefore.LastCommittedUpdatedUtc, cursorAfter!.LastCommittedUpdatedUtc);
            Assert.Equal(cursorBefore.LastCommittedId, cursorAfter.LastCommittedId);
        }
        else
        {
            Assert.NotNull(cursorAfter);
            Assert.Equal(completedRun.FromUpdatedAtUtc, cursorAfter!.LastCommittedUpdatedUtc);
        }

        // Assertion 7: re-read the source snapshot; a change during the test invalidates the comparison rather than passing silently.
        var expectedAfter = await expectedSnapshotReader.ReadSnapshotAsync(request.WindfarmId, request.ProductionDate, overallCancellation.Token);
        Assert.True(SnapshotsAreIdentical(expectedBefore, expectedAfter),
            "The source snapshot changed during the test; the before/after comparison cannot be treated as deterministic.");
    }

    private static void AssertSnapshotsMatch(
        IReadOnlyList<ProductionIngestor.Models.ProductionRowModel> expected,
        IReadOnlyList<ProductionIngestor.Models.ProductionRowModel> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        var expectedByKey = expected.ToDictionary(row => (row.WtgName, row.ProductionTimestampUtc));
        foreach (var row in actual)
        {
            Assert.True(expectedByKey.TryGetValue((row.WtgName, row.ProductionTimestampUtc), out var expectedRow),
                $"Unexpected artifact row for item '{row.WtgName}' at {row.ProductionTimestampUtc:o}.");
            Assert.Equal(expectedRow!.ActualProduction, row.ActualProduction);
            Assert.Equal(expectedRow.PossibleProduction, row.PossibleProduction);
            Assert.Equal(expectedRow.EndDateAndTimeUtc, row.EndDateAndTimeUtc);
            Assert.Equal(expectedRow.ContractualPoPEColumn, row.ContractualPoPEColumn);
            Assert.Equal(expectedRow.LastUpdatedUtc, row.LastUpdatedUtc);
        }
    }

    private static bool SnapshotsAreIdentical(
        IReadOnlyList<ProductionIngestor.Models.ProductionRowModel> before,
        IReadOnlyList<ProductionIngestor.Models.ProductionRowModel> after)
    {
        if (before.Count != after.Count)
            return false;
        var beforeByKey = before.ToDictionary(row => (row.WtgName, row.ProductionTimestampUtc));
        foreach (var row in after)
        {
            if (!beforeByKey.TryGetValue((row.WtgName, row.ProductionTimestampUtc), out var match))
                return false;
            if (match.ActualProduction != row.ActualProduction || match.PossibleProduction != row.PossibleProduction
                || match.EndDateAndTimeUtc != row.EndDateAndTimeUtc
                || match.ContractualPoPEColumn != row.ContractualPoPEColumn
                || match.LastUpdatedUtc != row.LastUpdatedUtc)
                return false;
        }
        return true;
    }

    private static void WithEnvironment(string variable, string? value, Action action)
    {
        var previous = Environment.GetEnvironmentVariable(variable);
        Environment.SetEnvironmentVariable(variable, value);
        try
        {
            action();
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, previous);
        }
    }
}
