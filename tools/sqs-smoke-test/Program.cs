// Throwaway SQS smoke-test harness. Reuses real production ingestor code; not part of the solution.
using System.Runtime.CompilerServices;
using System.Text.Json;
using Amazon;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using ProductionIngestor.Adapters.Messaging;
using ProductionIngestor.Configuration;
using ProductionIngestor.Contracts;
using ProductionIngestor.Validators;

// Anchored to this source file rather than CWD, so it works regardless of where `dotnet run` is invoked from.
static string GetSourceFilePath([CallerFilePath] string path = "") => path;
var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(GetSourceFilePath())!, "..", ".."));
var repoAppSettingsPath = Path.Combine(repoRoot, "src", "Raw", "Ingestors", "Production", "ProductionIngestor.Worker", "appsettings.json");

Console.WriteLine("=== Production Ingestor SQS smoke test ===");
Console.WriteLine($"AWS_PROFILE = {Environment.GetEnvironmentVariable("AWS_PROFILE")}");
Console.WriteLine();

var results = new List<(string Step, bool Passed)>();
string? runId = null;
string? callerArn = null;
bool cleanupSucceeded = false;

// Step 1: load config
SqsOptions? options = null;
try
{
    var configuration = new ConfigurationBuilder()
        .AddJsonFile(repoAppSettingsPath, optional: false, reloadOnChange: false)
        .Build();

    options = new SqsOptions();
    configuration.GetSection(SqsOptions.SectionName).Bind(options);

    Console.WriteLine("[1/6] Load config: PASS");
    Console.WriteLine($"      Region   = {options.Region}");
    Console.WriteLine($"      QueueUrl = {options.RawFanOut.QueueUrl}");
    results.Add(("Load config", true));
}
catch (Exception ex)
{
    Console.WriteLine("[1/6] Load config: FAIL");
    Console.WriteLine($"      {ex.GetType().FullName}: {ex.Message}");
    results.Add(("Load config", false));
}

// Step 2: validate
if (options is not null)
{
    try
    {
        SqsOptionsValidator.ValidateAndThrow(options);
        Console.WriteLine("[2/6] Validate options: PASS");
        results.Add(("Validate options", true));
    }
    catch (Exception ex)
    {
        Console.WriteLine("[2/6] Validate options: FAIL");
        Console.WriteLine($"      {ex.GetType().FullName}: {ex.Message}");
        results.Add(("Validate options", false));
    }
}
else
{
    Console.WriteLine("[2/6] Validate options: SKIPPED (no config)");
    results.Add(("Validate options", false));
}

if (options is null || !results[1].Passed)
{
    PrintSummaryAndExit();
    return;
}

// Step 3: resolve caller identity (diagnostics only)
try
{
    using var stsClient = new AmazonSecurityTokenServiceClient(RegionEndpoint.GetBySystemName(options.Region));
    var identity = await stsClient.GetCallerIdentityAsync(new GetCallerIdentityRequest());
    callerArn = identity.Arn;
    Console.WriteLine("[3/6] Resolve caller identity: PASS");
    Console.WriteLine($"      Arn = {callerArn}");
    results.Add(("Resolve caller identity", true));
}
catch (Exception ex)
{
    Console.WriteLine("[3/6] Resolve caller identity: FAIL");
    Console.WriteLine($"      {ex.GetType().FullName}: {ex.Message}");
    results.Add(("Resolve caller identity", false));
}

// Step 4: construct SQS client (mirrors ProductionMessagingConfiguration.AddProductionMessaging)
IAmazonSQS? sqsClient = null;
try
{
    var sqsConfig = new AmazonSQSConfig
    {
        RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region)
    };
    sqsClient = new AmazonSQSClient(sqsConfig);
    Console.WriteLine("[4/6] Construct AmazonSQSClient: PASS");
    results.Add(("Construct AmazonSQSClient", true));
}
catch (Exception ex)
{
    Console.WriteLine("[4/6] Construct AmazonSQSClient: FAIL");
    Console.WriteLine($"      {ex.GetType().FullName}: {ex.Message}");
    results.Add(("Construct AmazonSQSClient", false));
}

if (sqsClient is null)
{
    PrintSummaryAndExit();
    return;
}

// Step 5: publish via the real production publisher
runId = $"smoketest-{Guid.NewGuid()}";
var dataDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
var extractionToken = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
var message = new RawArtifactAvailableV1
{
    SourceType = "Production",
    WindfarmId = 1,
    WindfarmName = "SMOKETEST",
    DataDate = dataDate,
    ExtractionToken = extractionToken,
    BlobPath = $"/raw/production/windfarm=SMOKETEST/date={dataDate}/extraction={extractionToken}/production.parquet",
    RunId = runId,
    TraceId = Guid.NewGuid().ToString()
};

try
{
    var publisher = new SqsRawArtifactPublisher(sqsClient, options.RawFanOut.QueueUrl);
    await publisher.PublishAsync(message, CancellationToken.None);
    Console.WriteLine("[5/6] Publish RawArtifactAvailableV1: PASS");
    Console.WriteLine($"      RunId = {runId}");
    results.Add(("Publish message", true));
}
catch (Exception ex)
{
    Console.WriteLine("[5/6] Publish RawArtifactAvailableV1: FAIL");
    Console.WriteLine($"      {ex.GetType().FullName}: {ex.Message}");
    results.Add(("Publish message", false));
    PrintSummaryAndExit();
    return;
}

// Step 6: cleanup — find and delete only the message this harness sent
var skipCleanup = string.Equals(Environment.GetEnvironmentVariable("SKIP_CLEANUP"), "1", StringComparison.Ordinal);
if (skipCleanup)
{
    Console.WriteLine("[6/6] Cleanup (receive + delete): SKIPPED (SKIP_CLEANUP=1)");
    Console.WriteLine($"      Message left on queue for manual confirmation. RunId = {runId}");
    results.Add(("Cleanup", true));
    PrintSummaryAndExit();
    return;
}

try
{
    var found = false;
    for (var attempt = 1; attempt <= 3 && !found; attempt++)
    {
        var receiveResponse = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = options.RawFanOut.QueueUrl,
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 5
        });

        foreach (var received in receiveResponse.Messages)
        {
            if (received.Body.Contains(runId, StringComparison.Ordinal))
            {
                await sqsClient.DeleteMessageAsync(new DeleteMessageRequest
                {
                    QueueUrl = options.RawFanOut.QueueUrl,
                    ReceiptHandle = received.ReceiptHandle
                });
                found = true;
                break;
            }
        }
    }

    if (found)
    {
        cleanupSucceeded = true;
        Console.WriteLine("[6/6] Cleanup (receive + delete): PASS");
        results.Add(("Cleanup", true));
    }
    else
    {
        Console.WriteLine("[6/6] Cleanup (receive + delete): WARNING");
        Console.WriteLine($"      Smoke-test message not found after 3 receive attempts.");
        Console.WriteLine($"      A stray message may remain on the queue. Search for RunId: {runId}");
        results.Add(("Cleanup", false));
    }
}
catch (Exception ex)
{
    Console.WriteLine("[6/6] Cleanup (receive + delete): FAIL");
    Console.WriteLine($"      {ex.GetType().FullName}: {ex.Message}");
    Console.WriteLine($"      Search for RunId: {runId}");
    results.Add(("Cleanup", false));
}

PrintSummaryAndExit();

void PrintSummaryAndExit()
{
    Console.WriteLine();
    Console.WriteLine("=== Summary ===");
    foreach (var (step, passed) in results)
    {
        Console.WriteLine($"  {(passed ? "PASS" : "FAIL/WARN")} - {step}");
    }
    if (callerArn is not null)
        Console.WriteLine($"  Caller ARN: {callerArn}");
    if (runId is not null)
        Console.WriteLine($"  RunId: {runId} (cleanup {(cleanupSucceeded ? "succeeded" : "NOT confirmed - see above")})");
}
