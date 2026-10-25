using Azure;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Harmonizer.Core.Adapters;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.Json;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Observability;
using Harmonizer.Worker.Observability;
using Harmonizer.Worker.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace Harmonizer.UnitTests;

// Console capture and the shared ActivitySource require isolation from other telemetry tests.
[CollectionDefinition("Telemetry", DisableParallelization = true)]
public sealed class TelemetryCollection;

[Collection("Telemetry")]
public sealed class TelemetryTests
{
    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void Invalid_sampling_ratio_is_rejected(double ratio)
    {
        var options = new TelemetryOptions { TraceSampleRatio = ratio };
        Assert.Throws<ArgumentException>(() => options.Validate(null));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("ftp://collector")]
    [InlineData("https://user:password@collector")]
    [InlineData("https://collector?secret=value")]
    [InlineData("https://collector/#fragment")]
    public void Export_requires_a_valid_endpoint_without_embedded_secrets(string? endpoint)
    {
        var options = new TelemetryOptions { ExportEnabled = true };
        Assert.Throws<ArgumentException>(() => options.Validate(endpoint));
    }

    [Fact]
    public void Disabled_export_needs_no_endpoint_but_still_validates_service_metadata()
    {
        new TelemetryOptions().Validate(null);
        Assert.Throws<ArgumentException>(() => new TelemetryOptions { ServiceName = " " }.Validate(null));
        Assert.Throws<ArgumentException>(() => new TelemetryOptions { ServiceVersion = "" }.Validate(null));
    }

    [Theory]
    [InlineData("http://localhost:4317", 0)]
    [InlineData("https://collector.example:4318", 1)]
    public void Valid_endpoint_and_sampling_boundaries_are_accepted(string endpoint, double ratio)
    {
        new TelemetryOptions { ExportEnabled = true, TraceSampleRatio = ratio }.Validate(endpoint);
    }

    [Fact]
    public void Host_registration_fails_before_start_for_invalid_telemetry()
    {
        var builder = CreateBuilder();
        builder.Configuration["Telemetry:TraceSampleRatio"] = "-1";
        Assert.Throws<ArgumentException>(() => builder.AddHarmonizerTelemetry());
    }

    [Fact]
    public void All_hosted_services_and_core_adapters_have_registered_telemetry_dependencies()
    {
        var builder = CreateBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Harmonizer:QueueUrl"] = "https://sqs.example.invalid/queue",
            ["Harmonizer:BlobContainerUrl"] = "https://raw.example.invalid/container",
            ["Harmonizer:OutputBlobContainerUrl"] = "https://harmonized.example.invalid/container",
            ["Harmonizer:PostgresConnectionString"] = "Host=localhost;Username=test;Database=test",
            ["Harmonizer:PublishNotifications"] = "true",
            ["Harmonizer:NotificationQueueUrl"] = "https://sqs.example.invalid/notifications"
        });
        builder.AddHarmonizerTelemetry();
        builder.Services.AddHarmonizer(builder.Configuration);

        // Validates constructors for both background workers, repositories, adapters,
        // and Production handlers without opening third-party connections.
        using var provider = builder.Services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true
        });
        Assert.NotNull(provider.GetRequiredService<ITelemetry<TelemetryTests>>());
    }

    [Fact]
    public void Logs_preserve_utc_timestamp_scope_structured_values_exception_and_trace_correlation()
    {
        var builder = CreateBuilder();
        builder.AddHarmonizerTelemetry();
        var capture = new LogCapture();
        builder.Logging.AddOpenTelemetry(logging => logging.AddProcessor(capture));
        using var host = builder.Build();
        var telemetry = host.Services.GetRequiredService<ITelemetry<TelemetryTests>>();
        var tracer = host.Services.GetRequiredService<TracerProvider>();
        var loggerProvider = Assert.Single(host.Services.GetServices<ILoggerProvider>().OfType<OpenTelemetryLoggerProvider>());
        var before = DateTime.UtcNow;
        var expectedException = new InvalidOperationException("Injected diagnostic failure");
        ActivityTraceId expectedTrace;
        ActivitySpanId expectedSpan;
        using (var activity = HarmonizerDiagnostics.Activities.StartActivity("test.operation"))
        {
            Assert.NotNull(activity);
            expectedTrace = activity.TraceId;
            expectedSpan = activity.SpanId;
            using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["BatchId"] = "batch-test", ["OperationId"] = "OP-001", ["ExecutionId"] = "EXEC-001" });
            telemetry.Error(expectedException, "Failed turbine {WtgId}", 42);
        }

        var record = Assert.Single(capture.Records);
        Assert.InRange(record.Timestamp, before, DateTime.UtcNow);
        Assert.Equal(DateTimeKind.Utc, record.Timestamp.Kind);
        Assert.Equal(expectedTrace, record.TraceId);
        Assert.Equal(expectedSpan, record.SpanId);
        Assert.Equal(42, record.Attributes["WtgId"]);
        Assert.Equal("Failed turbine {WtgId}", record.Attributes["{OriginalFormat}"]);
        Assert.Equal("Failed turbine 42", record.FormattedMessage);
        Assert.Equal("batch-test", record.Scopes["BatchId"]);
        Assert.Equal("OP-001", record.Scopes["OperationId"]);
        Assert.Equal("EXEC-001", record.Scopes["ExecutionId"]);
        Assert.Same(expectedException, record.Exception);
        Assert.Equal(LogLevel.Error, record.Level);
        AssertResource(loggerProvider);
        AssertResource(tracer);
    }

    [Fact]
    public async Task Early_heartbeat_uses_validated_correlation_for_renewal_and_failure_logs()
    {
        var builder = CreateBuilder();
        builder.AddHarmonizerTelemetry();
        var capture = new LogCapture();
        builder.Logging.AddOpenTelemetry(logging => logging.AddProcessor(capture));
        using var host = builder.Build();
        var telemetry = host.Services.GetRequiredService<ITelemetry<Harmonizer.Core.Operations.AsyncHeartbeat>>();
        using var operation = new CancellationTokenSource();
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = operation.Token.Register(() => cancelled.TrySetResult());
        var identity = Harmonizer.Core.Models.OperationIdentity.Untracked;
        await using var heartbeat = new Harmonizer.Core.Operations.AsyncHeartbeat(TimeSpan.FromMilliseconds(50), _ =>
        {
            telemetry.Information("Renewing visibility.");
            throw new IOException("Injected renewal failure");
        }, operation, telemetry, () =>
        {
            var current = Volatile.Read(ref identity);
            return new Dictionary<string, object?> { ["OperationId"] = current.OperationId, ["ExecutionId"] = current.ExecutionId };
        });
        Volatile.Write(ref identity, new("OP-HEARTBEAT", "EXEC-HEARTBEAT"));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<IOException>(() => heartbeat.StopAsync());
        Assert.Equal(2, capture.Records.Count);
        Assert.All(capture.Records, record =>
        {
            Assert.Equal("OP-HEARTBEAT", record.Scopes["OperationId"]);
            Assert.Equal("EXEC-HEARTBEAT", record.Scopes["ExecutionId"]);
        });
    }

    [Fact]
    public async Task Raw_blob_read_failure_has_storage_and_operation_context()
    {
        var builder = CreateBuilder();
        builder.AddHarmonizerTelemetry();
        var capture = new LogCapture();
        builder.Logging.AddOpenTelemetry(logging => logging.AddProcessor(capture));
        using var host = builder.Build();
        var telemetry = host.Services.GetRequiredService<ITelemetry<BlobObjectStore>>();
        using var handler = new BlobReadFailureHandler();
        var clientOptions = new BlobClientOptions { Transport = new HttpClientTransport(handler) };
        clientOptions.Retry.MaxRetries = 0;
        var store = new BlobObjectStore(new BlobContainerClient(new Uri("https://raw.example.invalid/inputs"), clientOptions), telemetry, "Raw");
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["OperationId"] = "OP-RAW", ["ExecutionId"] = "EXEC-RAW" });
        await Assert.ThrowsAsync<RequestFailedException>(() => store.OpenReadAsync("raw/missing.parquet", default));
        var record = Assert.Single(capture.Records, record => record.Attributes.ContainsKey("BlobPath"));
        Assert.Equal("Raw", record.Scopes["StorageRole"]);
        Assert.Equal("OP-RAW", record.Scopes["OperationId"]);
        Assert.Equal("EXEC-RAW", record.Scopes["ExecutionId"]);
        Assert.Equal(LogLevel.Warning, record.Level);
        Assert.IsType<RequestFailedException>(record.Exception);
        Assert.Equal("/inputs/raw/missing.parquet", handler.RequestPath);
    }

    [Fact]
    public async Task Blob_writes_preserve_immutable_condition_storage_role_operation_scope_and_utc_logs()
    {
        var builder = CreateBuilder();
        builder.AddHarmonizerTelemetry();
        builder.Logging.SetMinimumLevel(LogLevel.Debug);
        var capture = new LogCapture();
        builder.Logging.AddOpenTelemetry(logging => logging.AddProcessor(capture));
        using var host = builder.Build();
        var telemetry = host.Services.GetRequiredService<ITelemetry<BlobObjectStore>>();
        using var handler = new BlobWriteHandler();
        var clientOptions = new BlobClientOptions { Transport = new HttpClientTransport(handler) };
        clientOptions.Retry.MaxRetries = 0;
        var client = new BlobContainerClient(new Uri("https://silver.example.invalid/outputs"), clientOptions);
        var store = new BlobObjectStore(client, telemetry, "Harmonized");
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["OperationId"] = "OP-STORAGE", ["ExecutionId"] = "EXEC-STORAGE" });
        var before = DateTime.UtcNow;
        using var content = new MemoryStream("{}"u8.ToArray());
        await store.WriteNewAsync("candidate/snapshot.json", content, "application/json", default);
        content.Position = 0;
        await Assert.ThrowsAsync<RequestFailedException>(() => store.WriteNewAsync("candidate/snapshot.json", content, "application/json", default));
        Assert.Equal(2, handler.Requests.Count);
        Assert.All(handler.Requests, request =>
        {
            Assert.Equal("/outputs/candidate/snapshot.json", request.Path);
            Assert.Equal("*", request.Condition);
        });
        var records = capture.Records.Where(record => record.Attributes.ContainsKey("BlobPath")).ToArray();
        Assert.Equal(2, records.Length);
        Assert.All(records, record =>
        {
            Assert.Equal("Harmonized", record.Scopes["StorageRole"]);
            Assert.Equal("OP-STORAGE", record.Scopes["OperationId"]);
            Assert.Equal("EXEC-STORAGE", record.Scopes["ExecutionId"]);
            Assert.InRange(record.Timestamp, before, DateTime.UtcNow);
            Assert.Equal(DateTimeKind.Utc, record.Timestamp.Kind);
        });
        Assert.Contains(records, record => record.Level == LogLevel.Debug);
        Assert.Contains(records, record => record.Level == LogLevel.Warning && record.Exception is RequestFailedException);
    }

    [Fact]
    public void Console_emits_one_json_record_with_utc_timestamp_and_scope()
    {
        var previousOutput = Console.Out;
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        Console.SetOut(output);
        try
        {
            var builder = CreateBuilder();
            builder.AddHarmonizerTelemetry();
            using (var host = builder.Build())
            {
                var options = host.Services.GetRequiredService<IOptionsMonitor<JsonConsoleFormatterOptions>>().CurrentValue;
                Assert.True(options.UseUtcTimestamp);
                Assert.True(options.IncludeScopes);
                Assert.Equal("yyyy-MM-ddTHH:mm:ss.fffZ", options.TimestampFormat);
                Assert.Single(host.Services.GetServices<ILoggerProvider>().OfType<ConsoleLoggerProvider>());
                var telemetry = host.Services.GetRequiredService<ITelemetry<TelemetryTests>>();
                using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["MessageId"] = "message-test" });
                telemetry.Information("Applied {Count} items", 2);
            } // Disposing the host drains the console provider queue.

            var line = Assert.Single(output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries));
            using var document = JsonDocument.Parse(line);
            var record = document.RootElement;
            Assert.True(DateTime.TryParseExact(record.GetProperty("Timestamp").GetString(),
                "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out _));
            Assert.Equal("Applied 2 items", record.GetProperty("Message").GetString());
            Assert.Contains("message-test", record.GetProperty("Scopes").GetRawText());
        }
        finally
        {
            Console.SetOut(previousOutput);
        }
    }

    [Fact]
    public void Zero_ratio_preserves_a_sampled_parent_but_does_not_sample_new_roots()
    {
        var builder = CreateBuilder();
        builder.Configuration["Telemetry:TraceSampleRatio"] = "0";
        builder.AddHarmonizerTelemetry();
        using var host = builder.Build();
        _ = host.Services.GetRequiredService<TracerProvider>();
        using (var root = HarmonizerDiagnostics.Activities.StartActivity("test.root", ActivityKind.Internal, default(ActivityContext)))
        {
            Assert.False(root?.Recorded ?? false);
        }

        var parent = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded, isRemote: true);
        using var child = HarmonizerDiagnostics.Activities.StartActivity("test.child", ActivityKind.Internal, parent);
        Assert.NotNull(child);
        Assert.True(child.Recorded);
    }

    [Fact]
    public void Enabled_otlp_exporter_sends_logs_traces_and_metrics_with_service_identity()
    {
        // Inject an HTTP transport so the exporter can be exercised without a live collector.
        var builder = CreateBuilder();
        builder.Configuration["Telemetry:ExportEnabled"] = "true";
        var requests = new ConcurrentQueue<(string Path, byte[] Body)>();
        var handler = new RecordingHandler(requests);
        var previousEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
        var previousProtocol = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_PROTOCOL");
        try
        {
            Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318");
            Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf");
            builder.AddHarmonizerTelemetry();
            builder.Services.PostConfigureAll<OtlpExporterOptions>(options =>
            {
                options.Protocol = OtlpExportProtocol.HttpProtobuf;
                options.HttpClientFactory = () => new HttpClient(handler, disposeHandler: false);
            });

            using (var host = builder.Build())
            {
                var tracer = host.Services.GetRequiredService<TracerProvider>();
                var meter = host.Services.GetRequiredService<MeterProvider>();
                var telemetry = host.Services.GetRequiredService<ITelemetry<TelemetryTests>>();
                using (HarmonizerDiagnostics.Activities.StartActivity("test.otlp", ActivityKind.Consumer))
                {
                    telemetry.Information("OTLP correlation {BatchId}", "test-batch");
                    HarmonizerDiagnostics.Committed.Add(1, new KeyValuePair<string, object?>("status", "complete"));
                }

                Assert.True(tracer.ForceFlush());
                Assert.True(meter.ForceFlush(), string.Join(", ", requests.Select(request => request.Path)));
            }
            // Disposing the host flushes the log provider's batch exporter.
            foreach (var path in new[] { "/v1/logs", "/v1/traces", "/v1/metrics" })
            {
                Assert.Contains(requests, request => request.Path == path);
                var sent = requests.First(request => request.Path == path);
                Assert.NotEmpty(sent.Body);
                Assert.Contains("cat-harmonizer-test", System.Text.Encoding.UTF8.GetString(sent.Body));
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT", previousEndpoint);
            Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_PROTOCOL", previousProtocol);
            handler.Dispose();
        }
    }

    private static HostApplicationBuilder CreateBuilder()
    {
        var builder = new HostApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Telemetry:ServiceName"] = "cat-harmonizer-test",
            ["Telemetry:ServiceVersion"] = "test-version",
            ["Telemetry:ExportEnabled"] = "false",
            ["HOSTNAME"] = "test-instance"
        });
        return builder;
    }

    private static void AssertResource(BaseProvider provider)
    {
        var attributes = provider.GetResource().Attributes.ToDictionary(pair => pair.Key, pair => pair.Value);
        Assert.Equal("cat-harmonizer-test", attributes["service.name"]);
        Assert.Equal("test-version", attributes["service.version"]);
        Assert.Equal("test-instance", attributes["service.instance.id"]);
    }

    private sealed class LogCapture : BaseProcessor<LogRecord>
    {
        public ConcurrentQueue<CapturedLog> Records { get; } = new();

        public override void OnEnd(LogRecord data)
        {
            var scopes = new Dictionary<string, object?>();
            data.ForEachScope((scope, destination) =>
            {
                foreach (var value in scope)
                {
                    destination[value.Key] = value.Value;
                }
            }, scopes);
            // Copy during the callback: the SDK may recycle LogRecord objects after processing.
            Records.Enqueue(new CapturedLog(data.Timestamp, data.TraceId, data.SpanId,
                data.Attributes?.ToDictionary(pair => pair.Key, pair => pair.Value) ?? [],
                scopes, data.FormattedMessage, data.Exception, data.LogLevel));
        }
    }

    private sealed record CapturedLog(
        DateTime Timestamp, ActivityTraceId TraceId, ActivitySpanId SpanId,
        IReadOnlyDictionary<string, object?> Attributes, IReadOnlyDictionary<string, object?> Scopes,
        string? FormattedMessage, Exception? Exception, LogLevel Level);

    private sealed class BlobReadFailureHandler : HttpMessageHandler
    {
        public string? RequestPath { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RequestPath = request.RequestUri!.AbsolutePath;
            var response = new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new StringContent("<Error><Code>BlobNotFound</Code><Message>Missing Raw blob</Message></Error>")
            };
            response.Headers.TryAddWithoutValidation("x-ms-request-id", "test-request");
            response.Headers.TryAddWithoutValidation("x-ms-error-code", "BlobNotFound");
            return Task.FromResult(response);
        }
    }

    private sealed class BlobWriteHandler : HttpMessageHandler
    {
        public List<(string Path, string Condition)> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Requests.Add((request.RequestUri!.AbsolutePath, Assert.Single(request.Headers.GetValues("If-None-Match"))));
            var response = new HttpResponseMessage(Requests.Count == 1 ? HttpStatusCode.Created : HttpStatusCode.PreconditionFailed)
            {
                Content = new StringContent(Requests.Count == 1 ? "" : "<Error><Code>BlobAlreadyExists</Code><Message>Existing immutable blob</Message></Error>")
            };
            response.Headers.TryAddWithoutValidation("x-ms-request-id", "test-request");
            response.Headers.TryAddWithoutValidation("ETag", "\"test-etag\"");
            response.Headers.TryAddWithoutValidation("Last-Modified", "Sun, 04 Oct 2026 09:00:00 GMT");
            if (Requests.Count > 1) response.Headers.TryAddWithoutValidation("x-ms-error-code", "BlobAlreadyExists");
            return Task.FromResult(response);
        }
    }

    private sealed class RecordingHandler(ConcurrentQueue<(string Path, byte[] Body)> requests) : HttpMessageHandler
    {
        protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content!.ReadAsByteArrayAsync(cancellationToken).GetAwaiter().GetResult();
            requests.Enqueue((request.RequestUri!.AbsolutePath, body));
            return new HttpResponseMessage(HttpStatusCode.OK);
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadAsByteArrayAsync(cancellationToken);
            requests.Enqueue((request.RequestUri!.AbsolutePath, body));
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
    }
}
