using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Contracts;
using ProductionIngestor.Models;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Adapters.Messaging;
using ProductionIngestor.Worker.Configuration;
using ProductionIngestor.Validators;
using System.Text.Json;

namespace ProductionIngestor.UnitTests;

public sealed class SqsPublishingTests
{
    private const string QueueUrl = "https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev";

    [Fact]
    public async Task PublisherPropagatesW3CTraceContextWithoutChangingTheEnvelope()
    {
        using var activity = new System.Diagnostics.Activity("assignment")
            .SetIdFormat(System.Diagnostics.ActivityIdFormat.W3C).Start();
        activity.TraceStateString = "vendor=value";
        using var client = new StubSqsClient();
        var message = CreateMessage();
        await new SqsRawArtifactPublisher(client, QueueUrl).PublishAsync(message, default);
        var request = Assert.Single(client.Requests);
        Assert.Equal(activity.Id, request.MessageAttributes["traceparent"].StringValue);
        Assert.Equal(activity.TraceStateString, request.MessageAttributes["tracestate"].StringValue);
        Assert.Equal(JsonSerializer.Serialize(message), request.MessageBody);
    }

    [Fact]
    public void DevelopmentRawFanOutQueueIsValid()
    {
        SqsOptionsValidator.ValidateAndThrow(CreateOptions());
    }

    [Fact]
    public async Task WorkerAppSettingsConfigureAValidRawFanOutPublisher()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "ProductionIngestor.Worker.appsettings.json"), optional: false)
            .Build();
        var options = configuration.GetSection(SqsOptions.SectionName).Get<SqsOptions>();

        Assert.NotNull(options);
        SqsOptionsValidator.ValidateAndThrow(options);

        using var client = new StubSqsClient();
        var publisher = new SqsRawArtifactPublisher(client, options.RawFanOut.QueueUrl);
        await publisher.PublishAsync(CreateMessage(), CancellationToken.None);

        Assert.Equal(options.RawFanOut.QueueUrl, Assert.Single(client.Requests).QueueUrl);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("not-a-region")]
    [InlineData("eu-west-1")]
    public void MissingUnknownOrMismatchedRegionIsRejected(string region)
    {
        var options = CreateOptions();
        options.Region = region;
        Assert.Throws<ArgumentException>(() => SqsOptionsValidator.ValidateAndThrow(options));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-url")]
    [InlineData("arn:aws:sqs:eu-north-1:902707423845:cat-raw-dev")]
    [InlineData("http://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev")]
    [InlineData("https://sqs.eu-west-1.amazonaws.com/902707423845/cat-raw-dev")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com.example.org/902707423845/cat-raw-dev")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/90270742384/cat-raw-dev")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/902707423845/")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev.fifo")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev/extra")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev?option=1")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev#fragment")]
    [InlineData("https://user@sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com:8080/902707423845/cat-raw-dev")]
    [InlineData("https://sqs.eu-north-1.amazonaws.com/902707423845/other/../cat-raw-dev")]
    public void InvalidQueueUrlIsRejected(string queueUrl)
    {
        var options = CreateOptions();
        options.RawFanOut.QueueUrl = queueUrl;
        Assert.Throws<ArgumentException>(() => SqsOptionsValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void MissingRawFanOutSectionIsRejected()
    {
        var options = new SqsOptions { Region = "eu-north-1" };
        Assert.Throws<ArgumentException>(() => SqsOptionsValidator.ValidateAndThrow(options));
    }

    [Fact]
    public async Task WorkerBindsRegionAndPublishesToRawFanOutWithoutUsingLegacyUrl()
    {
        var settings = CreateSettings();
        settings["Source:SqsQueueUrl"] = "https://legacy.example/ignored";
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddProductionMessaging(builder.Configuration);
        using var client = new StubSqsClient();
        builder.Services.AddSingleton<IAmazonSQS>(client);
        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var config = host.Services.GetRequiredService<AmazonSQSConfig>();
            Assert.Equal("eu-north-1", config.RegionEndpoint.SystemName);
            Assert.Null(config.ServiceURL);
            var publisher = host.Services.GetRequiredService<IRawArtifactPublisher>();
            Assert.Same(publisher, host.Services.GetRequiredService<IRawArtifactPublisher>());
            var message = CreateMessage();
            await publisher.PublishAsync(message, CancellationToken.None);
            Assert.Equal(QueueUrl, Assert.Single(client.Requests).QueueUrl);
            Assert.Equal(JsonSerializer.Serialize(message), client.Requests[0].MessageBody);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Theory]
    [InlineData("Sqs:Region", "")]
    [InlineData("Sqs:Region", "eu-west-1")]
    [InlineData("Sqs:RawFanOut:QueueUrl", "")]
    [InlineData("Sqs:RawFanOut:QueueUrl", "http://localhost:4566/902707423845/cat-raw-dev")]
    public async Task InvalidSettingsFailAtStartupDespiteLegacyQueueSetting(string key, string value)
    {
        var settings = CreateSettings();
        settings[key] = value;
        settings["Source:SqsQueueUrl"] = QueueUrl;
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddProductionMessaging(builder.Configuration);
        using var host = builder.Build();
        var failure = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.Equal(typeof(SqsOptions), failure.OptionsType);
    }

    [Fact]
    public void EnvironmentVariablesOverrideQueueAndRegion()
    {
        var prefix = $"SQS_CONFIG_TEST_{Guid.NewGuid():N}_";
        const string overriddenUrl = "https://sqs.eu-west-1.amazonaws.com/902707423845/cat-raw-test";
        Environment.SetEnvironmentVariable(prefix + "Sqs__Region", "eu-west-1");
        Environment.SetEnvironmentVariable(prefix + "Sqs__RawFanOut__QueueUrl", overriddenUrl);
        try
        {
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(CreateSettings())
                .AddEnvironmentVariables(prefix).Build();
            using var provider = new ServiceCollection().AddProductionMessaging(configuration).BuildServiceProvider();
            var options = provider.GetRequiredService<IOptions<SqsOptions>>().Value;
            Assert.Equal(overriddenUrl, options.RawFanOut.QueueUrl);
            Assert.Equal("eu-west-1", provider.GetRequiredService<AmazonSQSConfig>().RegionEndpoint.SystemName);
        }
        finally
        {
            Environment.SetEnvironmentVariable(prefix + "Sqs__Region", null);
            Environment.SetEnvironmentVariable(prefix + "Sqs__RawFanOut__QueueUrl", null);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task MissingMessageIdFailsAndReleasesPublishingSlot(string? messageId)
    {
        using var client = new StubSqsClient
        {
            Send = (_, _) => Task.FromResult(new SendMessageResponse { MessageId = messageId })
        };
        var publisher = new SqsRawArtifactPublisher(client, QueueUrl, 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(CreateMessage(), CancellationToken.None));
        client.Send = (_, _) => Task.FromResult(new SendMessageResponse { MessageId = "accepted" });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await publisher.PublishAsync(CreateMessage(), timeout.Token);
        Assert.Equal(2, client.Requests.Count);
    }

    [Fact]
    public async Task SendFailurePropagatesWithoutAnExtraRetryLayer()
    {
        var failure = new AmazonSQSException("Test access denied.");
        using var client = new StubSqsClient { Send = (_, _) => Task.FromException<SendMessageResponse>(failure) };
        var publisher = new SqsRawArtifactPublisher(client, QueueUrl);
        Assert.Same(failure, await Assert.ThrowsAsync<AmazonSQSException>(() => publisher.PublishAsync(CreateMessage(), CancellationToken.None)));
        Assert.Single(client.Requests);
    }

    [Fact]
    public async Task CancellationReachesSendAndReleasesPublishingSlot()
    {
        using var cancellation = new CancellationTokenSource();
        using var client = new StubSqsClient
        {
            Send = (_, cancellationToken) =>
            {
                Assert.Equal(cancellation.Token, cancellationToken);
                cancellation.Cancel();
                return Task.FromCanceled<SendMessageResponse>(cancellationToken);
            }
        };
        var publisher = new SqsRawArtifactPublisher(client, QueueUrl, 1);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publisher.PublishAsync(CreateMessage(), cancellation.Token));
        client.Send = (_, _) => Task.FromResult(new SendMessageResponse { MessageId = "accepted" });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await publisher.PublishAsync(CreateMessage(), timeout.Token);
        Assert.Equal(2, client.Requests.Count);
    }

    [Fact]
    public async Task AlreadyCancelledPublicationDoesNotCallSqs()
    {
        using var client = new StubSqsClient();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var publisher = new SqsRawArtifactPublisher(client, QueueUrl);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publisher.PublishAsync(CreateMessage(), cancellation.Token));
        Assert.Empty(client.Requests);
    }

    private sealed class StubSqsClient() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.EUNorth1)
    {
        public List<SendMessageRequest> Requests { get; } = [];
        public Func<SendMessageRequest, CancellationToken, Task<SendMessageResponse>> Send { get; set; }
            = (_, _) => Task.FromResult(new SendMessageResponse { MessageId = "accepted" });

        public override Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            return Send(request, cancellationToken);
        }
    }

    private static RawArtifactAvailableV1 CreateMessage() => new()
    {
        SourceType = "Production", WindfarmId = 42, DataDate = "2026-09-01",
        ExtractionToken = 42, RunId = "test-run",
        BlobPath = "/raw/production/windfarm=42/date=2026-09-01/extraction=42/production.parquet"
    };

    private static Dictionary<string, string?> CreateSettings() => new()
    {
        ["Sqs:Region"] = "eu-north-1", ["Sqs:RawFanOut:QueueUrl"] = QueueUrl
    };

    private static SqsOptions CreateOptions() => new()
    {
        Region = "eu-north-1",
        RawFanOut = new() { QueueUrl = QueueUrl }
    };
}