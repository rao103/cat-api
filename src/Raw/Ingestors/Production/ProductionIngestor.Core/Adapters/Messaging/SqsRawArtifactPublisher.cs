using Amazon.SQS;
using Amazon.SQS.Model;
using CAT.Messaging.Contracts;
using CAT.Messaging.Sqs;
using ProductionIngestor.Interfaces;
using System.Diagnostics;
using System.Text.Json;
using ProductionIngestor.Contracts;

namespace ProductionIngestor.Adapters.Messaging;

/// <summary>Source adapter over the shared validated Raw transport.</summary>
public sealed class SqsRawArtifactPublisher : IRawArtifactPublisher, IDisposable
{
    private readonly SqsRawArtifactSender _sender;
    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;
    private readonly SemaphoreSlim _legacyInFlight;

    public SqsRawArtifactPublisher(IAmazonSQS client, string queueUrl, int maxInFlightSends = 10)
    {
        _sender = new SqsRawArtifactSender(client, queueUrl, maxInFlightSends);
        _client = client;
        _queueUrl = queueUrl;
        _legacyInFlight = new SemaphoreSlim(maxInFlightSends, maxInFlightSends);
    }

    public Task PublishAsync(RawArtifactAvailableV2 message, CancellationToken cancellationToken)
        => _sender.PublishAsync(message, cancellationToken);

    public Task<bool> PublishBatchAsync(IReadOnlyList<RawArtifactAvailableV2> messages, CancellationToken cancellationToken)
        => _sender.PublishBatchAsync(messages, cancellationToken);

    public async Task PublishAsync(RawArtifactAvailableV1 message, CancellationToken cancellationToken)
    {
        await _legacyInFlight.WaitAsync(cancellationToken);
        try
        {
            var request = new SendMessageRequest
            {
                QueueUrl = _queueUrl,
                MessageBody = JsonSerializer.Serialize(message),
            };

            if (Activity.Current is { IdFormat: ActivityIdFormat.W3C, Id: not null } activity)
            {
                request.MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    ["traceparent"] = new() { DataType = "String", StringValue = activity.Id }
                };
                if (!string.IsNullOrEmpty(activity.TraceStateString))
                    request.MessageAttributes["tracestate"] = new() { DataType = "String", StringValue = activity.TraceStateString };
            }

            var response = await _client.SendMessageAsync(request, cancellationToken);
            if (string.IsNullOrWhiteSpace(response.MessageId))
            {
                throw new InvalidOperationException("SQS send was not accepted (no MessageId returned).");
            }
        }
        finally
        {
            _legacyInFlight.Release();
        }
    }

    public void Dispose()
    {
        _sender.Dispose();
        _legacyInFlight.Dispose();
    }
}
