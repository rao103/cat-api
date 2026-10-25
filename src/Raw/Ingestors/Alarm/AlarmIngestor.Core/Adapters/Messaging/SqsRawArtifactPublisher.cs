using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using AlarmIngestor.Contracts;
using AlarmIngestor.Interfaces;

namespace AlarmIngestor.Adapters.Messaging;

/// <summary>SQS-backed <see cref="IRawArtifactPublisher"/> with bounded in-flight sends.</summary>
public sealed class SqsRawArtifactPublisher : IRawArtifactPublisher
{
    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;
    private readonly SemaphoreSlim _inFlight;

    public SqsRawArtifactPublisher(IAmazonSQS client, string queueUrl, int maxInFlightSends = 10)
    {
        _client = client;
        _queueUrl = queueUrl;
        _inFlight = new SemaphoreSlim(maxInFlightSends, maxInFlightSends);
    }

    public async Task PublishAsync(RawAlarmBatchAvailableV1 message, CancellationToken cancellationToken)
    {
        await _inFlight.WaitAsync(cancellationToken);
        try
        {
            var request = new SendMessageRequest
            {
                QueueUrl = _queueUrl,
                MessageBody = JsonSerializer.Serialize(message),
            };

            var response = await _client.SendMessageAsync(request, cancellationToken);
            if (string.IsNullOrEmpty(response.MessageId))
            {
                throw new InvalidOperationException("SQS send was not accepted (no MessageId returned).");
            }
        }
        finally
        {
            _inFlight.Release();
        }
    }
}
