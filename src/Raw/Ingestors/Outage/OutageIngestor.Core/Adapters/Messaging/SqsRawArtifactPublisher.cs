using Amazon.SQS;
using Amazon.SQS.Model;
using CAT.Messaging.Contracts;
using CAT.Messaging.Sqs;
using OutageIngestor.Interfaces;

namespace OutageIngestor.Adapters.Messaging;

/// <summary>Source adapter over the shared validated Raw transport.</summary>
public sealed class SqsRawArtifactPublisher : IRawArtifactPublisher, IDisposable
{
    private readonly SqsRawArtifactSender _sender;

    public SqsRawArtifactPublisher(IAmazonSQS client, string queueUrl, int maxInFlightSends = 10)
    {
        _sender = new SqsRawArtifactSender(client, queueUrl, maxInFlightSends);
    }

    public Task PublishAsync(RawArtifactAvailableV2 message, CancellationToken cancellationToken)
        => _sender.PublishAsync(message, cancellationToken);

    public Task<bool> PublishBatchAsync(IReadOnlyList<RawArtifactAvailableV2> messages, CancellationToken cancellationToken)
        => _sender.PublishBatchAsync(messages, cancellationToken);

    public void Dispose()
    {
        _sender.Dispose();
    }
}
