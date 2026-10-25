using CAT.Messaging.Contracts;

namespace OutageIngestor.Interfaces;

/// <summary>Publishes a `raw-fanout` message once a Raw artifact (or shard-set) is durable.</summary>
public interface IRawArtifactPublisher
{
    Task<bool> PublishBatchAsync(IReadOnlyList<RawArtifactAvailableV2> messages, CancellationToken cancellationToken);

    Task PublishAsync(RawArtifactAvailableV2 message, CancellationToken cancellationToken);
}
