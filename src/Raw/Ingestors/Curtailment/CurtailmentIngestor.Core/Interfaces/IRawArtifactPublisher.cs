using CurtailmentIngestor.Contracts;

namespace CurtailmentIngestor.Interfaces;

/// <summary>Publishes a `raw-fanout` message once a Raw artifact (or shard-set) is durable.</summary>
public interface IRawArtifactPublisher
{
    Task PublishAsync(RawCurtailmentBatchAvailableV1 message, CancellationToken cancellationToken);
}
