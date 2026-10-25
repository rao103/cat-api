using CAT.Messaging.Contracts;
using ProductionIngestor.Contracts;

namespace ProductionIngestor.Interfaces;

/// <summary>Publishes a `raw-fanout` message once a Raw artifact (or shard-set) is durable.</summary>
public interface IRawArtifactPublisher
{
    Task PublishAsync(RawArtifactAvailableV1 message, CancellationToken cancellationToken);

    Task PublishAsync(RawArtifactAvailableV2 message, CancellationToken cancellationToken);

    /// <summary>
    /// Publishes changed-row part notifications (schemaVersion 2) in bounded SQS batches. Every
    /// entry of every batch response is inspected; only failed/ambiguous entries are retried, with
    /// the identical envelope, up to a bounded number of attempts. Returns false if any part could
    /// not be confirmed delivered after retries are exhausted (the caller fails that window).
    /// </summary>
    Task<bool> PublishBatchAsync(IReadOnlyList<RawArtifactAvailableV2> parts, CancellationToken cancellationToken);
}
