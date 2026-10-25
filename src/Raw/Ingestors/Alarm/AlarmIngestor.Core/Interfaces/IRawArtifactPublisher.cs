using AlarmIngestor.Contracts;

namespace AlarmIngestor.Interfaces;

public interface IRawArtifactPublisher
{
    Task PublishAsync(RawAlarmBatchAvailableV1 message, CancellationToken cancellationToken);
}
