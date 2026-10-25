using ProductionIngestor.Models;

namespace ProductionIngestor.Api.Interfaces;

/// <summary>Hides the Job-orchestration SDK behind an interface consumed by controllers.</summary>
public interface IIngestionJobTrigger
{
    /// <summary>Launches a one-shot Job to process the given refresh request; returns the created Job name.</summary>
    Task<string> TriggerAsync(RefreshRequestModel request, CancellationToken cancellationToken);
}
