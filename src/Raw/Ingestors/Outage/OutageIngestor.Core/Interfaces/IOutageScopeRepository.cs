using OutageIngestor.Models;

namespace OutageIngestor.Interfaces;

/// <summary>
/// Reads the immutable PostgreSQL planning scope (windfarm identity + active turbine set) for one
/// windfarm execution. A focused read alongside <see cref="IExtractionCursorRepository"/>/
/// <see cref="IExtractionSchedulingRepository"/> — not a second operational coordinator.
/// </summary>
public interface IOutageScopeRepository
{
    /// <summary>
    /// Returns null when the windfarm does not exist or is not active
    /// (<c>windfarm.is_active = false</c>); the caller must not proceed with extraction in that case.
    /// <paramref name="asOfUtc"/> is the reference instant used to evaluate
    /// <c>turbine_toc &lt; asOfUtc</c> (normally the current UTC time).
    /// </summary>
    Task<WindfarmScopeModel?> GetActiveScopeAsync(int windfarmId, DateTime asOfUtc, CancellationToken cancellationToken);
}
