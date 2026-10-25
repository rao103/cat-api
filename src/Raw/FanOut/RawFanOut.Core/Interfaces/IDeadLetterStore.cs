using System;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Interfaces;

public interface IDeadLetterStore
{
    // Idempotent per (queue, message id); identical re-delivery only refreshes last_observed_at_utc.
    // Different content for an existing identity throws DeadLetterIntegrityException.
    Task<DeadLetterArchiveResult> RecordAsync(DeadLetterArchiveRequest request, CancellationToken cancellationToken);
    Task<DeadLetterIncident> LoadAsync(Guid incidentId, CancellationToken cancellationToken);
    // Records that republication was requested. Never resolves the incident.
    Task MarkReplayRequestedAsync(Guid incidentId, CancellationToken cancellationToken);
    // Explicit operator closure with a reason; the only resolution path.
    Task ResolveAsync(ResolveIncidentRequest request, CancellationToken cancellationToken);
}