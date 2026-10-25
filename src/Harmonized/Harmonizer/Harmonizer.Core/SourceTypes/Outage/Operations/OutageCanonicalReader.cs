using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

public sealed record LoadedOutageCatalog(OutageCatalog Catalog,
    IReadOnlyDictionary<string, EventMonthSnapshot> Partitions,
    IReadOnlyDictionary<string, CanonicalOutage> Events,
    IReadOnlyDictionary<string, EventMonthReference> References);

/// <summary>Loads only explicitly committed pointers; storage listing never establishes authority.</summary>
public sealed class OutageCanonicalReader(IOutageArtifacts artifacts, IOptions<HarmonizerOptions> options,
    ITelemetry<OutageCanonicalReader> telemetry)
{
    public async Task<LoadedOutageCatalog> ReadAsync(OutageCatalogLease lease, CancellationToken cancellationToken)
    {
        var catalog = await artifacts.ReadCatalogAsync(lease.CatalogPath, cancellationToken);
        if (catalog.SchemaVersion != 1 || catalog.Months.Count > options.Value.EventLifecycle.MaximumPartitions ||
            catalog.Months.Select(month => month.PartitionKey).Distinct(StringComparer.Ordinal).Count() != catalog.Months.Count)
            throw new WorkValidationException("Committed Outage catalog has an unsupported schema or invalid partition set.");
        var partitions = new Dictionary<string, EventMonthSnapshot>(StringComparer.Ordinal);
        var events = new Dictionary<string, CanonicalOutage>(StringComparer.Ordinal);
        var references = new Dictionary<string, EventMonthReference>(StringComparer.Ordinal);
        foreach (var month in catalog.Months)
        {
            var snapshot = await artifacts.ReadMonthAsync(month, cancellationToken);
            if (snapshot.SchemaVersion != 1 || month.Revision <= 0 || month.Attempt <= 0)
                throw new WorkValidationException("Invalid committed Outage month reference/schema.");
            partitions.Add(month.PartitionKey, snapshot);
            foreach (var item in snapshot.Events)
            {
                OutageRecordValidator.Validate(item.State.LatestChange);
                var id = item.State.Key.OutageClassificationId;
                if (item.EventRevision <= 0 || item.State.Key.WindfarmId != lease.Windfarm.WindfarmId ||
                    item.State.CurrentEvent is { } active && active.StartMonth.ToString("yyyy-MM") != month.PartitionKey ||
                    !events.TryAdd(id, item))
                    throw new WorkValidationException("Canonical Outage identities, revision, or start-month partition are inconsistent.");
                references.Add(id, month);
                if (events.Count > options.Value.MaximumRowsPerArtifact)
                    throw new WorkValidationException("Committed farm Outage state exceeds the configured in-memory bound.");
            }
        }
        telemetry.Debug("Committed Outage catalog loaded. WindfarmId={WindfarmId} PartitionCount={PartitionCount} EventCount={EventCount}",
            lease.Windfarm.WindfarmId, partitions.Count, events.Count);
        return new(catalog, partitions, events, references);
    }
}
