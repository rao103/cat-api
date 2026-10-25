using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Operations.FanOut.Strategies;

/// <summary>Shared event planning algorithm, configured per source through the strategy registry.</summary>
public sealed class CanonicalEventFanOutStrategy(string sourceType) : IRawFanOutStrategy
{
    public string SourceType { get; } = sourceType;

    public IEnumerable<WorkSelection> BuildWork(FanOutPlanningContext context)
    {
        if (SourceType == "Outage")
        {
            var outageEventIds = context.Rows.Select(row => row.EventId
                ?? throw new InvalidDataException("Canonical event identity is required."))
                .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
            if (outageEventIds.Length > 0)
                yield return new WorkSelection(HarmonizationWorkKinds.CanonicalEvents, null, [], outageEventIds);

            if (context.Artifact.PayloadKind == "OutageSnapshot")
                yield return new WorkSelection(HarmonizationWorkKinds.EventSnapshotReconciliation, null, [], []);
            else if (outageEventIds.Length == 0)
                yield return new WorkSelection("ArtifactObserved", null, [], []);
            yield break;
        }

        var identities = context.Rows.Select(row => row.EventId
            ?? throw new InvalidDataException("Canonical event identity is required."))
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        if (identities.Length > 0)
            yield return new WorkSelection(HarmonizationWorkKinds.CanonicalEvents, null, [], identities);

        if (context.Artifact.IsFullSnapshot)
            yield return new WorkSelection(HarmonizationWorkKinds.EventSnapshotReconciliation, null, [], []);
        else if (identities.Length == 0)
            yield return new WorkSelection("ArtifactObserved", null, [], []);
    }
}
