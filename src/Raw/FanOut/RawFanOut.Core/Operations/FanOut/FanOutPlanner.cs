using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Adapters.Storage;

namespace CAT.RawFanOut.Operations.FanOut;

public sealed class FanOutPlanner : IFanOutPlanner
{
    private readonly IRawIdentityReader reader;
    private readonly IFarmCatalog catalog;
    private readonly FanOutOptions options;
    private readonly IReadOnlyDictionary<string, IRawFanOutStrategy> strategies;
    private readonly ITelemetry<FanOutPlanner> telemetry;
    private readonly IRawObjectStore? objects;

    public FanOutPlanner(IRawIdentityReader reader, IFarmCatalog catalog, FanOutOptions options,
        IEnumerable<IRawFanOutStrategy> strategies, ITelemetry<FanOutPlanner> telemetry, IRawObjectStore? objects = null)
    {
        this.reader = reader;
        this.catalog = catalog;
        this.options = options;
        this.strategies = strategies.ToDictionary(strategy => strategy.SourceType, StringComparer.Ordinal);
        this.telemetry = telemetry;
        this.objects = objects;
    }

    public async Task<IReadOnlyList<OutgoingMessage>> PlanAsync(RawEnvelope artifact, CancellationToken cancellationToken)
    {
        if (!strategies.TryGetValue(artifact.SourceType, out var strategy))
            throw new InvalidDataException("No strategy is registered for this source.");

        if (artifact.ManifestPath is not null)
        {
            if (objects is null) throw new InvalidDataException("Manifest input requires a Raw object store.");
            artifact = await new RawSnapshotManifestResolver(objects, options).ResolveAsync(artifact, cancellationToken);
        }

        var farm = await catalog.LoadAsync(new FarmCatalogRequest(artifact.WindfarmId, artifact.SourceType), cancellationToken);
        if (farm.WindfarmId != artifact.WindfarmId || string.IsNullOrWhiteSpace(farm.WindfarmName))
            throw new InvalidDataException("Catalog returned an invalid farm identity.");
        if (artifact.WindfarmName is { } outageFarmName &&
            !string.Equals(outageFarmName, farm.WindfarmName, StringComparison.Ordinal))
            throw new InvalidDataException("Raw farm name does not match the catalog.");
        if (artifact.Snapshot is { } snapshot && snapshot.ScopeItemIds.Except(farm.Turbines.Select(turbine => turbine.WtgId)).Any())
            throw new InvalidDataException("Snapshot scope contains turbines outside this windfarm.");
        if (artifact.EventSnapshotScope is { } eventScope && eventScope.WtgIds.Except(farm.Turbines.Select(t => t.WtgId)).Any())
            throw new InvalidDataException("Outage snapshot contains turbines outside this windfarm.");
        if (artifact.EventSnapshotScope is { WtgIds.Length: 0 } && artifact.EventScopeWtgIdsSpecified)
            throw new InvalidDataException("An explicit Outage WTG scope cannot be empty.");
        if (artifact.CanonicalManifest is null && artifact.SourceType == "Outage" && artifact.PayloadKind == "OutageSnapshot" && artifact.EventSnapshotScope is { } outageSnapshotScope)
        {
            var scopeWtgIds = artifact.EventScopeWtgIdsSpecified
                ? outageSnapshotScope.WtgIds
                : farm.Turbines.Select(turbine => turbine.WtgId).ToArray();
            if (scopeWtgIds.Length == 0 || scopeWtgIds.Any(id => id <= 0) || scopeWtgIds.Distinct().Count() != scopeWtgIds.Length)
                throw new InvalidDataException("Outage snapshot scope cannot be frozen from an invalid or empty WTG roster.");
            artifact = artifact with
            {
                EventSnapshotScope = outageSnapshotScope with { WtgIds = scopeWtgIds.Order().ToArray() }
            };
        }
        if (artifact.IsExtractionReady)
        {
            if (!string.Equals(artifact.WindfarmName, farm.WindfarmName, StringComparison.Ordinal))
                throw new InvalidDataException("Production farm name does not match the catalog.");
            var names = farm.Turbines.ToLookup(turbine => turbine.WtgName, StringComparer.Ordinal);
            if ((artifact.ScopeWtgNames ?? []).Any(name => names[name].Count() != 1))
                throw new InvalidDataException("Refresh scope turbine name is missing or ambiguous in the catalog.");
        }
        if (artifact.CanonicalManifest is { } manifest && artifact.SourceType == "Outage")
        {
            var names = farm.Turbines.ToLookup(turbine => turbine.WtgName, StringComparer.Ordinal);
            if (manifest.ScopeWtgNames.Any(name => names[name].Count() != 1))
                throw new InvalidDataException("Snapshot turbine scope is missing or ambiguous in the catalog.");
            var scopedIds = manifest.ScopeWtgNames.Select(name => names[name].Single().WtgId).Order().ToArray();
            artifact = artifact with { EventSnapshotScope = artifact.EventSnapshotScope! with { WtgIds = scopedIds } };
        }
        var identities = await ReadIdentitiesAsync(artifact, farm, cancellationToken);
        var context = new FanOutPlanningContext(artifact, farm, identities);
        var builder = new WorkBatchBuilder(context, options);
        foreach (var selection in strategy.BuildWork(context))
        {
            cancellationToken.ThrowIfCancellationRequested();
            builder.Add(selection);
        }
        builder.Complete();
        telemetry.Information(
            $"Planned {builder.Messages.Count} batches from {identities.Count} identities for windfarm {farm.WindfarmId} ({farm.WindfarmName}), source {artifact.SourceType}",
            artifact.RunId, callerId: artifact.ArtifactId);
        return builder.Messages;
    }

    private async Task<IReadOnlyList<RawIdentity>> ReadIdentitiesAsync(
        RawEnvelope artifact, FarmCatalog farm, CancellationToken cancellationToken)
    {
        var identities = new HashSet<RawIdentity>();
        var turbines = farm.Turbines.ToDictionary(turbine => turbine.WtgId);
        var names = farm.Turbines.ToLookup(turbine => turbine.WtgName, StringComparer.Ordinal);
        var itemDays = new HashSet<(int?, DateOnly?)>();
        var partCounts = new Dictionary<string, long>(StringComparer.Ordinal);
        long rowCount = 0;
        await foreach (var sourceRow in reader.ReadAsync(artifact, cancellationToken).WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (++rowCount > options.MaxRowsPerArtifact) throw new InvalidDataException("Artifact row limit exceeded.");
            if (sourceRow.WindfarmId != artifact.WindfarmId) throw new InvalidDataException("Raw row farm does not match the envelope.");
            if (sourceRow.RawFarmName is { } rawFarmName && !string.Equals(rawFarmName, farm.WindfarmName, StringComparison.Ordinal))
                throw new InvalidDataException("Raw Outage farm name does not match the catalog.");
            if (artifact.CanonicalManifest is not null)
            {
                if (sourceRow.RawPath is null || !artifact.Paths.Contains(sourceRow.RawPath, StringComparer.Ordinal))
                    throw new InvalidDataException("Snapshot row path is outside the manifest.");
                partCounts[sourceRow.RawPath] = partCounts.GetValueOrDefault(sourceRow.RawPath) + 1;
            }
            var row = sourceRow;
            if (row.WtgId is null && row.WtgName is not null)
            {
                var matches = names[row.WtgName].ToArray();
                if (matches.Length != 1) throw new InvalidDataException("Legacy turbine name is missing or ambiguous in the catalog.");
                row = row with { WtgId = matches[0].WtgId, WtgName = matches[0].WtgName };
            }
            if (row.WtgId is { } id && !turbines.ContainsKey(id))
                throw new InvalidDataException("Raw turbine is not registered to this windfarm.");
            if (row.WtgId is { } namedId && row.WtgName is { } rawName &&
                !string.Equals(rawName, turbines[namedId].WtgName, StringComparison.Ordinal))
                throw new InvalidDataException("Raw WTG ID/name pair does not match the catalog.");
            if (artifact.CanonicalManifest is not null && artifact.SourceType == "Outage" &&
                (row.WtgId is null || !artifact.EventSnapshotScope!.WtgIds.Contains(row.WtgId.Value)))
                throw new InvalidDataException("Outage row is outside the manifest turbine scope.");
            if (artifact.SourceType == "Outage" && row.WtgId is { } outageWtgId)
            {
                var catalogTurbine = turbines[outageWtgId];
                if (row.WtgName is { } rawWtgName && !string.Equals(rawWtgName, catalogTurbine.WtgName, StringComparison.Ordinal))
                    throw new InvalidDataException("Raw Outage turbine name does not match its catalog identity.");
                row = row with { WtgName = catalogTurbine.WtgName };
            }
            if (artifact.EventSnapshotScope is { WtgIds.Length: > 0 } outageScope &&
                row.WtgId is { } scopedWtgId && !outageScope.WtgIds.Contains(scopedWtgId))
                throw new InvalidDataException("Outage row turbine is outside the declared snapshot scope.");
            if (artifact.SourceType == "Outage" && row.WtgId is null)
                throw new InvalidDataException("Outage WTG identity is required.");
            if (artifact.SourceType == "Outage")
            {
                if (string.IsNullOrWhiteSpace(row.RawPath)) throw new InvalidDataException("Outage row Raw path is required.");
                var path = RawEnvelope.ValidatePath(row.RawPath);
                if (!artifact.Paths.Contains(path, StringComparer.Ordinal))
                    throw new InvalidDataException("Outage row path is outside the declared artifact set.");
                row = row with { RawPath = path };
            }
            if (artifact.SourceType == "Production")
            {
                ValidateProduction(artifact, row);
                row = row with { WtgName = turbines[row.WtgId!.Value].WtgName };
                if (artifact.IsExtractionReady && !(artifact.ScopeWtgNames ?? []).Contains(row.WtgName, StringComparer.Ordinal))
                    throw new InvalidDataException("Production row turbine is outside the refresh scope.");
                if (artifact.IsExtractionReady || artifact.IsIncrementalChanges)
                {
                    if (string.IsNullOrWhiteSpace(row.RawPath)) throw new InvalidDataException("Production row source path is required.");
                    var path = RawEnvelope.ValidatePath(row.RawPath);
                    if (artifact.ArtifactPrefix is { } prefix ? !path.StartsWith(prefix, StringComparison.Ordinal) : !artifact.Paths.Contains(path, StringComparer.Ordinal))
                        throw new InvalidDataException("Production row path is outside the declared artifact set.");
                }
                itemDays.Add((row.WtgId, row.ProductionDate));
                if (itemDays.Count > options.MaxIdentities) throw new InvalidDataException("Artifact identity limit exceeded.");
            }
            else if (string.IsNullOrWhiteSpace(row.EventId) || row.EventId.Length > 128)
                throw new InvalidDataException("Canonical event identity is invalid.");
            // Canonical work is deduplicated by event identity, including tombstones and
            // adjustment parents with no current turbine members.
            var identity = artifact.SourceType switch
            {
                "Production" => row,
                "Outage" => row,
                _ => row with { WtgId = null, WtgName = null, RawPath = null }
            };
            identities.Add(identity);
            if (artifact.SourceType != "Production" && identities.Count > options.MaxIdentities) throw new InvalidDataException("Artifact identity limit exceeded.");
        }
        if (artifact.SourceType == "Outage" && identities.GroupBy(row => row.EventId, StringComparer.Ordinal)
            .Any(group => group.Select(row => row.WtgId).Distinct().Count() != 1))
            throw new InvalidDataException("One Outage event identity cannot belong to multiple WTGs.");
        if (artifact.ExpectedTotalRowCount is { } expected && rowCount != expected)
            throw new InvalidDataException("Raw rows read differ from the declared row count.");
        if (artifact.CanonicalManifest is { } manifest)
            foreach (var part in manifest.Parts)
                if (partCounts.GetValueOrDefault(part.BlobPath) != part.RowCount)
                    throw new InvalidDataException("Snapshot part row count differs from the manifest.");
        return identities.ToArray();
    }

    private static void ValidateProduction(RawEnvelope artifact, RawIdentity row)
    {
        if (row.WtgId is null || row.ProductionDate is null) throw new InvalidDataException("Production turbine and date are required.");
        if (artifact.DataDate is { } declared && declared != row.ProductionDate)
            throw new InvalidDataException("Production row differs from the declared daily artifact date.");
        if (artifact.IsExtractionReady && !(artifact.RequestedScope ?? []).Any(range =>
            row.ProductionDate >= DateOnly.ParseExact(range.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture) &&
            row.ProductionDate <= DateOnly.ParseExact(range.EndDateInclusive, "yyyy-MM-dd", CultureInfo.InvariantCulture)))
            throw new InvalidDataException("Production row date is outside the requested refresh scope.");
        if (artifact.Snapshot is not { } scope) return;
        var start = DateOnly.ParseExact(scope.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        var end = DateOnly.ParseExact(scope.EndDateExclusive, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (row.ProductionDate < start || row.ProductionDate >= end || !scope.ScopeItemIds.Contains(row.WtgId.Value))
            throw new InvalidDataException("Production row is outside the authoritative snapshot scope.");
    }
}
