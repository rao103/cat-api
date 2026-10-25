using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Models;
using ProductionIngestor.Contracts;
namespace Harmonizer.Core.SourceTypes.Production.Serialization;

public static class RefreshManifestValidator
{
    public static ProductionExtractionReadyV1 Validate(ProductionWork work)
    {
        var manifest = work.ResolvedRefresh ?? JsonSerializer.Deserialize<ProductionExtractionReadyV1>(work.OriginalMessageBody, HarmonizerJson.Options)
            ?? throw new WorkValidationException("Missing complete refresh envelope.");
        if (manifest.EnvelopeVersion != 2 || manifest.MessageKind != "ExtractionReady" || manifest.SourceType != "Production" ||
            manifest.PayloadKind != "ProductionSnapshot" || manifest.PayloadSchemaVersion != 2 || manifest.ExtractionMode != "Refresh" ||
            manifest.WindfarmId != work.Windfarm.WindfarmId || manifest.WindfarmName != work.Windfarm.WindfarmName ||
            manifest.RequestId != work.RequestId || manifest.AttemptId != work.AttemptId || manifest.ExtractionToken != work.ExtractionToken)
        {
            throw new WorkValidationException("Refresh envelope does not match work identity.");
        }

        if (manifest.CompletedWindows.Count == 0 || manifest.CompletedWindows.Any(window => !window.Completed || window.RowCount < 0 ||
            window.QueryEndExclusiveUtc <= window.QueryStartInclusiveUtc) ||
            manifest.CompletedWindows.Select(window => window.WindowNumber).Distinct().Count() != manifest.CompletedWindows.Count)
        {
            throw new WorkValidationException("Refresh query windows are incomplete or invalid.");
        }

        if (!manifest.RequestedScope.Any(range => Contains(range, work.DataDate)) ||
            !manifest.CompletedWindows.Any(window => window.SelectedScope.Any(range => Contains(range, work.DataDate))))
        {
            throw new WorkValidationException("Work date is outside the completed declared scope.");
        }

        if (manifest.Artifacts.Select(artifact => artifact.BlobPath).Distinct(StringComparer.Ordinal).Count() != manifest.Artifacts.Count ||
            manifest.Artifacts.Any(artifact => !ObjectPath.IsSafe(artifact.BlobPath)) ||
            !work.RawPaths.All(path => manifest.Artifacts.Any(artifact => artifact.BlobPath == path)))
        {
            throw new UnsupportedWorkException("Refresh work paths must belong to its complete explicit extraction manifest.");
        }

        if (manifest.TotalRowCount < 0 || manifest.Artifacts.Any(artifact => artifact.RowCount < 0 || artifact.PartNumber < 1) ||
            manifest.Artifacts.Sum(artifact => artifact.RowCount) != manifest.TotalRowCount ||
            manifest.CompletedWindows.Sum(window => window.RowCount) != manifest.TotalRowCount ||
            manifest.IsEmptyResult != (manifest.TotalRowCount == 0))
        {
            throw new WorkValidationException("Refresh row-count declarations disagree.");
        }

        foreach (var window in manifest.CompletedWindows)
        {
            var parts = manifest.Artifacts.Where(artifact => artifact.QueryWindowNumber == window.WindowNumber).ToArray();
            if (parts.Length != window.ArtifactCount || parts.Sum(part => part.RowCount) != window.RowCount ||
                parts.Select(part => part.PartNumber).Distinct().Count() != parts.Length)
            {
                throw new WorkValidationException("Refresh query window part accounting is inconsistent.");
            }
        }
        if (manifest.Artifacts.Any(artifact => !manifest.CompletedWindows.Any(window => window.WindowNumber == artifact.QueryWindowNumber)))
        {
            throw new WorkValidationException("Refresh contains an artifact from an undeclared query window.");
        }

        if (manifest.ExpectedArtifactCount is { } expected && expected != manifest.Artifacts.Count)
        {
            throw new WorkValidationException("Refresh expected artifact count differs.");
        }

        if (manifest.SourceVersionEvidence != work.SourceVersionEvidence)
        {
            throw new WorkValidationException("Refresh source-version evidence differs from its envelope.");
        }

        if (work.Turbines.Any(turbine => !manifest.ScopeWtgIds.Contains(turbine.WtgName, StringComparer.Ordinal)))
        {
            throw new WorkValidationException("Work membership is outside the extraction's declared turbine names.");
        }

        if (work.Turbines.Count == 0 && (!work.HasNoSourceRows || manifest.ScopeWtgIds.Count != 0 ||
            !manifest.IsEmptyResult || manifest.Artifacts.Count != 0))
            throw new WorkValidationException("Empty membership requires a completed empty refresh with an empty turbine scope.");

        var relevant = RequiredArtifacts(manifest, work.DataDate);
        if (!work.RawPaths.All(path => relevant.Any(artifact => artifact.BlobPath == path)))
        {
            throw new WorkValidationException("Work references an artifact outside its business-date windows.");
        }

        if (manifest.Artifacts.Count == 0 && !manifest.IsEmptyResult)
        {
            throw new UnsupportedWorkException("Prefix-only extraction requires an immutable enumerated manifest.");
        }

        return manifest;
    }

    // Raw Fan-Out may emit only the part where it observed this member. Complete the day from
    // every explicitly declared part in the overlapping completed windows before committing.
    public static IReadOnlyList<ProductionArtifactReferenceV1> RequiredArtifacts(ProductionExtractionReadyV1 manifest, DateOnly day)
    {
        var windows = manifest.CompletedWindows.Where(window => window.SelectedScope.Any(range => Contains(range, day)))
            .Select(window => window.WindowNumber).ToHashSet();
        return manifest.Artifacts.Where(artifact => windows.Contains(artifact.QueryWindowNumber))
            .OrderBy(artifact => artifact.QueryWindowNumber).ThenBy(artifact => artifact.PartNumber).ToArray();
    }

    private static bool Contains(ProductionDateRangeV1 range, DateOnly day)
    {
        var start = ProductionWorkMessageReader.ParseDate(range.StartDate);
        var end = ProductionWorkMessageReader.ParseDate(range.EndDateInclusive);
        if (end < start)
        {
            throw new WorkValidationException("Invalid refresh date range.");
        }

        return start <= day && day <= end;
    }
}
