using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using ProductionIngestor.Contracts;
namespace CAT.RawFanOut.Adapters.Parquet;

internal sealed class RawArtifactPathResolver(IRawObjectStore objects, FanOutOptions options)
{
    public async Task<string[]> ResolveAsync(RawEnvelope artifact, CancellationToken cancellationToken)
    {
        var paths = artifact.Paths;
        if (artifact.ArtifactPrefix is { } prefix)
        {
            if (artifact.ArtifactSelectionRule != ProductionArtifactSelectionRules.DirectParquetFilesV1 ||
                !artifact.ExpectedArtifactCount.HasValue || artifact.ExpectedArtifactCount.Value < 1 ||
                artifact.ExpectedArtifactCount.Value > options.MaxFilesPerExtraction)
                throw new InvalidDataException("Invalid sealed extraction artifact selection.");

            var expectedCount = artifact.ExpectedArtifactCount.Value;
            var discovered = new List<string>(expectedCount);
            await foreach (var candidate in objects.ListPathsAsync(prefix, cancellationToken).WithCancellation(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var path = RawEnvelope.ValidatePath(candidate);
                if (!path.StartsWith(prefix, StringComparison.Ordinal))
                    throw new InvalidDataException("Listed Raw artifact does not match the sealed extraction prefix.");
                var name = path[prefix.Length..];
                if (name.Length == 0 || name.Contains('/') || !name.EndsWith(".parquet", StringComparison.Ordinal) ||
                    name.EndsWith("-quarantine.parquet", StringComparison.Ordinal))
                    continue;
                if (discovered.Count == expectedCount)
                    throw new InvalidDataException("Sealed extraction contains more consumable files than declared.");
                discovered.Add(path);
            }
            if (discovered.Count != expectedCount)
                throw new InvalidDataException("Sealed extraction consumable file count does not match its declaration.");
            if (discovered.Distinct(StringComparer.Ordinal).Count() != discovered.Count)
                throw new InvalidDataException("Duplicate listing path.");
            paths = discovered.Order(StringComparer.Ordinal).ToArray();
        }

        return paths;
    }
}
