using CAT.Messaging.Contracts;
using ProductionIngestor.Contracts;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Raw;
using ProductionIngestor.Serialization;

namespace ProductionIngestor.Operations.Execution.Changes;

/// <summary>
/// Streams one bounded extraction window's changed rows into one or more immutable Parquet
/// parts and publishes their notifications. Owns no shared state across windows: every call gets
/// its own adapter enumeration (own connection/reader) and its own part counter starting at 1.
/// </summary>
public sealed class ProductionChangeWindowProcessor
{
    private readonly IProductionSourceAdapter _sourceAdapter;
    private readonly ProductionChangesPartWriter _partWriter;
    private readonly IRawArtifactPublisher _publisher;
    private readonly IIngestionTelemetry<ProductionChangeWindowProcessor> _telemetry;
    private readonly int _maxRowsPerPart;

    public ProductionChangeWindowProcessor(
        IProductionSourceAdapter sourceAdapter,
        ProductionChangesPartWriter partWriter,
        IRawArtifactPublisher publisher,
        IIngestionTelemetry<ProductionChangeWindowProcessor> telemetry,
        int maxRowsPerPart)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRowsPerPart);
        _sourceAdapter = sourceAdapter;
        _partWriter = partWriter;
        _publisher = publisher;
        _telemetry = telemetry;
        _maxRowsPerPart = maxRowsPerPart;
    }

    public async Task<WindowProcessingResult> ProcessWindowAsync(
        ExtractionWindowExecutionRequest window, CancellationToken cancellationToken)
    {
        try
        {
            _telemetry.Information(
                $"Starting ProductionChanges source pull for windfarm {window.WindfarmId}, " +
                $"LastUpdatedUtc window {window.WindowMinInclusiveUtc:O} to {window.WindowMaxInclusiveUtc:O}.", window.RunId);
            var buffer = new List<ProductionChangeRowModel>(_maxRowsPerPart);
            var parts = new List<(ProductionChangesPartResult Part, string BlobPath, int PartNumber)>();
            var publishedDates = new HashSet<DateOnly>();
            var totalRows = 0;
            var partNumber = 0;
            SourceRowKey? lastObserved = null;

            await foreach (var row in _sourceAdapter.GetProductionChangeRowsAsync(
                window.WindfarmName, window.ActiveTurbineNames, window.MinRetrieveDateUtc,
                window.WindowMinInclusiveUtc, window.WindowMaxInclusiveUtc, cancellationToken))
            {
                buffer.Add(row);
                publishedDates.Add(DateOnly.FromDateTime(row.ProductionTimestampUtc));
                totalRows++;

                var candidate = new SourceRowKey(row.LastUpdatedUtc, row.WtgName, row.ProductionTimestampUtc);
                if (lastObserved is null || candidate.LastUpdatedUtc > lastObserved.LastUpdatedUtc)
                {
                    lastObserved = candidate;
                }

                if (buffer.Count >= _maxRowsPerPart)
                {
                    partNumber++;
                    parts.Add(await WritePartAsync(window, partNumber, buffer, cancellationToken));
                    buffer = new List<ProductionChangeRowModel>(_maxRowsPerPart);
                }
            }

            if (buffer.Count > 0 || parts.Count == 0)
            {
                partNumber++;
                parts.Add(await WritePartAsync(window, partNumber, buffer, cancellationToken));
            }

            _telemetry.Information(
                $"ProductionChanges source pull completed for windfarm {window.WindfarmId}, " +
                $"LastUpdatedUtc window {window.WindowMinInclusiveUtc:O} to {window.WindowMaxInclusiveUtc:O}: " +
                $"{totalRows} row(s) read across {parts.Count} Parquet part(s).", window.RunId);

            if (totalRows == 0)
            {
                _telemetry.Information(
                    $"ProductionChanges window contains no rows; no notification will be published for windfarm {window.WindfarmId}.",
                    window.RunId);
                return new WindowProcessingResult(window, WindowOutcome.NoData, 0, 0, Array.Empty<DateOnly>(), null);
            }

            var envelopes = parts
                .Where(part => part.Part.RowCount > 0)
                .Select(part => BuildEnvelope(window, part.PartNumber, part.BlobPath, part.Part.RowCount))
                .ToList();

            _telemetry.Information(
                $"Publishing {envelopes.Count} queue message(s) for ProductionChanges Parquet part notification(s) for windfarm {window.WindfarmId}.",
                window.RunId);
            if (envelopes.Count > 0 && !await _publisher.PublishBatchAsync(envelopes, cancellationToken))
            {
                return new WindowProcessingResult(window, WindowOutcome.Failed, totalRows, parts.Count, publishedDates,
                    lastObserved, "One or more part notifications could not be confirmed delivered after bounded retries.");
            }

            _telemetry.Information(
                $"ProductionChanges window completed for windfarm {window.WindfarmId}: {totalRows} row(s), " +
                $"{envelopes.Count} part notification(s).", window.RunId);
            return new WindowProcessingResult(window, WindowOutcome.Succeeded, totalRows, parts.Count, publishedDates, lastObserved);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception error)
        {
            return new WindowProcessingResult(window, WindowOutcome.Failed, 0, 0, Array.Empty<DateOnly>(), null,
                error.Message, error);
        }
    }

    private async Task<(ProductionChangesPartResult Part, string BlobPath, int PartNumber)> WritePartAsync(
        ExtractionWindowExecutionRequest window, int partNumber, IReadOnlyList<ProductionChangeRowModel> rows, CancellationToken cancellationToken)
    {
        var expectedBlobPath = RawPathBuilder.BuildWindowPartPath(
            window.WindfarmName, window.ExtractionToken, window.WindowMinInclusiveUtc, window.WindowMaxInclusiveUtc, partNumber);
        _telemetry.Information(
            $"Starting ProductionChanges Parquet write for windfarm {window.WindfarmId}, part {partNumber}: " +
            $"{rows.Count} row(s), path '{expectedBlobPath}'.", window.RunId);
        var request = new ProductionChangesPartRequest(
            window.WindfarmId, window.WindfarmName,
            window.SourceType, window.SourceSystem, window.RunId, window.ExtractionToken,
            window.WindowMinInclusiveUtc, window.WindowMaxInclusiveUtc, partNumber);
        var result = await _partWriter.WritePartAsync(request, rows, cancellationToken);
        _telemetry.Information(
            $"ProductionChanges Parquet write completed for windfarm {window.WindfarmId}, part {partNumber}: " +
            $"{result.RowCount} row(s), path '{result.BlobPath}'.", window.RunId);
        return (result, result.BlobPath, partNumber);
    }

    private static RawArtifactAvailableV2 BuildEnvelope(
        ExtractionWindowExecutionRequest window, int partNumber, string blobPath, long rowCount)
        => new()
        {
            PayloadSchemaVersion = ProductionParquetSchemaV2.Version,
            ExtractionMode = "Incremental",
            PayloadKind = "ProductionChanges",
            TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString(),
            SourceType = window.SourceType,
            WindfarmId = window.WindfarmId,
            WindfarmName = window.WindfarmName,
            RunId = window.RunId, OperationId = window.OperationId,
            ExtractionToken = window.ExtractionToken,
            AttemptId = RawArtifactConventions.AttemptId(window.ExtractionToken),
            QueryAxis = RawArtifactConventions.LastUpdatedUtc,
            QueryStartInclusiveUtc = window.WindowMinInclusiveUtc,
            QueryEndExclusiveUtc = RawArtifactConventions.ExclusiveEnd(window.WindowMaxInclusiveUtc),
            QueryWindowNumber = window.WindowNumber,
            PartNumber = partNumber,
            BlobPath = blobPath,
            RowCount = rowCount,
        };
}
