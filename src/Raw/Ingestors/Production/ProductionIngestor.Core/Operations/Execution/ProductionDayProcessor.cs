using System.Globalization;
using ProductionIngestor.Contracts;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Serialization;

namespace ProductionIngestor.Operations.Execution;

/// <summary>
/// Publishes a day only after its complete snapshot has been written and verified.
/// Failures propagate to the run boundary; they must never look like an empty successful day.
/// </summary>
internal sealed class ProductionDayProcessor(
    WindfarmDayExtractor extractor,
    ProductionParquetWriter writer,
    IRawArtifactPublisher publisher,
    IIngestionTelemetry<ProductionDayProcessor> telemetry)
{
    public async Task ProcessAsync(
        IngestionRunContext context, DateOnly date, CancellationToken cancellationToken)
    {
        var request = context.Request;
        var extractionToken = context.Lease.LeaseToken;
        var rows = new List<ProductionRowModel>();

        await foreach (var row in extractor.GetDeduplicatedSnapshotAsync(request.WindfarmName, date, cancellationToken))
        {
            rows.Add(row);
        }

        var result = await writer.WriteDayAsync(context.Lease.WindfarmName, date, extractionToken, rows,
            cancellationToken, request.WindfarmId);
        telemetry.DayWritten(request.WindfarmName, date, result.RowCount, result.QuarantinedRows.Count);

        var message = new RawArtifactAvailableV1
        {
            SourceType = request.SourceType,
            WindfarmId = request.WindfarmId,
            DataDate = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ExtractionToken = extractionToken,
            BlobPath = result.BlobPath,
            ShardPaths = result.ShardPaths?.ToArray(),
            RunId = request.RunId ?? extractionToken.ToString(CultureInfo.InvariantCulture),
        };

        await publisher.PublishAsync(message, cancellationToken);
    }
}