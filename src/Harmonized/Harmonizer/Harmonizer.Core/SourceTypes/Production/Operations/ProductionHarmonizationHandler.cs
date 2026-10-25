using System.Text.Json;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Production.Serialization;

namespace Harmonizer.Core.SourceTypes.Production.Operations;

public sealed class ProductionHarmonizationHandler(ProductionWorkMessageReader reader,
    ProductionBatchProcessor processor) : IHarmonizationHandler
{
    public bool CanHandle(JsonElement envelope) => envelope.TryGetProperty("SourceType", out var source) && source.ValueKind == JsonValueKind.String && source.GetString() == "Production";

    public async Task<HarmonizationReceipt> ProcessAsync(string body, Action<OperationIdentity> onValidated, CancellationToken cancellationToken)
    {
        var work = await reader.ReadAsync(body, cancellationToken);
        onValidated(work.Operation);
        await processor.ProcessAsync(work, cancellationToken);
        return new(work.Batch, "Production");
    }
}
