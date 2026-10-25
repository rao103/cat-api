using System.Text.Json;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Serialization;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

public sealed class OutageHarmonizationHandler(OutageWorkReader reader, OutageBatchProcessor processor) : IHarmonizationHandler
{
    public bool CanHandle(JsonElement envelope) => envelope.TryGetProperty("SourceType", out var source) && source.ValueKind == JsonValueKind.String && source.GetString() == "Outage";

    public async Task<HarmonizationReceipt> ProcessAsync(string body, Action<OperationIdentity> onValidated, CancellationToken cancellationToken)
    {
        var work = await reader.ReadAsync(body, cancellationToken);
        onValidated(work.Operation);
        await processor.ProcessAsync(work, cancellationToken);
        return new(work.Batch, "Outage");
    }
}
