using System.Text.Json;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;

namespace Harmonizer.Core.Operations;

public sealed class HarmonizationDispatcher(IEnumerable<IHarmonizationHandler> handlers,
    ITelemetry<HarmonizationDispatcher> telemetry)
{
    private readonly IReadOnlyList<IHarmonizationHandler> strategies = handlers.ToArray();

    public async Task<HarmonizationReceipt> ProcessAsync(string body, Action<OperationIdentity> onValidated,
        CancellationToken cancellationToken)
    {
        using var document = JsonDocument.Parse(body, new() { MaxDepth = 32 });
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            throw new WorkValidationException("Harmonizer work must be a JSON object.");
        var matches = strategies.Where(handler => handler.CanHandle(document.RootElement)).ToArray();
        if (matches.Length != 1) throw new UnsupportedWorkException("No unique verified Harmonizer strategy for this work envelope.");
        telemetry.Debug("Harmonizer source strategy selected. Handler={Handler}", matches[0].GetType().Name);
        return await matches[0].ProcessAsync(body, onValidated, cancellationToken);
    }
}
