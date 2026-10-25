using System.Text.Json;
using Harmonizer.Core.Models;

namespace Harmonizer.Core.Interfaces;

public sealed record HarmonizationReceipt(BatchIdentity Batch, string SourceType);

/// <summary>A source strategy owns its verified reader and use case.</summary>
public interface IHarmonizationHandler
{
    bool CanHandle(JsonElement envelope);
    Task<HarmonizationReceipt> ProcessAsync(string body, Action<OperationIdentity> onValidated,
        CancellationToken cancellationToken);
}
