using System.Net.Http.Json;
using ProductionIngestor.Api.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Api.Triggering;

/// <summary>Development-only transport to the worker already running under the debugger.</summary>
public sealed class LocalIngestionJobTrigger(HttpClient client) : IIngestionJobTrigger
{
    public async Task<string> TriggerAsync(RefreshRequestModel request, CancellationToken cancellationToken)
    {
        var command = new LocalRefreshCommand(
            request.WindfarmId, request.WindfarmName, request.RunId,
            request.RequestedBy, request.Reason,
            string.Join(',', request.GetDataDates().Select(date =>
                date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture))));

        using var response = await client.PostAsJsonAsync("debug/refresh/", command, cancellationToken);
        response.EnsureSuccessStatusCode();
        return $"local-refresh-{request.RunId}";
    }

    private sealed record LocalRefreshCommand(
        string WindfarmId, string WindfarmName, string RunId,
        string RequestedBy, string? Reason, string DataDates);
}
