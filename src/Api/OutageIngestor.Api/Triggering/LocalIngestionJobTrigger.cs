using System.Globalization;
using System.Net.Http.Json;
using OutageIngestor.Api.Interfaces;
using OutageIngestor.Models;

namespace OutageIngestor.Api.Triggering;

/// <summary>Development-only transport to the worker already running under the debugger.</summary>
public sealed class LocalIngestionJobTrigger(HttpClient client) : IIngestionJobTrigger
{
    public async Task<string> TriggerAsync(RefreshRequestModel request, CancellationToken cancellationToken)
    {
        // Keep the worker's string-based environment/input contract; Core uses the numeric database ID.
        var command = new LocalRefreshCommand(
            request.WindfarmId.ToString(CultureInfo.InvariantCulture), request.WindfarmName, request.RunId,
            request.RequestedBy, request.Reason,
            request.ScopeStartUtc.ToString("O", CultureInfo.InvariantCulture),
            request.ScopeEndUtc.ToString("O", CultureInfo.InvariantCulture));

        using var response = await client.PostAsJsonAsync("debug/refresh/", command, cancellationToken);
        response.EnsureSuccessStatusCode();
        return $"local-refresh-{request.RunId}";
    }

    private sealed record LocalRefreshCommand(
        string WindfarmId, string WindfarmName, string RunId, string RequestedBy, string? Reason,
        string ScopeStartUtc, string ScopeEndUtc);
}