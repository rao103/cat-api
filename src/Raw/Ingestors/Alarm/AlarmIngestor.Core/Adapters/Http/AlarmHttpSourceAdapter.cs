using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;

namespace AlarmIngestor.Adapters.Http;

/// <summary>
/// HTTP-backed <see cref="IAlarmSourceAdapter"/> against the deployed Alarm API's
/// POST /sdh/alarm/api/search endpoint. One source page fails the affected work outright
/// (Section "One source page fails") — there is no swallow-and-continue like the legacy
/// GetNewestAlarm catch. Processes the complete requested result set across pages rather than
/// assuming a single page/default limit covers it (Section "Process the complete result").
/// </summary>
public sealed class AlarmHttpSourceAdapter : IAlarmSourceAdapter
{
    private readonly HttpClient _httpClient;

    public AlarmHttpSourceAdapter(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<long?> GetMaxAlarmIdAsync(
        string turbineRdspp, DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, CancellationToken cancellationToken)
    {
        var request = AlarmSearchRequestBuilder.MaxAlarmId(turbineRdspp, minRetrievalDateUtc, alarmCodes);
        var response = await SendAsync(request, cancellationToken);
        var sequence = 0;
        var results = AlarmSearchRequestBuilder.ParseAlarms(response, string.Empty, turbineRdspp, string.Empty, DateTime.UtcNow, ref sequence);
        if (results.Count > 1)
        {
            throw new InvalidOperationException(
                $"Max-alarm-id lookup for turbine {turbineRdspp} returned {results.Count} rows for a top:1 request.");
        }

        return results.Count == 0 ? null : results[0].AlarmId;
    }

    // Exactly one bounded request per call — the reader owns keyset paging via afterAlarmId/upperBoundAlarmId
    // across successive calls, so this adapter must never loop internally (Section "reader controls keyset").
    public async Task<IReadOnlyList<AlarmEventModel>> DiscoverPageAsync(
        string windfarmId, string turbineRdspp, string itemId, long afterAlarmId, long upperBoundAlarmId,
        DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, int pageSize, CancellationToken cancellationToken)
    {
        var observedAtUtc = DateTime.UtcNow;
        var sequence = 0;
        var request = AlarmSearchRequestBuilder.ByAlarmIdRange(
            turbineRdspp, afterAlarmId, upperBoundAlarmId, minRetrievalDateUtc, alarmCodes, pageSize, 0);
        var response = await SendAsync(request, cancellationToken);
        return AlarmSearchRequestBuilder.ParseAlarms(response, windfarmId, turbineRdspp, itemId, observedAtUtc, ref sequence);
    }

    // Paginates by offset but is strictly bounded to the requested ID set (Section "requested known IDs
    // can be >500 and must paginate"); a returned ID outside the request, a duplicate, or a repeated
    // non-progressing page is a hard failure rather than a silent stop.
    public async Task<IReadOnlyList<AlarmEventModel>> PollKnownAlarmsAsync(
        string windfarmId, string turbineRdspp, string itemId, IReadOnlyList<long> alarmIds, CancellationToken cancellationToken)
    {
        if (alarmIds.Count == 0)
        {
            return [];
        }

        var requested = new HashSet<long>(alarmIds);
        var seen = new HashSet<long>();
        var results = new List<AlarmEventModel>();
        var offset = 0;
        var observedAtUtc = DateTime.UtcNow;
        var sequence = 0;
        const int pageSize = 500;

        while (true)
        {
            var request = AlarmSearchRequestBuilder.ByAlarmIds(turbineRdspp, alarmIds, pageSize, offset);
            var response = await SendAsync(request, cancellationToken);
            var page = AlarmSearchRequestBuilder.ParseAlarms(response, windfarmId, turbineRdspp, itemId, observedAtUtc, ref sequence);

            if (page.Count > pageSize)
            {
                throw new InvalidOperationException(
                    $"Alarm poll page for turbine {turbineRdspp} returned {page.Count} rows exceeding requested page size {pageSize}.");
            }

            foreach (var alarm in page)
            {
                if (!requested.Contains(alarm.AlarmId))
                {
                    throw new InvalidOperationException(
                        $"Alarm poll page for turbine {turbineRdspp} returned AlarmId {alarm.AlarmId} outside the requested set.");
                }

                if (!seen.Add(alarm.AlarmId))
                {
                    throw new InvalidOperationException(
                        $"Alarm poll page for turbine {turbineRdspp} returned duplicate AlarmId {alarm.AlarmId}.");
                }
            }

            results.AddRange(page);

            if (seen.Count == requested.Count)
            {
                break;
            }

            if (page.Count == 0 || page.Count < pageSize)
            {
                break;
            }

            offset += pageSize;
        }

        return results;
    }

    public async Task<IReadOnlyList<AlarmEventModel>> GetAlarmsBetweenDatesAsync(
        string windfarmId, string turbineRdspp, string itemId, DateTime startUtc, DateTime endUtc,
        IReadOnlyList<string> alarmCodes, int pageSize, int offset, CancellationToken cancellationToken)
    {
        var observedAtUtc = DateTime.UtcNow;
        var sequence = 0;
        var request = AlarmSearchRequestBuilder.BetweenDates(turbineRdspp, startUtc, endUtc, alarmCodes, pageSize, offset);
        var response = await SendAsync(request, cancellationToken);
        return AlarmSearchRequestBuilder.ParseAlarms(response, windfarmId, turbineRdspp, itemId, observedAtUtc, ref sequence);
    }

    private async Task<string> SendAsync(SearchRequest request, CancellationToken cancellationToken)
    {
        using var content = AlarmSearchRequestBuilder.Serialize(request);
        using var response = await _httpClient.PostAsync("search", content, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(cancellationToken);
    }
}
