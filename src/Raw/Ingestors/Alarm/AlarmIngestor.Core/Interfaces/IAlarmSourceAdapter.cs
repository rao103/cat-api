using AlarmIngestor.Models;

namespace AlarmIngestor.Interfaces;

/// <summary>
/// Abstraction over the deployed Alarm HTTP API. No LastUpdatedUtc request is ever built —
/// the deployed API rejects that field (see AlarmSourceOptions research note in repo memory
/// and README "Alarm Source Contract"). Discovery uses ascending AlarmId keyset paging;
/// polling uses an explicit AlarmId-In filter. Both must process the complete result rather
/// than assume a single page/default limit covers the request.
/// </summary>
public interface IAlarmSourceAdapter
{
    /// <summary>
    /// Returns the current maximum AlarmId visible for this turbine/code/date-floor selection,
    /// used to freeze an upper bound for one discovery pass. Null when no alarms match.
    /// </summary>
    Task<long?> GetMaxAlarmIdAsync(
        string turbineRdspp, DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, CancellationToken cancellationToken);

    /// <summary>
    /// One ascending-AlarmId page strictly greater than <paramref name="afterAlarmId"/> and less
    /// than or equal to <paramref name="upperBoundAlarmId"/>, with TimeOnUtc greater than
    /// <paramref name="minRetrievalDateUtc"/>. Requests Alarm/Warning records in Good/Deleted state.
    /// </summary>
    Task<IReadOnlyList<AlarmEventModel>> DiscoverPageAsync(
        string windfarmId, string turbineRdspp, string itemId, long afterAlarmId, long upperBoundAlarmId,
        DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, int pageSize, CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves current state for exactly the given known-open AlarmIds, without any
    /// start-date restriction. IDs absent from the result are not returned as any record —
    /// callers must diff against the requested set themselves.
    /// </summary>
    Task<IReadOnlyList<AlarmEventModel>> PollKnownAlarmsAsync(
        string windfarmId, string turbineRdspp, string itemId, IReadOnlyList<long> alarmIds, CancellationToken cancellationToken);

    /// <summary>
    /// One bounded page of an explicit historical start-time range retrieval (Section
    /// "Historical Retrieval"). Callers page via <paramref name="offset"/> and stop once a page
    /// returns fewer than <paramref name="pageSize"/> rows; this method never accumulates the
    /// full range in memory itself.
    /// </summary>
    Task<IReadOnlyList<AlarmEventModel>> GetAlarmsBetweenDatesAsync(
        string windfarmId, string turbineRdspp, string itemId, DateTime startUtc, DateTime endUtc,
        IReadOnlyList<string> alarmCodes, int pageSize, int offset, CancellationToken cancellationToken);
}
