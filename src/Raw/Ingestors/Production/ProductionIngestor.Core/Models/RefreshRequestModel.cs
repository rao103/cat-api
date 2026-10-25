namespace ProductionIngestor.Models;

/// <summary>
/// A requested historical Production refresh: explicit windfarm + UTC production dates,
/// regardless of whether the source's LastUpdatedUtc has changed for them.
/// </summary>
public sealed record RefreshRequestModel(
    string WindfarmId,
    string WindfarmName,
    DateOnly StartDate,
    DateOnly EndDate,
    string RequestedBy,
    string Reason,
    string RunId)
{
    public IReadOnlyList<DateOnly> GetDataDates()
    {
        if (EndDate < StartDate)
        {
            throw new ArgumentException("EndDate must be on or after StartDate.", nameof(EndDate));
        }

        var dayCount = EndDate.DayNumber - StartDate.DayNumber + 1;
        return Enumerable.Range(0, dayCount)
            .Select(offset => DateOnly.FromDayNumber(StartDate.DayNumber + offset))
            .ToArray();
    }
}
