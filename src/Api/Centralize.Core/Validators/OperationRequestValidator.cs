using Centralize.Core.Models;

namespace Centralize.Core.Validators;

public static class OperationRequestValidator
{
    public static readonly IReadOnlyList<string> SupportedSources = Array.AsReadOnly(new[] { "Production", "Outage", "Alarm", "Curtailment" });

    public static OperationRequestModel Normalize(OperationRequestModel request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(request.WindfarmId);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RequestedBy);
        if (request.RequestedBy.Length > 256)
            throw new ArgumentException("Requester must not exceed 256 characters.");
        if (request.StartDate == default || request.EndDate < request.StartDate || request.EndDate == DateOnly.MaxValue)
            throw new ArgumentException("Provide an inclusive UTC date range with a representable next-day boundary.");
        if (request.Reason?.Length > 4000) throw new ArgumentException("Reason must not exceed 4000 characters.");
        string? source = null;
        if (request.SourceType is not null)
            source = SupportedSources.FirstOrDefault(value => value.Equals(request.SourceType.Trim(), StringComparison.OrdinalIgnoreCase))
                ?? throw new ArgumentException("Supported sources are Production, Outage, Alarm and Curtailment; Adjustment cannot be triggered.");
        return request with { RequestedBy = request.RequestedBy.Trim(), SourceType = source };
    }

    public static DateOnly[] GetDates(OperationRequestModel request) =>
        Enumerable.Range(0, request.EndDate.DayNumber - request.StartDate.DayNumber + 1)
            .Select(request.StartDate.AddDays).ToArray();
}
