namespace Centralize.Core.Models;

public sealed record OperationRequestModel(
    int WindfarmId, DateOnly StartDate, DateOnly EndDate, string RequestedBy,
    string? SourceType = null, string? Reason = null);
