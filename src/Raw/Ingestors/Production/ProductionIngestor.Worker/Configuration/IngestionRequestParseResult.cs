using System.Diagnostics.CodeAnalysis;
using ProductionIngestor.Models;

namespace ProductionIngestor.Worker.Configuration;

public sealed class IngestionRequestParseResult
{
    internal IngestionRequestParseResult(IngestionRequestModel request) => Request = request;

    internal IngestionRequestParseResult(IngestionRequestFailure failure, string error)
    {
        Failure = failure;
        Error = error;
    }

    [MemberNotNullWhen(true, nameof(Request))]
    [MemberNotNullWhen(false, nameof(Error))]
    public bool Succeeded => Request is not null;

    public IngestionRequestModel? Request { get; }

    // Only validation guidance is returned; never include raw configuration values.
    public string? Error { get; }

    internal IngestionRequestFailure Failure { get; }
}

internal enum IngestionRequestFailure
{
    None,
    RequiredWindfarmId,
    RequiredWIndfarmName,
    InvalidWindfarmId,
    RequiredRefreshDates,
    InvalidRefreshDates
}