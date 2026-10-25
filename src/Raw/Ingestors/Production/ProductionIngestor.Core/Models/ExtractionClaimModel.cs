using ProductionIngestor.Configuration;

namespace ProductionIngestor.Models;

public sealed record ExtractionScopeModel(string SourceType);

public sealed record ExtractionClaimRequestModel(
    ExtractionScopeModel Scope, string OwnerId, IngestionOptions Policy,
    IngestionRequestModel? Refresh = null, string? RequestedBy = null, string? Reason = null,
    string? JobName = null, string? OperationId = null);

public sealed record ExtractionClaimModel(
    IngestionRequestModel Request, ExtractionLeaseModel Lease, ExtractionRunModel Run, string OwnerId);

public sealed record ExtractionLookupModel(ExtractionScopeModel Scope, int WindfarmId);

public sealed record ExtractionSelectionStatusModel(int RegisteredCount, int BusyCount, int DueCount);

public sealed record ExtractionAttemptResultModel(ExtractionClaimModel Claim, RunResultModel Result);