namespace ProductionIngestor.Models;

public sealed record WindfarmSourceQueryModel(string WindfarmId);

public sealed record LatestSourceUpdatedDateModel(DateTime? LastUpdatedUtc);