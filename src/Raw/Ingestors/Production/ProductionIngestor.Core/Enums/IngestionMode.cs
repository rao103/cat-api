namespace ProductionIngestor.Enums;

/// <summary>Distinguishes normal incremental catch-up runs from historical refresh runs.</summary>
public enum IngestionMode
{
    Incremental,
    Refresh,
}
