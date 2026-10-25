namespace CurtailmentIngestor.Enums;

/// <summary>Distinguishes normal incremental catch-up runs from full historical refresh runs.</summary>
public enum IngestionMode
{
    Incremental,
    Refresh,
}
