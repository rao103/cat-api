namespace OutageIngestor.Enums;

/// <summary>Lifecycle status of one `extraction_run` audit row. Terminal once Completed/Failed.</summary>
public enum ExtractionRunStatus
{
    Running,
    Completed,
    Failed,
}
