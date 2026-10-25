namespace AlarmIngestor.Enums;

/// <summary>
/// Incremental is the routine per-turbine ID-keyset discovery plus known-open polling pass
/// (Section "Normal Run"). Historical is the explicit bounded start-time range re-read
/// (Section "Historical Retrieval and Explicit Limitations") — it never advances the
/// discovery cursor and is not an authoritative snapshot.
/// </summary>
public enum IngestionMode
{
    Incremental,
    Historical,
}
