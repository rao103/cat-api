namespace OutageIngestor.Contracts;

/// <summary>Recommended <see cref="RawOutageBatchAvailableV1.PayloadKind"/> values (Section 25 of the
/// Incremental Refresh Spec). Each kind carries one, and only one, semantic meaning.</summary>
public static class OutagePayloadKinds
{
    /// <summary>Incremental new/changed full event state (live/changed-outage feed).</summary>
    public const string OutageEvents = "OutageEvents";

    /// <summary>Incremental deletion/tombstone feed.</summary>
    public const string OutageDeletedEvents = "OutageDeletedEvents";

    /// <summary>Authoritative Refresh snapshot.</summary>
    public const string OutageSnapshot = "OutageSnapshot";
}
