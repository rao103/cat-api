namespace OutageIngestor.Models;

/// <summary>
/// Durable manifest for one Refresh snapshot (Incremental Refresh Spec, Sections 9-10). Written
/// to <c>snapshot/manifest.json</c> only after every listed part is already durable and the
/// source read completed successfully — never emitted for a partial/failed read. A valid empty
/// scope still produces a manifest with <see cref="RecordCount"/> = 0 and empty <see cref="Parts"/>.
/// </summary>
[Obsolete("Legacy manifest shape. New refreshes use CAT.Messaging.Contracts.RawSnapshotManifestV1.")]
public sealed record OutageSnapshotManifestModel(
    int SchemaVersion,
    string WindfarmId,
    string WindfarmName,
    string RunId,
    long ExtractionToken,
    DateTime ScopeStartUtc,
    DateTime ScopeEndUtc,
    DateTime SnapshotAsOfUtc,
    int RecordCount,
    IReadOnlyList<string> Parts);

/// <summary>Result of writing one Refresh snapshot: every durable part path plus the manifest
/// path referencing all of them. Unlike <see cref="RawWriteResultModel"/>, there is no
/// single-artifact/sharded distinction \u2014 a snapshot is always a manifest plus zero or more parts.</summary>
public sealed record RawSnapshotWriteResultModel(
    string WindfarmName,
    long ExtractionToken,
    string ManifestPath,
    IReadOnlyList<string> PartPaths,
    int RowCount,
    IReadOnlyList<QuarantinedEventModel> QuarantinedEvents);
