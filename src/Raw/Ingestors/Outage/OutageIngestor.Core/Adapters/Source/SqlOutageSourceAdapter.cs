using System.Data;
using Microsoft.Data.SqlClient;
using OutageIngestor.Factories;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;

namespace OutageIngestor.Adapters.Source;

/// <summary>
/// ADO.NET adapter over the legacy outage schema (`cat_gold.OutageClassification` /
/// `cat_gold.OutageClassificationPeriodDeleted`), scoped to one windfarm (`SiteRDSPPID`) per
/// call, matching the ExtractionCursor's (WindfarmId, SourceType) granularity. Every predicate
/// is a bind parameter; no windfarm/turbine identifier is ever concatenated into SQL text.
/// See README "Outage Source Contract" for the documented, unverified assumptions this
/// adapter relies on (LastUpdatedUTC monotonicity, deletion-feed retention).
/// </summary>
public sealed class SqlOutageSourceAdapter : IOutageSourceAdapter
{
    private readonly Func<SqlConnection> _createConnection;
    private readonly int _batchSize;

    /// <summary>Legacy path: a bare connection string with no AAD token (e.g. SQL auth embedded in the string).</summary>
    public SqlOutageSourceAdapter(string connectionString, int batchSize)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        _createConnection = () => new SqlConnection(connectionString);
        _batchSize = batchSize;
    }

    /// <summary>Token-authenticated path against the shared DART Synapse server, mirroring Production's SqlProductionSourceAdapter.</summary>
    public SqlOutageSourceAdapter(SqlOutageConnectionFactory connectionFactory, int batchSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        _createConnection = connectionFactory.CreateConnection;
        _batchSize = batchSize;
    }

    public async Task<DateTime> GetLatestSourceUpdatedDateByWindfarmIdAsync(string windfarmName, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(OutageSqlQueryBuilder.LatestSourceUpdatedDateQuery, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull
            ? DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc)
            : DateTime.SpecifyKind((DateTime)result, DateTimeKind.Utc);
    }

    public async IAsyncEnumerable<OutageEventModel> GetChangedOutagesAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime low,
        DateTime end,
        DateTime minRetrieveDateUtc,
        (DateTime LastUpdatedDate, string EventId)? continuationKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Bounded in memory by the window planner's MaxCatchUpWindowSize + safety lag, not
        // spill-safe for an unbounded window; see README for the documented limitation.
        var live = await ReadAllPagesAsync(windfarmName, activeTurbineNames, low, end, minRetrieveDateUtc, continuationKey, isDeletedFeed: false, cancellationToken);
        foreach (var evt in live)
        {
            yield return evt;
        }
    }

    public async IAsyncEnumerable<OutageEventModel> GetDeletedOutagesAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime low,
        DateTime end,
        (DateTime LastUpdatedDate, string EventId)? continuationKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // The deletion feed carries no minRetrieveDate bound (Section 2.2/9.1) — pass MinValue,
        // which ReadAllPagesAsync never applies for this feed anyway.
        var deleted = await ReadAllPagesAsync(windfarmName, activeTurbineNames, low, end, DateTime.MinValue, continuationKey, isDeletedFeed: true, cancellationToken);
        foreach (var evt in deleted)
        {
            yield return evt;
        }
    }

    /// <summary>
    /// Streams one scoped SELECT from DART. Synapse serverless transactions apply to metadata,
    /// not external data, so SQL Server SNAPSHOT isolation cannot protect this read. Do not
    /// replace this with independent TOP/keyset page queries: their views could differ.
    /// The writer buffers bounded parts and publishes a manifest only after enumeration ends
    /// successfully. Source/file-change errors propagate; no inconsistent-read hint or
    /// mid-stream restart is used. External-file consistency remains a source publication
    /// contract, not a transaction guarantee supplied by this adapter (see README).
    /// </summary>
    public async IAsyncEnumerable<OutageEventModel> GetAllCurrentEventsAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, DateTime scopeStartUtc, DateTime scopeEndUtc,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (activeTurbineNames.Count == 0) yield break;
        await using var connection = _createConnection();
        await using var command = CreateSnapshotCommand(connection, windfarmName, activeTurbineNames, minRetrieveDateUtc, scopeStartUtc, scopeEndUtc);
        await connection.OpenAsync(cancellationToken);
        // Default row buffering is intentional: ReadLiveRow accesses columns by name rather
        // than strictly increasing ordinals, which SequentialAccess would prohibit.
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult, cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return ReadLiveRow(reader);
        }
    }

    internal static SqlCommand CreateSnapshotCommand(
        SqlConnection connection, string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc,
        DateTime scopeStartUtc, DateTime scopeEndUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(windfarmName);
        if (scopeEndUtc <= scopeStartUtc)
            throw new ArgumentException("Refresh scope end must be strictly after scope start.");

        var command = new SqlCommand(OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(activeTurbineNames.Count), connection) { CommandTimeout = 600 };
        AddTurbineParameters(command, activeTurbineNames);
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.Add(new SqlParameter("@minRetrieveDate", SqlDbType.DateTime2) { Value = minRetrieveDateUtc, Scale = 7 });
        command.Parameters.Add(new SqlParameter("@scopeStartUtc", SqlDbType.DateTime2) { Value = scopeStartUtc, Scale = 7 });
        command.Parameters.Add(new SqlParameter("@scopeEndUtc", SqlDbType.DateTime2) { Value = scopeEndUtc, Scale = 7 });
        return command;
    }

    private async Task<List<OutageEventModel>> ReadAllPagesAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end, DateTime minRetrieveDateUtc,
        (DateTime LastUpdatedDate, string EventId)? continuationKey,
        bool isDeletedFeed, CancellationToken cancellationToken)
    {
        var results = new List<OutageEventModel>();
        cancellationToken.ThrowIfCancellationRequested();
        if (activeTurbineNames.Count == 0) return results;
        var afterUtc = continuationKey?.LastUpdatedDate;
        var afterEventId = continuationKey?.EventId;

        while (true)
        {
            await using var connection = _createConnection();
            await using var command = CreatePageCommand(connection, windfarmName, activeTurbineNames,
                low, end, minRetrieveDateUtc,
                afterUtc.HasValue ? (afterUtc.Value, afterEventId!) : null, isDeletedFeed, _batchSize);
            await connection.OpenAsync(cancellationToken);

            var page = new List<OutageEventModel>();
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    page.Add(isDeletedFeed ? ReadTombstoneRow(reader) : ReadLiveRow(reader));
                }
            }

            results.AddRange(page);

            if (page.Count < _batchSize)
            {
                break;
            }

            var lastRow = page[^1];
            afterUtc = lastRow.LastUpdatedDate;
            afterEventId = lastRow.EventId;
        }

        return results;
    }

    internal static SqlCommand CreatePageCommand(
        SqlConnection connection, string windfarmName, IReadOnlyList<string> activeTurbineNames,
        DateTime low, DateTime end, DateTime minRetrieveDateUtc,
        (DateTime LastUpdatedDate, string EventId)? continuationKey, bool isDeletedFeed, int batchSize)
    {
        var sql = isDeletedFeed
            ? OutageSqlQueryBuilder.BuildDeletedEventsPage(continuationKey.HasValue, activeTurbineNames.Count)
            : OutageSqlQueryBuilder.BuildChangedEventsPage(continuationKey.HasValue, activeTurbineNames.Count);
        var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        AddTurbineParameters(command, activeTurbineNames);
        command.Parameters.AddWithValue("@batchSize", batchSize);
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.Add(new SqlParameter("@low", SqlDbType.DateTime2) { Value = low, Scale = 7 });
        command.Parameters.Add(new SqlParameter("@end", SqlDbType.DateTime2) { Value = end, Scale = 7 });
        // Deletion rows have no event-time columns; retention must never filter tombstones.
        if (!isDeletedFeed)
            command.Parameters.Add(new SqlParameter("@minRetrieveDate", SqlDbType.DateTime2) { Value = minRetrieveDateUtc, Scale = 7 });
        if (continuationKey is { } after)
        {
            command.Parameters.Add(new SqlParameter("@afterUtc", SqlDbType.DateTime2) { Value = after.LastUpdatedDate, Scale = 7 });
            command.Parameters.AddWithValue("@afterOutageClassificationId", after.EventId);
        }
        return command;
    }

    private static void AddTurbineParameters(SqlCommand command, IReadOnlyList<string> turbineNames)
    {
        // Match Production's wtg_name -> WtgRDSPPID mapping and parameterized IN pattern.
        for (var i = 0; i < turbineNames.Count; i++)
            command.Parameters.Add(new SqlParameter($"@t{i}", SqlDbType.NVarChar, 256) { Value = turbineNames[i] });
    }

    internal static OutageEventModel ReadLiveRow(IDataReader reader)
    {
        var siteRdsppId = ReadIdentityString(reader, "SiteRDSPPID");
        var wtgRdsppId = ReadIdentityString(reader, "WtgRDSPPID");
        var outageId = ReadIdentityString(reader, "OutageID");
        var outageClassificationId = ReadIdentityString(reader, "OutageClassificationID");
        var startUtc = DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("OutageClassificationStartUTC")), DateTimeKind.Utc);
        var endOrd = reader.GetOrdinal("OutageClassificationEndUTC");
        // v11 requires a true null for an open outage; the legacy integrator's DateTime.MaxValue
        // sentinel is deliberately not reproduced here.
        DateTime? endUtc = reader.IsDBNull(endOrd) ? null : DateTime.SpecifyKind(reader.GetDateTime(endOrd), DateTimeKind.Utc);
        var lastUpdatedUtc = DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("LastUpdatedUTC")), DateTimeKind.Utc);

        return new OutageEventModel(
            EventId: outageClassificationId,
            WindfarmName: siteRdsppId,
            WtgName: wtgRdsppId,
            EventType: "Outage",
            OriginalStartUtc: startUtc,
            OriginalEndUtc: endUtc,
            Status: endUtc is null ? "Open" : "Closed",
            SourceRevision: null,
            SourceWatermarkUtc: lastUpdatedUtc,
            IsTombstone: false,
            LastUpdatedDate: lastUpdatedUtc,
            OutageId: outageId,
            Comment: ReadNullableString(reader, "OutageClassificationComment"),
            AlarmCode: ReadNullableString(reader, "OutageClassificationAlarmCode"),
            AlarmText: ReadNullableString(reader, "OutageClassificationAlarmText"),
            Category: ReadNullableString(reader, "OutageClassificationCategory"),
            SubCategory: ReadNullableString(reader, "OutageClassificationSubCategory"));
    }

    private static OutageEventModel ReadTombstoneRow(IDataReader reader)
    {
        var siteRdsppId = ReadIdentityString(reader, "SiteRDSPPID");
        var wtgRdsppId = ReadIdentityString(reader, "WtgRDSPPID");
        var outageId = ReadIdentityString(reader, "OutageID");
        var outageClassificationId = ReadIdentityString(reader, "OutageClassificationID");
        var lastUpdatedUtc = DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("LastUpdatedUtc")), DateTimeKind.Utc);

        // The deletion feed carries no start/end/status/classification columns; these fields
        // are unused for a tombstone (OutageBatchWriter.Validate skips start/end checks when
        // IsTombstone is true).
        return new OutageEventModel(
            EventId: outageClassificationId,
            WindfarmName: siteRdsppId,
            WtgName: wtgRdsppId,
            EventType: "Outage",
            OriginalStartUtc: default,
            OriginalEndUtc: null,
            Status: "Deleted",
            SourceRevision: null,
            SourceWatermarkUtc: lastUpdatedUtc,
            IsTombstone: true,
            LastUpdatedDate: lastUpdatedUtc,
            OutageId: outageId);
    }

    /// <summary>
    /// Reads a source identity column as a string without assuming its underlying SQL type is
    /// VARCHAR: the legacy integrator's mapper (<c>CreateOutageData</c>) never read <c>OutageID</c>
    /// at all, so its actual column type (VARCHAR vs UNIQUEIDENTIFIER) was never exercised here.
    /// <see cref="IDataRecord.GetValue"/> + invariant <see cref="Convert.ToString(object?, IFormatProvider?)"/>
    /// round-trips either shape (including a GUID) without throwing an InvalidCastException.
    /// </summary>
    internal static string ReadIdentityString(IDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        if (reader.IsDBNull(ordinal))
        {
            return string.Empty;
        }

        var value = reader.GetValue(ordinal);
        return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static string? ReadNullableString(IDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        var value = reader.GetValue(ordinal);
        return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
    }
}
