using Microsoft.Data.SqlClient;
using ProductionIngestor.Configuration;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using System.Data;
using System.Runtime.CompilerServices;

namespace ProductionIngestor.Adapters.Source;

/// <summary>
/// SQL Server-backed <see cref="IProductionSourceAdapter"/>, querying the same
/// [cat_gold].[WtgPowerProductionContractual] table as the legacy ContractualAvailabilityApp
/// integrator (see SourceSystemIntegrator/Repository/ProductionRepository.cs), using the same
/// raw ADO.NET/ordinal-read/TOP-batching style.
/// </summary>
public sealed class SqlProductionSourceAdapter : IProductionSourceAdapter
{
    private readonly Func<SqlConnection> _createConnection;
    private readonly IngestionOptions _ingestionOptions;

    public SqlProductionSourceAdapter(SourceOptions sourceOptions, IngestionOptions ingestionOptions)
    {
        _createConnection = () => new SqlConnection(sourceOptions.ConnectionString);
        _ingestionOptions = ingestionOptions;
    }

    public SqlProductionSourceAdapter(SqlProductionConnectionFactory connectionFactory, IngestionOptions ingestionOptions)
    {
        _createConnection = connectionFactory.CreateConnection;
        _ingestionOptions = ingestionOptions;
    }

    public async Task<LatestSourceUpdatedDateModel> GetLatestSourceUpdatedDateByWindfarmIdAsync(
        WindfarmSourceQueryModel query, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT MAX(pp.[LastUpdatedUtc])
              FROM [cat_gold].[WtgPowerProductionContractual] AS pp
             WHERE pp.[SiteRDSPPID] = @siteRdsppId;
            """;

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@siteRdsppId", query.WindfarmId);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return new LatestSourceUpdatedDateModel(result is DateTime lastUpdated
            ? DateTime.SpecifyKind(lastUpdated, DateTimeKind.Utc) : null);
    }

    public async IAsyncEnumerable<ChangedProductionRowModel> GetChangedRowsAsync(
        string windfarmName,
        DateTime low,
        DateTime end,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_ingestionOptions.SourceQueryBatchSize);
        const string sql = """
            SELECT TOP (@batchSize) pp.[WtgRDSPPID], pp.[StartDateAndTimeUTC], pp.[LastUpdatedUtc]
              FROM [cat_gold].[WtgPowerProductionContractual] AS pp
             WHERE pp.[SiteRDSPPID] = @siteRdsppId
               AND pp.[LastUpdatedUtc] > @low
               AND pp.[LastUpdatedUtc] < @end
            AND (@hasContinuation = 0 OR pp.[LastUpdatedUtc] > @continuationDate
                OR (pp.[LastUpdatedUtc] = @continuationDate AND pp.[WtgRDSPPID] > @continuationItemId)
                OR (pp.[LastUpdatedUtc] = @continuationDate AND pp.[WtgRDSPPID] = @continuationItemId
                    AND pp.[StartDateAndTimeUTC] > @continuationTimestamp))
            ORDER BY pp.[LastUpdatedUtc], pp.[WtgRDSPPID], pp.[StartDateAndTimeUTC];
            """;

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@batchSize", _ingestionOptions.SourceQueryBatchSize);
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.Add(new SqlParameter("@low", SqlDbType.DateTime2) { Scale = 7, Value = low });
        command.Parameters.Add(new SqlParameter("@end", SqlDbType.DateTime2) { Scale = 7, Value = end });
        command.Parameters.Add("@hasContinuation", SqlDbType.Bit).Value = false;
        command.Parameters.Add(new SqlParameter("@continuationDate", SqlDbType.DateTime2) { Scale = 7, Value = low });
        command.Parameters.Add("@continuationItemId", SqlDbType.NVarChar, 256).Value = string.Empty;
        command.Parameters.Add(new SqlParameter("@continuationTimestamp", SqlDbType.DateTime2) { Scale = 7, Value = low });

        while (true)
        {
        var pageCount = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var itemIdOrdinal = reader.GetOrdinal("WtgRDSPPID");
        var productionTimestampOrdinal = reader.GetOrdinal("StartDateAndTimeUTC");
        var lastUpdatedOrdinal = reader.GetOrdinal("LastUpdatedUtc");

        while (await reader.ReadAsync(cancellationToken))
        {
            var lastUpdatedUtc = DateTime.SpecifyKind(reader.GetDateTime(lastUpdatedOrdinal), DateTimeKind.Utc);
            var productionTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(productionTimestampOrdinal), DateTimeKind.Utc);
            var itemId = reader.GetString(itemIdOrdinal);
            command.Parameters["@hasContinuation"].Value = true;
            command.Parameters["@continuationDate"].Value = lastUpdatedUtc;
            command.Parameters["@continuationItemId"].Value = itemId;
            command.Parameters["@continuationTimestamp"].Value = productionTimestampUtc;
            pageCount++;

            yield return new ChangedProductionRowModel(
                LastUpdatedUtc: lastUpdatedUtc,
                ItemId: itemId,
                ProductionTimestampUtc: productionTimestampUtc,
                ProductionDate: DateOnly.FromDateTime(productionTimestampUtc));
        }
        if (pageCount < _ingestionOptions.SourceQueryBatchSize) yield break;
        }
    }

    public async IAsyncEnumerable<ProductionRowModel> GetWindfarmDaySnapshotAsync(
        string windfarmName,
        DateTime dayStartUtc,
        DateTime dayEndUtc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_ingestionOptions.SourceQueryBatchSize);
        const string sql = """
            SELECT TOP (@batchSize) pp.[WtgRDSPPID]
                   ,pp.[StartDateAndTimeUTC]
                     ,pp.[EndDateAndTimeUTC]
                   ,pp.[ContractualPoPEEstimatedPowerProductionkWh]
                   ,pp.[ContractualPoPE]
                     ,pp.[ContractualPoPEColumn]
                   ,pp.[LostProductionCapacitykWh]
                   ,pp.[SiteAverageWindSpeed]
                   ,pp.[LastUpdatedUtc]
              FROM [cat_gold].[WtgPowerProductionContractual] AS pp
             WHERE pp.[SiteRDSPPID] = @siteRdsppId
               AND pp.[StartDateAndTimeUTC] >= @dayStartUtc
               AND pp.[StartDateAndTimeUTC] < @dayEndUtc
            AND (@hasContinuation = 0 OR pp.[StartDateAndTimeUTC] > @continuationTimestamp
                OR (pp.[StartDateAndTimeUTC] = @continuationTimestamp AND pp.[WtgRDSPPID] > @continuationWtgName))
             ORDER BY pp.[StartDateAndTimeUTC], pp.[WtgRDSPPID];
            """;

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@batchSize", _ingestionOptions.SourceQueryBatchSize);
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.AddWithValue("@dayStartUtc", dayStartUtc);
        command.Parameters.AddWithValue("@dayEndUtc", dayEndUtc);
        command.Parameters.Add("@hasContinuation", SqlDbType.Bit).Value = false;
        command.Parameters.Add(new SqlParameter("@continuationTimestamp", SqlDbType.DateTime2) { Scale = 7, Value = dayStartUtc });
        command.Parameters.Add("@continuationWtgName", SqlDbType.NVarChar, 256).Value = string.Empty;

        while (true)
        {
        var pageCount = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var wtgNameOrdinal = reader.GetOrdinal("WtgRDSPPID");
        var productionTimestampOrdinal = reader.GetOrdinal("StartDateAndTimeUTC");
        var endTimestampOrdinal = reader.GetOrdinal("EndDateAndTimeUTC");
        var actualProductionOrdinal = reader.GetOrdinal("ContractualPoPEEstimatedPowerProductionkWh");
        var possibleProductionOrdinal = reader.GetOrdinal("ContractualPoPE");
        var contractualPoPEColumnOrdinal = reader.GetOrdinal("ContractualPoPEColumn");
        var lostProductionOrdinal = reader.GetOrdinal("LostProductionCapacitykWh");
        var windSpeedOrdinal = reader.GetOrdinal("SiteAverageWindSpeed");
        var lastUpdatedOrdinal = reader.GetOrdinal("LastUpdatedUtc");

        while (await reader.ReadAsync(cancellationToken))
        {
            var lastUpdatedUtc = DateTime.SpecifyKind(reader.GetDateTime(lastUpdatedOrdinal), DateTimeKind.Utc);
            var lostProductionCapacitykWh = reader.IsDBNull(lostProductionOrdinal) ? (decimal?)null : reader.GetDecimal(lostProductionOrdinal);
            var siteAverageWindSpeedMps = reader.IsDBNull(windSpeedOrdinal) ? (decimal?)null : reader.GetDecimal(windSpeedOrdinal);

            var wtgName = reader.GetString(wtgNameOrdinal);
            var productionTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(productionTimestampOrdinal), DateTimeKind.Utc);
            var endTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(endTimestampOrdinal), DateTimeKind.Utc);
            command.Parameters["@hasContinuation"].Value = true;
            command.Parameters["@continuationTimestamp"].Value = productionTimestampUtc;
            command.Parameters["@continuationWtgName"].Value = wtgName;
            pageCount++;
            yield return new ProductionRowModel(
                WindfarmName: windfarmName,
                WtgName: wtgName,
                ProductionTimestampUtc: productionTimestampUtc,
                ActualProduction: reader.IsDBNull(actualProductionOrdinal) ? null : reader.GetDecimal(actualProductionOrdinal),
                PossibleProduction: reader.IsDBNull(possibleProductionOrdinal) ? null : reader.GetDecimal(possibleProductionOrdinal),
                LostProductionCapacitykWh: lostProductionCapacitykWh,
                SiteAverageWindSpeedMps: siteAverageWindSpeedMps,
                LastUpdatedUtc: lastUpdatedUtc,
                SourceRowRevision: null,
                EndDateAndTimeUtc: endTimestampUtc,
                ContractualPoPEColumn: reader.IsDBNull(contractualPoPEColumnOrdinal) ? null : reader.GetString(contractualPoPEColumnOrdinal));
        }
            if (pageCount < _ingestionOptions.SourceQueryBatchSize) yield break;
            }
    }

    public async IAsyncEnumerable<ProductionRowModel> GetRefreshSnapshotAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime minRetrieveDateUtc,
        DateTime dayStartUtc,
        DateTime dayEndUtc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (activeTurbineNames.Count == 0) yield break;
        var (turbineClause, turbineParameters) = BuildTurbineInClause(activeTurbineNames);
        var sql = $"""
            SELECT pp.[WtgRDSPPID]
                   ,pp.[StartDateAndTimeUTC]
                     ,pp.[EndDateAndTimeUTC]
                   ,pp.[ContractualPoPEEstimatedPowerProductionkWh]
                   ,pp.[ContractualPoPE]
                     ,pp.[ContractualPoPEColumn]
                   ,pp.[LostProductionCapacitykWh]
                   ,pp.[SiteAverageWindSpeed]
                   ,pp.[LastUpdatedUtc]
              FROM [cat_gold].[WtgPowerProductionContractual] AS pp
             WHERE pp.[SiteRDSPPID] = @siteRdsppId
               AND pp.[StartDateAndTimeUTC] >= @dayStartUtc
               AND pp.[StartDateAndTimeUTC] < @dayEndUtc
               AND pp.[StartDateAndTimeUTC] > @minRetrieveDate
               AND pp.[WtgRDSPPID] IN ({turbineClause})
             ORDER BY pp.[StartDateAndTimeUTC], pp.[WtgRDSPPID];
            """;

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.Add(new SqlParameter("@dayStartUtc", SqlDbType.DateTime2) { Scale = 7, Value = dayStartUtc });
        command.Parameters.Add(new SqlParameter("@dayEndUtc", SqlDbType.DateTime2) { Scale = 7, Value = dayEndUtc });
        command.Parameters.Add(new SqlParameter("@minRetrieveDate", SqlDbType.DateTime2) { Scale = 7, Value = minRetrieveDateUtc });
        command.Parameters.AddRange(turbineParameters);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var wtgNameOrdinal = reader.GetOrdinal("WtgRDSPPID");
        var productionTimestampOrdinal = reader.GetOrdinal("StartDateAndTimeUTC");
        var endTimestampOrdinal = reader.GetOrdinal("EndDateAndTimeUTC");
        var actualProductionOrdinal = reader.GetOrdinal("ContractualPoPEEstimatedPowerProductionkWh");
        var possibleProductionOrdinal = reader.GetOrdinal("ContractualPoPE");
        var contractualPoPEColumnOrdinal = reader.GetOrdinal("ContractualPoPEColumn");
        var lostProductionOrdinal = reader.GetOrdinal("LostProductionCapacitykWh");
        var windSpeedOrdinal = reader.GetOrdinal("SiteAverageWindSpeed");
        var lastUpdatedOrdinal = reader.GetOrdinal("LastUpdatedUtc");

        while (await reader.ReadAsync(cancellationToken))
        {
            var lastUpdatedUtc = DateTime.SpecifyKind(reader.GetDateTime(lastUpdatedOrdinal), DateTimeKind.Utc);
            var lostProductionCapacitykWh = reader.IsDBNull(lostProductionOrdinal) ? (decimal?)null : reader.GetDecimal(lostProductionOrdinal);
            var siteAverageWindSpeedMps = reader.IsDBNull(windSpeedOrdinal) ? (decimal?)null : reader.GetDecimal(windSpeedOrdinal);

            var wtgName = reader.GetString(wtgNameOrdinal);
            var productionTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(productionTimestampOrdinal), DateTimeKind.Utc);
            var endTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(endTimestampOrdinal), DateTimeKind.Utc);
            yield return new ProductionRowModel(
                WindfarmName: windfarmName,
                WtgName: wtgName,
                ProductionTimestampUtc: productionTimestampUtc,
                ActualProduction: reader.IsDBNull(actualProductionOrdinal) ? null : reader.GetDecimal(actualProductionOrdinal),
                PossibleProduction: reader.IsDBNull(possibleProductionOrdinal) ? null : reader.GetDecimal(possibleProductionOrdinal),
                LostProductionCapacitykWh: lostProductionCapacitykWh,
                SiteAverageWindSpeedMps: siteAverageWindSpeedMps,
                LastUpdatedUtc: lastUpdatedUtc,
                SourceRowRevision: null,
                EndDateAndTimeUtc: endTimestampUtc,
                ContractualPoPEColumn: reader.IsDBNull(contractualPoPEColumnOrdinal) ? null : reader.GetString(contractualPoPEColumnOrdinal));
        }
    }

    public async Task<DateTime?> GetLatestLastUpdatedUtcAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, CancellationToken cancellationToken)
    {
        if (activeTurbineNames.Count == 0) return null;

        var (turbineClause, turbineParameters) = BuildTurbineInClause(activeTurbineNames);
        var sql = $"""
            SELECT MAX(pp.[LastUpdatedUtc])
              FROM [cat_gold].[WtgPowerProductionContractual] AS pp
             WHERE pp.[SiteRDSPPID] = @siteRdsppId
               AND pp.[StartDateAndTimeUTC] > @minRetrieveDate
               AND pp.[WtgRDSPPID] IN ({turbineClause});
            """;

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.Add(new SqlParameter("@minRetrieveDate", SqlDbType.DateTime2) { Scale = 7, Value = minRetrieveDateUtc });
        command.Parameters.AddRange(turbineParameters);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is DateTime lastUpdated ? DateTime.SpecifyKind(lastUpdated, DateTimeKind.Utc) : null;
    }

    public async IAsyncEnumerable<ProductionChangeRowModel> GetProductionChangeRowsAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime minRetrieveDateUtc,
        DateTime windowMinInclusiveUtc,
        DateTime windowMaxInclusiveUtc,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_ingestionOptions.SourceQueryBatchSize);
        if (activeTurbineNames.Count == 0) yield break;

        var (turbineClause, turbineParameters) = BuildTurbineInClause(activeTurbineNames);
        // Ordered/paginated by the source's unique production key (StartDateAndTimeUTC, WtgRDSPPID),
        // NOT by LastUpdatedUtc: that column is only a filter bound for this window.
        var sql = $"""
            SELECT TOP (@batchSize) pp.[WtgRDSPPID], pp.[StartDateAndTimeUTC], pp.[EndDateAndTimeUTC], pp.[LastUpdatedUtc]
                   ,pp.[ContractualPoPEEstimatedPowerProductionkWh]
                   ,pp.[ContractualPoPE]
                   ,pp.[ContractualPoPEColumn]
                   ,pp.[LostProductionCapacitykWh]
                   ,pp.[SiteAverageWindSpeed]
              FROM [cat_gold].[WtgPowerProductionContractual] AS pp
             WHERE pp.[SiteRDSPPID] = @siteRdsppId
               AND pp.[WtgRDSPPID] IN ({turbineClause})
               AND pp.[StartDateAndTimeUTC] > @minRetrieveDate
               AND pp.[LastUpdatedUtc] >= @windowMin
               AND pp.[LastUpdatedUtc] <= @windowMax
            AND (@hasContinuation = 0 OR pp.[StartDateAndTimeUTC] > @continuationTimestamp
                OR (pp.[StartDateAndTimeUTC] = @continuationTimestamp AND pp.[WtgRDSPPID] > @continuationWtgName))
            ORDER BY pp.[StartDateAndTimeUTC], pp.[WtgRDSPPID];
            """;

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 600 };
        command.Parameters.AddWithValue("@batchSize", _ingestionOptions.SourceQueryBatchSize);
        command.Parameters.AddWithValue("@siteRdsppId", windfarmName);
        command.Parameters.Add(new SqlParameter("@minRetrieveDate", SqlDbType.DateTime2) { Scale = 7, Value = minRetrieveDateUtc });
        command.Parameters.Add(new SqlParameter("@windowMin", SqlDbType.DateTime2) { Scale = 7, Value = windowMinInclusiveUtc });
        command.Parameters.Add(new SqlParameter("@windowMax", SqlDbType.DateTime2) { Scale = 7, Value = windowMaxInclusiveUtc });
        command.Parameters.Add("@hasContinuation", SqlDbType.Bit).Value = false;
        command.Parameters.Add(new SqlParameter("@continuationTimestamp", SqlDbType.DateTime2) { Scale = 7, Value = windowMinInclusiveUtc });
        command.Parameters.Add("@continuationWtgName", SqlDbType.NVarChar, 256).Value = string.Empty;
        command.Parameters.AddRange(turbineParameters);

        while (true)
        {
            var pageCount = 0;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var wtgNameOrdinal = reader.GetOrdinal("WtgRDSPPID");
            var productionTimestampOrdinal = reader.GetOrdinal("StartDateAndTimeUTC");
            var endTimestampOrdinal = reader.GetOrdinal("EndDateAndTimeUTC");
            var lastUpdatedOrdinal = reader.GetOrdinal("LastUpdatedUtc");
            var actualProductionOrdinal = reader.GetOrdinal("ContractualPoPEEstimatedPowerProductionkWh");
            var possibleProductionOrdinal = reader.GetOrdinal("ContractualPoPE");
            var contractualPoPEColumnOrdinal = reader.GetOrdinal("ContractualPoPEColumn");
            var lostProductionOrdinal = reader.GetOrdinal("LostProductionCapacitykWh");
            var windSpeedOrdinal = reader.GetOrdinal("SiteAverageWindSpeed");

            while (await reader.ReadAsync(cancellationToken))
            {
                var lastUpdatedUtc = DateTime.SpecifyKind(reader.GetDateTime(lastUpdatedOrdinal), DateTimeKind.Utc);
                var productionTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(productionTimestampOrdinal), DateTimeKind.Utc);
                var endTimestampUtc = DateTime.SpecifyKind(reader.GetDateTime(endTimestampOrdinal), DateTimeKind.Utc);
                var wtgName = reader.GetString(wtgNameOrdinal);
                command.Parameters["@hasContinuation"].Value = true;
                command.Parameters["@continuationTimestamp"].Value = productionTimestampUtc;
                command.Parameters["@continuationWtgName"].Value = wtgName;
                pageCount++;

                yield return new ProductionChangeRowModel(
                    WtgName: wtgName,
                    ProductionTimestampUtc: productionTimestampUtc,
                    EndDateAndTimeUtc: endTimestampUtc,
                    LastUpdatedUtc: lastUpdatedUtc,
                    ActualProduction: reader.IsDBNull(actualProductionOrdinal) ? null : reader.GetDecimal(actualProductionOrdinal),
                    PossibleProduction: reader.IsDBNull(possibleProductionOrdinal) ? null : reader.GetDecimal(possibleProductionOrdinal),
                    ContractualPoPEColumn: reader.IsDBNull(contractualPoPEColumnOrdinal) ? null : reader.GetString(contractualPoPEColumnOrdinal),
                    LostProductionCapacitykWh: reader.IsDBNull(lostProductionOrdinal) ? null : reader.GetDecimal(lostProductionOrdinal),
                    SiteAverageWindSpeedMps: reader.IsDBNull(windSpeedOrdinal) ? null : reader.GetDecimal(windSpeedOrdinal));
            }
            if (pageCount < _ingestionOptions.SourceQueryBatchSize) yield break;
        }
    }

    /// <summary>Builds a parameterized `IN (@t0,@t1,...)` clause; never string-concatenates turbine values into SQL.</summary>
    private static (string Clause, SqlParameter[] Parameters) BuildTurbineInClause(IReadOnlyList<string> turbineNames)
    {
        var parameters = new SqlParameter[turbineNames.Count];
        var names = new string[turbineNames.Count];
        for (var i = 0; i < turbineNames.Count; i++)
        {
            var parameterName = $"@t{i}";
            names[i] = parameterName;
            parameters[i] = new SqlParameter(parameterName, SqlDbType.NVarChar, 256) { Value = turbineNames[i] };
        }
        return (string.Join(",", names), parameters);
    }
}
