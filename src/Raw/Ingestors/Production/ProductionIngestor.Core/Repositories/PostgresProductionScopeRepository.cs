using Npgsql;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Repositories;

/// <summary>Postgres-backed <see cref="IProductionScopeRepository"/>.</summary>
public sealed class PostgresProductionScopeRepository : IProductionScopeRepository
{
    private readonly Func<NpgsqlConnection> _createConnection;

    public PostgresProductionScopeRepository(string connectionString)
    {
        _createConnection = () => new NpgsqlConnection(connectionString);
    }

    public PostgresProductionScopeRepository(PostgresConnectionFactory connectionFactory)
    {
        _createConnection = connectionFactory.CreateConnection;
    }

    public async Task<WindfarmScopeModel?> GetActiveScopeAsync(int windfarmId, DateTime asOfUtc, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        const string windfarmSql = """
            SELECT windfarm_name, min_retrieve_date
              FROM public.windfarm
             WHERE id = @windfarmId AND is_active = TRUE;
            """;

        string windfarmName;
        DateTime minRetrieveDateUtc;
        await using (var command = new NpgsqlCommand(windfarmSql, connection))
        {
            command.Parameters.Add("windfarmId", NpgsqlTypes.NpgsqlDbType.Integer).Value = windfarmId;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return null;

            windfarmName = reader.GetString(0);
            minRetrieveDateUtc = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
        }

        const string turbineSql = """
            SELECT wtg_name
              FROM public.wind_turbine
             WHERE windfarm_id = @windfarmId
               AND is_active = TRUE
               AND turbine_toc IS NOT NULL
               AND turbine_toc < @asOfUtc
             ORDER BY wtg_name;
            """;

        var turbineNames = new List<string>();
        await using (var command = new NpgsqlCommand(turbineSql, connection))
        {
            command.Parameters.Add("windfarmId", NpgsqlTypes.NpgsqlDbType.Integer).Value = windfarmId;
            command.Parameters.Add("asOfUtc", NpgsqlTypes.NpgsqlDbType.Timestamp).Value =
                DateTime.SpecifyKind(asOfUtc, DateTimeKind.Unspecified);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                turbineNames.Add(reader.GetString(0));
        }

        return new WindfarmScopeModel(windfarmId, windfarmName, minRetrieveDateUtc, turbineNames);
    }
}
