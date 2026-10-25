using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Npgsql;

namespace Harmonizer.Core.SourceTypes.Outage.Repositories;

public sealed class PostgresOutageIdentityCatalog(NpgsqlDataSource dataSource,
    ITelemetry<PostgresOutageIdentityCatalog> telemetry) : IOutageIdentityCatalog
{
    public async Task<IReadOnlyList<TurbineIdentity>> ReadAsync(WindfarmIdentity windfarm, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT DISTINCT cursor.windfarm_id AS windfarm_id, farm.windfarm_name,
                   turbine.id AS wtgid, turbine.wtg_name
            FROM extraction_cursor AS cursor
            JOIN windfarm AS farm ON farm.id=cursor.windfarm_id
            JOIN wind_turbine AS turbine ON turbine.windfarm_id=farm.id
            WHERE cursor.windfarm_id=@windfarm_id AND cursor.source_type='Outage'
            ORDER BY wtgid;
            """);
        command.Parameters.AddWithValue("windfarm_id", windfarm.WindfarmId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<TurbineIdentity>();
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt32(0) != windfarm.WindfarmId || reader.GetString(1) != windfarm.WindfarmName)
                throw new WorkValidationException("Outage windfarm ID/name disagrees with the catalog.");
            result.Add(new(reader.GetInt32(2), reader.GetString(3)));
        }
        if (result.Count == 0 || result.Any(turbine => turbine.WtgId <= 0 || string.IsNullOrWhiteSpace(turbine.WtgName)) ||
            result.Select(turbine => turbine.WtgName).Distinct(StringComparer.Ordinal).Count() != result.Count)
            throw new WorkValidationException("Outage turbine catalog is missing or ambiguous.");
        telemetry.Debug("Outage catalog identities resolved. WindfarmId={WindfarmId} TurbineCount={TurbineCount}",
            windfarm.WindfarmId, result.Count);
        return result;
    }
}
