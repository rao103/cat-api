using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Npgsql;
using NpgsqlTypes;
namespace Harmonizer.Core.SourceTypes.Production.Repositories;

/// <summary>Uses the catalog join already present in the supplied project history.</summary>
public sealed class PostgresIdentityCatalog(NpgsqlDataSource dataSource,
    ITelemetry<PostgresIdentityCatalog> telemetry) : IIdentityCatalog
{
    public async Task<IReadOnlyList<TurbineIdentity>> ResolveAsync(WindfarmIdentity windfarm,
        IReadOnlyList<string> names, DateOnly dataDate, CancellationToken cancellationToken)
    {
        if (names.Count == 0 || names.Count > 50 || names.Any(string.IsNullOrWhiteSpace) || names.Distinct(StringComparer.Ordinal).Count() != names.Count)
        {
            throw new WorkValidationException("Expected at most 50 distinct turbine names.");
        }

        var result = await ReadIdentitiesAsync(windfarm, names, [], cancellationToken);
        if (result.Count != names.Count || result.Select(turbine => turbine.WtgId).Distinct().Count() != result.Count ||
            !result.Select(turbine => turbine.WtgName).ToHashSet(StringComparer.Ordinal).SetEquals(names))
        {
            throw MissingIdentity(windfarm, names.Count, result.Count);
        }

        return result;
    }

    public async Task<IReadOnlyList<TurbineIdentity>> ResolveIdsAsync(WindfarmIdentity windfarm,
        IReadOnlyList<int> ids, DateOnly dataDate, CancellationToken cancellationToken)
    {
        if (ids.Count == 0 || ids.Count > 50 || ids.Any(id => id <= 0) || ids.Distinct().Count() != ids.Count)
        {
            throw new WorkValidationException("Expected at most 50 distinct positive turbine IDs.");
        }

        var result = await ReadIdentitiesAsync(windfarm, [], ids, cancellationToken);
        if (result.Count != ids.Count || !result.Select(turbine => turbine.WtgId).ToHashSet().SetEquals(ids) ||
            result.Select(turbine => turbine.WtgName).Distinct(StringComparer.Ordinal).Count() != result.Count)
        {
            throw MissingIdentity(windfarm, ids.Count, result.Count);
        }

        return result;
    }

    private async Task<IReadOnlyList<TurbineIdentity>> ReadIdentitiesAsync(WindfarmIdentity windfarm,
        IReadOnlyList<string> names, IReadOnlyList<int> ids, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT DISTINCT cursor.windfarm_id AS windfarm_id,
                   farm.windfarm_name,
                   turbine.id AS wtgid,
                   turbine.wtg_name
            FROM extraction_cursor AS cursor
            INNER JOIN windfarm AS farm ON farm.id = cursor.windfarm_id
            INNER JOIN wind_turbine AS turbine ON turbine.windfarm_id = farm.id
                AND (turbine.wtg_name = ANY(@wtg_names) OR turbine.id = ANY(@wtg_ids))
            WHERE cursor.windfarm_id = @windfarm_id AND cursor.source_type = 'Production'
            ORDER BY wtgid;
            """);
        command.Parameters.AddWithValue("windfarm_id", NpgsqlDbType.Integer, windfarm.WindfarmId);
        command.Parameters.AddWithValue("wtg_names", NpgsqlDbType.Array | NpgsqlDbType.Text, names.ToArray());
        command.Parameters.AddWithValue("wtg_ids", NpgsqlDbType.Array | NpgsqlDbType.Integer, ids.ToArray());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<TurbineIdentity>();
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt32(0) != windfarm.WindfarmId || reader.GetString(1) != windfarm.WindfarmName)
            {
                telemetry.Warning("Production windfarm identity disagrees with catalog. WindfarmId={WindfarmId}",
                    windfarm.WindfarmId);
                throw new WorkValidationException("Windfarm ID/name does not match the authoritative catalog.");
            }

            result.Add(new(reader.GetInt32(2), reader.GetString(3)));
        }
        // The observed catalog has no effective-dated identity columns. Do not invent temporal predicates.
        telemetry.Debug("Production turbine identities resolved. WindfarmId={WindfarmId} TurbineCount={TurbineCount}",
            windfarm.WindfarmId, result.Count);
        return result;
    }

    private WorkValidationException MissingIdentity(WindfarmIdentity windfarm, int requestedCount, int matchedCount)
    {
        telemetry.Warning("Production turbine identity lookup is missing or ambiguous. WindfarmId={WindfarmId} RequestedCount={RequestedCount} MatchedCount={MatchedCount}",
            windfarm.WindfarmId, requestedCount, matchedCount);
        return new("A turbine identity is unknown, ambiguous, or belongs to another windfarm.");
    }
}
