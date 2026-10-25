using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using NpgsqlTypes;

namespace CAT.RawFanOut.Repositories;

/// <summary>One bounded catalog read per new artifact; no per-row database queries.</summary>
public sealed class PostgresFarmCatalog(IPostgresConnectionFactory connectionFactory) : IFarmCatalog
{
    public async Task<FarmCatalog> LoadAsync(FarmCatalogRequest request, CancellationToken cancellationToken)
    {
        if (request.WindfarmId <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Windfarm ID must be positive.");

        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        // No active/lease/is_enabled filter: immutable Raw replay must remain possible.
        command.CommandText = """
            SELECT farm.windfarm_name, turbine.id AS wtgid, turbine.wtg_name
            FROM windfarm AS farm
            LEFT JOIN wind_turbine AS turbine ON turbine.windfarm_id = farm.id
            WHERE farm.id = @windfarm_id
            ORDER BY turbine.id;
        """;
        command.Parameters.AddWithValue("windfarm_id", NpgsqlDbType.Integer, request.WindfarmId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        string? name = null;
        var turbines = new List<TurbineIdentity>();
        var turbineIds = new HashSet<int>();
        while (await reader.ReadAsync(cancellationToken))
        {
            name = reader.GetString(0);
            if (reader.IsDBNull(1)) continue;
            var id = reader.GetInt32(1);
            if (!turbineIds.Add(id)) throw new InvalidDataException("Catalog join returned duplicate turbine IDs.");
            turbines.Add(new TurbineIdentity(id, reader.GetString(2)));
        }
        if (string.IsNullOrWhiteSpace(name))
            throw new KeyNotFoundException($"Windfarm {request.WindfarmId} is not registered in the control plane.");
        return new FarmCatalog(request.WindfarmId, name, turbines);
    }
}
