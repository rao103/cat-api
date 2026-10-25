using CAT.Messaging.Contracts;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Repositories;
using Npgsql;
using System;
using System.Threading.Tasks;
namespace CAT.RawFanOut.IntegrationTests;

[CollectionDefinition("Postgres",DisableParallelization=true)]
public sealed class PostgresCollection { }
internal sealed class TestPostgresConnectionFactory(NpgsqlDataSource dataSource) : IPostgresConnectionFactory
{
    public ValueTask<NpgsqlConnection> OpenConnectionAsync(System.Threading.CancellationToken cancellationToken) =>
        dataSource.OpenConnectionAsync(cancellationToken);
}

[Collection("Postgres")]
public sealed class PostgresTests
{
    // A fresh isolated database with the complete forward migration chain applied.
    internal static Task<MigrationDatabase> OpenAsync() => MigrationDatabase.CreateAsync(MigrationDatabase.FullChain);

    [ExternalIntegrationFact]
    public async Task ProductionIdentityCatalogJoinsTypedFarmAndTurbineKeys()
    {
        await using var database = await OpenAsync();
        var isolatedSource = database.DataSource;
        await using (var setup = isolatedSource.CreateCommand("""
            INSERT INTO windfarm(id, windfarm_name) VALUES (2, 'WOW02');
            INSERT INTO wind_turbine(id, wtg_name, windfarm_id) VALUES (1212, 'WOW02A12', 2), (1213, 'WOW02A13', 2);
            """))
            await setup.ExecuteNonQueryAsync();

        var catalog = new PostgresFarmCatalog(new TestPostgresConnectionFactory(isolatedSource));
        var result = await catalog.LoadAsync(new FarmCatalogRequest(2, "Production"), default);

        Assert.Equal(2, result.WindfarmId);
        Assert.Equal("WOW02", result.WindfarmName);
        Assert.Equal(new[]
        {
            new TurbineIdentity(1212, "WOW02A12"),
            new TurbineIdentity(1213, "WOW02A13")
        }, result.Turbines);
    }
}
