using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
namespace CAT.RawFanOut.IntegrationTests;

// Each instance owns a throwaway database on the isolated local server with the fresh-deploy scripts applied.
internal sealed class MigrationDatabase : IAsyncDisposable
{
    public static readonly string[] FullChain =
        ["0001_create_reference_schema.sql", "0002_create_extraction_schema.sql", "0003_create_raw_fanout_schema.sql"];

    private const string DefaultAdminConnection =
        "Host=localhost;Port=55432;Database=rawfanout_test;Username=rawfanout_test;Password=local-test-only;SSL Mode=Disable";
    private readonly string adminConnectionString;

    private MigrationDatabase(string name, string adminConnectionString, string connectionString)
    {
        Name = name;
        this.adminConnectionString = adminConnectionString;
        ConnectionString = connectionString;
        DataSource = NpgsqlDataSource.Create(connectionString);
    }

    public string Name { get; }
    public NpgsqlDataSource DataSource { get; }
    // Npgsql strips the password from DataSource.ConnectionString, so keep the usable one.
    public string ConnectionString { get; }
    public NpgsqlCommand CreateCommand(string sql) => DataSource.CreateCommand(sql);
    public static implicit operator NpgsqlDataSource(MigrationDatabase database) => database.DataSource;

    public static async Task<MigrationDatabase> CreateAsync(params string[] scripts)
    {
        var admin = new NpgsqlConnectionStringBuilder(
            Environment.GetEnvironmentVariable("RAWFANOUT_TEST_POSTGRES") ?? DefaultAdminConnection);
        // Intentionally restricted: never create/drop databases on company servers.
        if (admin.Host is not ("localhost" or "127.0.0.1") || admin.Database != "rawfanout_test")
            throw new InvalidOperationException("Integration DDL requires the isolated local rawfanout_test server.");
        var name = "rawfanout_it_" + Guid.NewGuid().ToString("N");
        await using (var adminSource = NpgsqlDataSource.Create(admin.ConnectionString))
        {
            await using var create = adminSource.CreateCommand($"CREATE DATABASE \"{name}\"");
            await create.ExecuteNonQueryAsync();
        }
        var builder = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = name };
        var database = new MigrationDatabase(name, admin.ConnectionString, builder.ConnectionString);
        try { await database.ApplyAsync(scripts); }
        catch { await database.DisposeAsync(); throw; }
        return database;
    }

    public async Task ApplyAsync(params string[] scripts)
    {
        foreach (var script in scripts)
        {
            await using var command = DataSource.CreateCommand(
                await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, script)));
            await command.ExecuteNonQueryAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await using var adminSource = NpgsqlDataSource.Create(adminConnectionString);
        await using var drop = adminSource.CreateCommand($"DROP DATABASE IF EXISTS \"{Name}\" WITH (FORCE)");
        await drop.ExecuteNonQueryAsync();
    }
}

internal static class Sql
{
    public static async Task<int> ExecAsync(NpgsqlDataSource source, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = source.CreateCommand(sql);
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        return await command.ExecuteNonQueryAsync();
    }

    public static async Task<T?> ScalarAsync<T>(NpgsqlDataSource source, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = source.CreateCommand(sql);
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        var result = await command.ExecuteScalarAsync();
        return result is null or DBNull ? default : (T)result;
    }

    public static string Hex64() => Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");

    public static string PlanJson(IEnumerable<string> batchIds) =>
        JsonSerializer.Serialize(batchIds.Select((id, i) => new { Id = id, Body = $"body-{i}" }));

    // Calls the current (actor-aware) lifecycle function directly.
    public static async Task<string> SavePlanAsync(
        NpgsqlDataSource source, string artifactId, IReadOnlyList<string> batchIds, string actor, int farm = 7, long token = 5)
    {
        await using var command = source.CreateCommand("""
            SELECT count(*) FROM control.raw_artifact_save_plan(@a,'Outage',@f,'run',@t,'{"k":1}'::jsonb,@p,@actor)
            """);
        command.Parameters.AddWithValue("a", artifactId);
        command.Parameters.AddWithValue("f", NpgsqlDbType.Integer, farm);
        command.Parameters.AddWithValue("t", NpgsqlDbType.Bigint, token);
        command.Parameters.AddWithValue("p", NpgsqlDbType.Jsonb, PlanJson(batchIds));
        command.Parameters.AddWithValue("actor", actor);
        await command.ExecuteScalarAsync();
        return artifactId;
    }
}
