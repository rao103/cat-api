using Centralize.Core.Models;
using Centralize.Core.Repositories;
using Npgsql;

namespace Centralize.UnitTests;

public sealed class OperationRepositoryValidationTests
{
    [Theory]
    [InlineData("Sources")]
    [InlineData("Create")]
    [InlineData("OperationStatus")]
    [InlineData("RequestStatus")]
    public async Task Repository_UsesSuppliedConnectionFactory(string action)
    {
        var expected = new InvalidOperationException("Connection factory invoked.");
        var repository = new PostgresOperationRepository(() => throw expected);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            if (action == "Sources")
                await repository.GetRefreshSourcesAsync(new(12, new(2026, 9, 1), new(2026, 9, 30), "caller"), default);
            else if (action == "Create")
                await repository.CreateAsync("caller", default);
            else if (action == "OperationStatus")
                await repository.GetAsync(Guid.NewGuid(), "caller", default);
            else
                await repository.GetRefreshAttemptsAsync("request", default);
        });
        Assert.Same(expected, error);
    }

    [Fact]
    public async Task InvalidRefresh_IsRejectedBeforeDatabaseAccess()
    {
        await using var source = NpgsqlDataSource.Create("Host=unit.invalid;Database=unused;Username=unused");
        var repository = new PostgresOperationRepository(source);
        await Assert.ThrowsAnyAsync<ArgumentException>(() => repository.GetRefreshSourcesAsync(
            new(0, new(2026, 9, 1), new(2026, 9, 30), "caller"), default));
    }

    [Fact]
    public async Task CancelledSourceLookup_DoesNotOpenDatabase()
    {
        await using var source = NpgsqlDataSource.Create("Host=unit.invalid;Database=unused;Username=unused");
        var repository = new PostgresOperationRepository(source);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.GetRefreshSourcesAsync(
            new(12, new(2026, 9, 1), new(2026, 9, 30), "caller"), new CancellationToken(true)));
    }

    [Fact]
    public async Task InvalidOrCancelledIdentity_DoesNotOpenDatabase()
    {
        var repository = new PostgresOperationRepository(() => throw new InvalidOperationException("Must not connect"));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => repository.CreateAsync("", default));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.CreateAsync("caller", new CancellationToken(true)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MissingMonitoringIdentity_IsRejectedBeforeDatabaseAccess(bool missingOperation)
    {
        await using var source = NpgsqlDataSource.Create("Host=unit.invalid;Database=unused;Username=unused");
        var repository = new PostgresOperationRepository(source);
        await Assert.ThrowsAnyAsync<ArgumentException>(() => repository.GetAsync(
            missingOperation ? Guid.Empty : Guid.NewGuid(), missingOperation ? "caller" : "", default));
    }
}
