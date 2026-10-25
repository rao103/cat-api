using System.Data;
using Microsoft.Data.SqlClient;
using OutageIngestor.Adapters.Source;

namespace OutageIngestor.UnitTests;

public sealed class OutageSnapshotCommandTests
{
    private static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void RefreshCommandPreservesExactScopeAndUsesNoTransactionOrPagingParameters()
    {
        using var connection = new SqlConnection();
        var start = Start.AddTicks(1234567);
        var end = start.AddMonths(1);
        const string name = "CHW01'; SELECT 1; --";
        using var command = SqlOutageSourceAdapter.CreateSnapshotCommand(connection, name, ["wtg-1"], Start.AddYears(-2), start, end);

        Assert.Same(connection, command.Connection);
        Assert.Null(command.Transaction);
        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Equal(600, command.CommandTimeout);
        Assert.Equal(OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(1), command.CommandText);
        Assert.DoesNotContain(name, command.CommandText);
        Assert.Equal(5, command.Parameters.Count);
        Assert.Equal(name, command.Parameters["@siteRdsppId"].Value);
        Assert.Equal(Start.AddYears(-2), command.Parameters["@minRetrieveDate"].Value);
        Assert.Equal(start, command.Parameters["@scopeStartUtc"].Value);
        Assert.Equal(end, command.Parameters["@scopeEndUtc"].Value);
        foreach (var parameterName in new[] { "@minRetrieveDate", "@scopeStartUtc", "@scopeEndUtc" })
        {
            Assert.Equal(SqlDbType.DateTime2, command.Parameters[parameterName].SqlDbType);
            Assert.Equal(7, command.Parameters[parameterName].Scale);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InvalidScopeIsRejectedBeforeOpeningConnection(int days)
    {
        using var connection = new SqlConnection();
        Assert.Throws<ArgumentException>(() => SqlOutageSourceAdapter.CreateSnapshotCommand(
            connection, "CHW01", ["wtg-1"], Start.AddYears(-2), Start, Start.AddDays(days)));
        Assert.Equal(ConnectionState.Closed, connection.State);
    }
}