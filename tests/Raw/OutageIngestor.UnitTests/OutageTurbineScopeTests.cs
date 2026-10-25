using System.Data;
using Microsoft.Data.SqlClient;
using OutageIngestor.Adapters.Source;

namespace OutageIngestor.UnitTests;

public sealed class OutageTurbineScopeTests
{
    private static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("live", false)]
    [InlineData("live", true)]
    [InlineData("deleted", false)]
    [InlineData("deleted", true)]
    [InlineData("refresh", false)]
    public void CommandsBindTurbineNamesWithoutInterpolatingValues(string feed, bool continued)
    {
        string[] turbines = ["CHW01-A01", "CHW01-A02'); DROP TABLE anything; --"];
        using var connection = new SqlConnection();
        using var command = feed == "refresh"
            ? SqlOutageSourceAdapter.CreateSnapshotCommand(connection, "CHW01", turbines, Start, Start, Start.AddMonths(1))
            : SqlOutageSourceAdapter.CreatePageCommand(connection, "CHW01", turbines,
                Start, Start.AddHours(1), Start.AddYears(-1),
                continued ? (Start.AddMinutes(1), "OC-1") : null, feed == "deleted", 100);

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Contains("[WtgRDSPPID] IN (@t0,@t1)", command.CommandText);
        for (var i = 0; i < turbines.Length; i++)
        {
            Assert.DoesNotContain(turbines[i], command.CommandText);
            var parameter = command.Parameters[$"@t{i}"];
            Assert.Equal(turbines[i], parameter.Value);
            Assert.Equal(SqlDbType.NVarChar, parameter.SqlDbType);
            Assert.Equal(256, parameter.Size);
        }
        Assert.Equal(feed != "deleted", command.Parameters.Contains("@minRetrieveDate"));
        Assert.Equal(continued, command.Parameters.Contains("@afterUtc"));
        Assert.Equal(continued, command.Parameters.Contains("@afterOutageClassificationId"));
        if (continued)
        {
            Assert.Equal(Start.AddMinutes(1), command.Parameters["@afterUtc"].Value);
            Assert.Equal("OC-1", command.Parameters["@afterOutageClassificationId"].Value);
        }
        if (feed != "refresh")
        {
            Assert.Equal(Start, command.Parameters["@low"].Value);
            Assert.Equal(Start.AddHours(1), command.Parameters["@end"].Value);
            Assert.Equal(100, command.Parameters["@batchSize"].Value);
        }
    }

    [Theory]
    [InlineData("live")]
    [InlineData("deleted")]
    [InlineData("refresh")]
    public async Task EmptyScopeReturnsNoRowsWithoutEvenCreatingAConnection(string feed)
    {
        // This string would throw on SqlConnection construction, before any network I/O.
        var adapter = new SqlOutageSourceAdapter("invalid connection string", 1);
        var rows = feed switch
        {
            "live" => adapter.GetChangedOutagesAsync("CHW01", [], Start, Start.AddHours(1), Start, null, default),
            "deleted" => adapter.GetDeletedOutagesAsync("CHW01", [], Start, Start.AddHours(1), null, default),
            _ => adapter.GetAllCurrentEventsAsync("CHW01", [], Start, Start, Start.AddMonths(1), default)
        };
        await foreach (var row in rows)
            Assert.Fail($"Empty scope returned event {row.EventId}.");
    }

    [Theory]
    [InlineData(-1, -1, false)]
    [InlineData(-1, 0, false)]
    [InlineData(-1, 1, true)]
    [InlineData(-1, null, true)]
    [InlineData(0, -1, false)]
    [InlineData(0, 0, false)]
    [InlineData(0, 1, false)]
    [InlineData(0, null, false)]
    [InlineData(1, -1, true)]
    [InlineData(1, 1, true)]
    [InlineData(1, null, true)]
    public void GeneratedRetentionPredicatePreservesStrictLegacyBoundary(int startDays, int? endDays, bool expected)
    {
        // Exercise the generated predicate with an in-memory expression evaluator (not a SQL
        // server integration test). Include even inconsistent source intervals: Start > floor
        // is independently sufficient in legacy SQL, regardless of End.
        using var table = new DataTable();
        table.Columns.Add("OutageClassificationStartUTC", typeof(DateTime));
        table.Columns.Add("OutageClassificationEndUTC", typeof(DateTime));
        table.Columns.Add("Floor", typeof(DateTime));
        table.Rows.Add(Start.AddDays(startDays), endDays.HasValue ? Start.AddDays(endDays.Value) : DBNull.Value, Start);
        foreach (var sql in new[] { OutageSqlQueryBuilder.BuildChangedEventsPage(false, 1),
                     OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(1) })
        {
            var retentionLine = Assert.Single(sql.Split('\n'), line => line.Contains("@minRetrieveDate"));
            var predicate = retentionLine.Trim()[4..].Replace("@minRetrieveDate", "[Floor]");
            Assert.Equal(expected, table.Select(predicate).Length == 1);
        }
    }
}