using OutageIngestor.Adapters.Source;

namespace OutageIngestor.UnitTests;

/// <summary>
/// Legacy turbine/retention filter parity while preserving the ingestor's half-open windows,
/// classification-ID keyset and single scoped refresh query.
/// </summary>
public sealed class OutageSqlQueryBuilderTests
{
    [Fact]
    public void ChangedEventsPageBoundsAHalfOpenLastUpdatedWindow()
    {
        var sql = OutageSqlQueryBuilder.BuildChangedEventsPage(hasContinuation: false, turbineCount: 2);
        Assert.Contains("[LastUpdatedUTC] >= @low", sql);
        Assert.Contains("[LastUpdatedUTC] < @end", sql);
        Assert.DoesNotContain("@afterUtc", sql);
    }

    [Fact]
    public void DeletedEventsPageBoundsTheSameHalfOpenWindow()
    {
        var sql = OutageSqlQueryBuilder.BuildDeletedEventsPage(hasContinuation: false, turbineCount: 2);
        Assert.Contains("[LastUpdatedUtc] >= @low", sql);
        Assert.Contains("[LastUpdatedUtc] < @end", sql);
        Assert.DoesNotContain("@afterUtc", sql);
    }

    [Fact]
    public void ChangedEventsPageContinuationUsesStableStringKeyset()
    {
        var sql = OutageSqlQueryBuilder.BuildChangedEventsPage(hasContinuation: true, turbineCount: 2);
        Assert.Contains("[LastUpdatedUTC] > @afterUtc", sql);
        Assert.Contains("[OutageClassificationID] > @afterOutageClassificationId", sql);
        Assert.DoesNotContain("@afterWtg", sql);
        Assert.Contains("ORDER BY [LastUpdatedUTC], [OutageClassificationID]", sql);
    }

    [Fact]
    public void DeletedEventsPageContinuationUsesTheIdenticalKeysetIdentityAsChangedEvents()
    {
        var sql = OutageSqlQueryBuilder.BuildDeletedEventsPage(hasContinuation: true, turbineCount: 2);
        Assert.Contains("[LastUpdatedUtc] > @afterUtc", sql);
        Assert.Contains("[OutageClassificationID] > @afterOutageClassificationId", sql);
        Assert.DoesNotContain("@afterWtg", sql);
        Assert.Contains("ORDER BY [LastUpdatedUtc], [OutageClassificationID]", sql);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LiveQueriesUseExactLegacyRetentionButDeletionHasNoRetention(bool hasContinuation)
    {
        // Intentionally independent of the implementation constant. Strict < excludes Start == floor,
        // even for an open event; an end-only predicate is not equivalent.
        const string expected = "AND ([OutageClassificationStartUTC] > @minRetrieveDate OR ([OutageClassificationStartUTC] < @minRetrieveDate AND [OutageClassificationEndUTC] > @minRetrieveDate) OR ([OutageClassificationStartUTC] < @minRetrieveDate AND [OutageClassificationEndUTC] IS NULL))";
        foreach (var sql in new[] { OutageSqlQueryBuilder.BuildChangedEventsPage(hasContinuation, 2),
                     OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(2) })
        {
            Assert.Contains(expected, sql);
            Assert.Equal(4, System.Text.RegularExpressions.Regex.Matches(sql, "@minRetrieveDate").Count);
            Assert.DoesNotContain("[OutageClassificationStartUTC] <=", sql);
            Assert.DoesNotContain("[OutageClassificationStartUTC] >=", sql);
        }
        var deleted = OutageSqlQueryBuilder.BuildDeletedEventsPage(hasContinuation, 2);
        Assert.DoesNotContain("@minRetrieveDate", deleted);
        Assert.DoesNotContain("OutageClassificationStartUTC", deleted);
        Assert.DoesNotContain("OutageClassificationEndUTC", deleted);
    }

    [Fact]
    public void AllCurrentEventsQueryHasNoLastUpdatedWindowBound()
    {
        var sql = OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(2);
        Assert.DoesNotContain("[LastUpdatedUTC] >=", sql);
        Assert.DoesNotContain("[LastUpdatedUTC] <", sql);
        Assert.DoesNotContain("@low", sql);
        Assert.DoesNotContain("@end", sql);
        Assert.Contains("ORDER BY [WtgRDSPPID], [OutageClassificationID]", sql);
    }

    [Fact]
    public void RefreshIsOneScopedLiveQueryWithoutPaginationOrSqlServerIsolationRequirements()
    {
        var sql = OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(2);
        Assert.Contains("[SiteRDSPPID] = @siteRdsppId", sql);
        Assert.Contains("[OutageClassificationStartUTC] < @scopeEndUtc", sql);
        Assert.Contains("([OutageClassificationEndUTC] IS NULL OR [OutageClassificationEndUTC] >= @scopeStartUtc)", sql);
        Assert.Contains("[OutageClassificationEndUTC] > @minRetrieveDate", sql);
        foreach (var forbidden in new[] { "TOP", "OFFSET", "@batchSize", "@after", "PeriodDeleted",
                     "sys.databases", "SNAPSHOT", "TRANSACTION", "NOLOCK", "ALLOW_INCONSISTENT_READS" })
            Assert.DoesNotContain(forbidden, sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LatestSourceUpdatedDateQueryUnionsBothStreamsScopedToOneSite()
    {
        Assert.Contains("[cat_gold].[OutageClassification]", OutageSqlQueryBuilder.LatestSourceUpdatedDateQuery);
        Assert.Contains("[cat_gold].[OutageClassificationPeriodDeleted]", OutageSqlQueryBuilder.LatestSourceUpdatedDateQuery);
        Assert.Contains("UNION ALL", OutageSqlQueryBuilder.LatestSourceUpdatedDateQuery);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(
            OutageSqlQueryBuilder.LatestSourceUpdatedDateQuery, "@siteRdsppId").Count);
    }

    [Fact]
    public void NoQueryEverConcatenatesAWindfarmOrTurbineIdentifierIntoText()
    {
        // Every predicate must be a bind parameter; unlike the legacy integrator's
        // `WtgRDSPPID IN ({windTurbines})` string interpolation.
        foreach (var sql in new[]
        {
            OutageSqlQueryBuilder.BuildChangedEventsPage(true, 2),
            OutageSqlQueryBuilder.BuildChangedEventsPage(false, 2),
            OutageSqlQueryBuilder.BuildDeletedEventsPage(true, 2),
            OutageSqlQueryBuilder.BuildDeletedEventsPage(false, 2),
            OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(2),
        })
        {
            Assert.Contains("[WtgRDSPPID] IN (@t0,@t1)", sql);
            Assert.Contains("@siteRdsppId", sql);
        }
    }

    [Fact]
    public void EmptyTurbineScopeIsAlwaysFalseAndNeverAnInvalidEmptyInClause()
    {
        foreach (var sql in new[]
        {
            OutageSqlQueryBuilder.BuildChangedEventsPage(false, 0),
            OutageSqlQueryBuilder.BuildChangedEventsPage(true, 0),
            OutageSqlQueryBuilder.BuildDeletedEventsPage(false, 0),
            OutageSqlQueryBuilder.BuildDeletedEventsPage(true, 0),
            OutageSqlQueryBuilder.BuildAllCurrentEventsQuery(0)
        })
        {
            Assert.Contains("AND 1 = 0", sql);
            Assert.DoesNotContain(" IN (", sql);
            Assert.DoesNotContain("@t0", sql);
        }
    }
}
