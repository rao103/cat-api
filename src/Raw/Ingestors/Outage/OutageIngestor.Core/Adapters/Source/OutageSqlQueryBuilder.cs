namespace OutageIngestor.Adapters.Source;

/// <summary>
/// Pure, parameterized SQL text builder for the outage source (`cat_gold.OutageClassification`
/// and `cat_gold.OutageClassificationPeriodDeleted`). No windfarm/turbine identifier is ever
/// concatenated into SQL text — every predicate is a bind parameter — unlike the legacy
/// integrator's <c>WtgRDSPPID IN ({windTurbines})</c> string interpolation. Turbine membership
/// filters rows without changing the windfarm-level cursor or classification-ID keyset.
/// </summary>
public static class OutageSqlQueryBuilder
{
    /// <summary>
    /// Keyset page of the live table within a half-open [@low, @end) LastUpdatedUTC window,
    /// resuming strictly after (@afterUtc, @afterOutageClassificationId) when supplied.
    /// Ordered by (LastUpdatedUTC, OutageClassificationID) — the source's real stable event
    /// identity, never a fabricated numeric row id and never tie-broken by turbine/item id
    /// (the shared `extraction_cursor.last_committed_id` carries only the OutageClassificationID).
    /// The @minRetrieveDate predicate reproduces the legacy `OutageRepository.GetData`
    /// minRetrivelDate bound exactly, including exclusion when Start equals the floor.
    /// </summary>
    public static string BuildChangedEventsPage(bool hasContinuation, int turbineCount) => $"""
        SELECT TOP (@batchSize)
               [SiteRDSPPID], [WtgRDSPPID], [OutageID], [OutageClassificationID],
               [OutageClassificationStartUTC], [OutageClassificationEndUTC],
               [OutageClassificationComment], [OutageClassificationAlarmCode], [OutageClassificationAlarmText],
               [OutageClassificationCategory], [OutageClassificationSubCategory], [LastUpdatedUTC]
          FROM [cat_gold].[OutageClassification]
         WHERE [SiteRDSPPID] = @siteRdsppId
           AND {BuildTurbinePredicate(turbineCount)}
           AND [LastUpdatedUTC] >= @low
           AND [LastUpdatedUTC] < @end
           AND {LegacyRetentionPredicate}
           {(hasContinuation ? "AND ([LastUpdatedUTC] > @afterUtc OR ([LastUpdatedUTC] = @afterUtc AND [OutageClassificationID] > @afterOutageClassificationId))" : string.Empty)}
         ORDER BY [LastUpdatedUTC], [OutageClassificationID];
        """;

    /// <summary>Keyset page of the deletion feed, same window/continuation shape as <see cref="BuildChangedEventsPage"/>.</summary>
    public static string BuildDeletedEventsPage(bool hasContinuation, int turbineCount) => $"""
        SELECT TOP (@batchSize)
               [SiteRDSPPID], [WtgRDSPPID], [OutageID], [OutageClassificationID], [LastUpdatedUtc]
          FROM [cat_gold].[OutageClassificationPeriodDeleted]
         WHERE [SiteRDSPPID] = @siteRdsppId
           AND {BuildTurbinePredicate(turbineCount)}
           AND [LastUpdatedUtc] >= @low
           AND [LastUpdatedUtc] < @end
           {(hasContinuation ? "AND ([LastUpdatedUtc] > @afterUtc OR ([LastUpdatedUtc] = @afterUtc AND [OutageClassificationID] > @afterOutageClassificationId))" : string.Empty)}
         ORDER BY [LastUpdatedUtc], [OutageClassificationID];
        """;

    /// <summary>
    /// One complete current-state query for Refresh mode, ordered by the same stable
    /// identity (no LastUpdatedUTC bound), scoped to events overlapping the requested half-open
    /// event-time range [@scopeStartUtc, @scopeEndUtc) per the guide's Section 9 predicate: an
    /// event starting before the scope's end and (open, or ending at/after the scope's start) is
    /// included, including events that started before the scope or remain open. Still bounded
    /// below by @minRetrieveDate (see <see cref="BuildChangedEventsPage"/>).
    /// </summary>
    public static string BuildAllCurrentEventsQuery(int turbineCount) => $"""
        SELECT
               [SiteRDSPPID], [WtgRDSPPID], [OutageID], [OutageClassificationID],
               [OutageClassificationStartUTC], [OutageClassificationEndUTC],
               [OutageClassificationComment], [OutageClassificationAlarmCode], [OutageClassificationAlarmText],
               [OutageClassificationCategory], [OutageClassificationSubCategory], [LastUpdatedUTC]
          FROM [cat_gold].[OutageClassification]
         WHERE [SiteRDSPPID] = @siteRdsppId
           AND {BuildTurbinePredicate(turbineCount)}
           AND {LegacyRetentionPredicate}
           AND [OutageClassificationStartUTC] < @scopeEndUtc
           AND ([OutageClassificationEndUTC] IS NULL OR [OutageClassificationEndUTC] > @scopeStartUtc)
         ORDER BY [WtgRDSPPID], [OutageClassificationID];
        """;

    private const string LegacyRetentionPredicate = "([OutageClassificationStartUTC] > @minRetrieveDate OR ([OutageClassificationStartUTC] < @minRetrieveDate AND [OutageClassificationEndUTC] > @minRetrieveDate) OR ([OutageClassificationStartUTC] < @minRetrieveDate AND [OutageClassificationEndUTC] IS NULL))";

    private static string BuildTurbinePredicate(int turbineCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(turbineCount);
        return turbineCount == 0
            ? "1 = 0"
            : $"[WtgRDSPPID] IN ({string.Join(",", Enumerable.Range(0, turbineCount).Select(i => $"@t{i}"))})";
    }

    /// <summary>
    /// Latest source updated-date query. Uses <c>MAX(LastUpdatedUTC)</c> across both streams; this
    /// assumes the source commits `LastUpdatedUTC` monotonically with transaction commit order and
    /// that no later-committing transaction can ever produce a value already read as the max — an
    /// assumption inherited unverified from the legacy integrator (see README "Outage Source
    /// Contract" for the documented risk and the safety-lag mitigation).
    /// </summary>
    public const string LatestSourceUpdatedDateQuery = """
        SELECT MAX(hi) FROM (
            SELECT MAX([LastUpdatedUTC]) AS hi FROM [cat_gold].[OutageClassification] WHERE [SiteRDSPPID] = @siteRdsppId
            UNION ALL
            SELECT MAX([LastUpdatedUtc]) AS hi FROM [cat_gold].[OutageClassificationPeriodDeleted] WHERE [SiteRDSPPID] = @siteRdsppId
        ) AS combined;
        """;
}
