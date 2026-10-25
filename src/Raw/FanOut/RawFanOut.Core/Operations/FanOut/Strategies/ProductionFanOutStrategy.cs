using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Operations.FanOut.Strategies;

public sealed class ProductionFanOutStrategy(FanOutOptions? configuredOptions = null) : IRawFanOutStrategy
{
    private readonly FanOutOptions options = configuredOptions ?? new();
    public string SourceType => "Production";

    public IEnumerable<WorkSelection> BuildWork(FanOutPlanningContext context)
    {
        var artifact = context.Artifact;
        var rowsByDate = context.Rows.GroupBy(row => row.ProductionDate!.Value)
            .ToDictionary(group => group.Key, group => group.ToArray());
        var dates = artifact.IsExtractionReady ? RequestedDates(context) : rowsByDate.Keys.Order();
        foreach (var date in dates)
        {
            var rows = rowsByDate.GetValueOrDefault(date) ?? [];
            yield return new WorkSelection(HarmonizationWorkKinds.ProductionWtgDays, date,
                rows.Select(row => row.WtgId!.Value).Distinct().Order().ToArray(), [])
            {
                RawPaths = rows.Select(row => row.RawPath).OfType<string>().Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray(),
                ScopeItemIds = artifact.IsExtractionReady && rows.Length == 0
                    ? context.Farm.Turbines.Where(turbine => (artifact.ScopeWtgNames ?? []).Contains(turbine.WtgName, StringComparer.Ordinal))
                        .OrderBy(turbine => turbine.WtgName, StringComparer.Ordinal).Select(turbine => turbine.WtgId).ToArray()
                    : [],
                HasNoSourceRows = rows.Length == 0
            };
        }
        // ExtractionReady emits selected empty days itself. Its removal-evidence contract
        // must not be replaced by the older, unconditional reconciliation message.
        if (!artifact.IsExtractionReady && !artifact.IsIncrementalChanges && artifact.PayloadKind == "ProductionSnapshot")
            yield return new WorkSelection(HarmonizationWorkKinds.ProductionSnapshotReconciliation, artifact.DataDate, [], []);
    }

    private IEnumerable<DateOnly> RequestedDates(FanOutPlanningContext context)
    {
        var count = 0;
        foreach (var range in context.Artifact.RequestedScope ?? [])
        {
            var start = DateOnly.ParseExact(range.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            var end = DateOnly.ParseExact(range.EndDateInclusive, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            var days = end.DayNumber - start.DayNumber + 1;
            if (days > options.MaxScopeDatesPerExtraction - count)
                throw new InvalidDataException("Requested production dates exceed the configured limit.");
            count += days;
            for (var day = start.DayNumber; day <= end.DayNumber; day++) yield return DateOnly.FromDayNumber(day);
        }
    }
}
