using System.Runtime.CompilerServices;
using OutageIngestor.Models;

namespace OutageIngestor.Resolvers;

public static class OutageIdentityResolver
{
    public static OutageEventModel Resolve(OutageEventModel row, WindfarmScopeModel scope)
    {
        if (row.WindfarmName != scope.WindfarmName || scope.WindfarmId <= 0)
            throw new InvalidDataException("Source row farm differs from the extraction scope.");
        var matches = scope.ActiveTurbines.Where(t => t.WtgName == row.WtgName).ToArray();
        if (matches.Length != 1 || matches[0].WtgId <= 0 ||
            (row.WindfarmId != 0 && row.WindfarmId != scope.WindfarmId) ||
            (row.WtgId != 0 && row.WtgId != matches[0].WtgId))
            throw new InvalidDataException("Missing, ambiguous or inconsistent source WTG catalog identity.");
        return row with { WindfarmId = scope.WindfarmId, WtgId = matches[0].WtgId };
    }

    public static async IAsyncEnumerable<OutageEventModel> ResolveAsync(IAsyncEnumerable<OutageEventModel> rows,
        WindfarmScopeModel scope, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var row in rows.WithCancellation(cancellationToken)) yield return Resolve(row, scope);
    }
}
