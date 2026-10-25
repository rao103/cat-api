using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Production.Interfaces;

namespace Harmonizer.UnitTests;

internal sealed class RecordingIdentityCatalog(IReadOnlyList<TurbineIdentity> identities) : IIdentityCatalog
{
    public int NameLookups { get; private set; }
    public int IdLookups { get; private set; }

    public Task<IReadOnlyList<TurbineIdentity>> ResolveAsync(WindfarmIdentity windfarm,
        IReadOnlyList<string> names, DateOnly dataDate, CancellationToken cancellationToken)
    {
        NameLookups++;
        return SelectAsync(identity => names.Contains(identity.WtgName, StringComparer.Ordinal), names.Count, cancellationToken);
    }

    public Task<IReadOnlyList<TurbineIdentity>> ResolveIdsAsync(WindfarmIdentity windfarm,
        IReadOnlyList<int> ids, DateOnly dataDate, CancellationToken cancellationToken)
    {
        IdLookups++;
        return SelectAsync(identity => ids.Contains(identity.WtgId), ids.Count, cancellationToken);
    }

    private Task<IReadOnlyList<TurbineIdentity>> SelectAsync(Func<TurbineIdentity, bool> predicate,
        int expectedCount, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = identities.Where(predicate).OrderByDescending(identity => identity.WtgId).ToArray();
        if (result.Length != expectedCount)
        {
            throw new WorkValidationException("Unknown test catalog identity.");
        }

        return Task.FromResult<IReadOnlyList<TurbineIdentity>>(result);
    }
}
