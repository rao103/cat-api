using Harmonizer.Core.Models;
namespace Harmonizer.Core.SourceTypes.Production.Interfaces;

// V2 resolves names and refresh scope IDs; V3 already carries both fields.
public interface IIdentityCatalog
{
    Task<IReadOnlyList<TurbineIdentity>> ResolveAsync(WindfarmIdentity windfarm,
        IReadOnlyList<string> names, DateOnly dataDate, CancellationToken cancellationToken);

    Task<IReadOnlyList<TurbineIdentity>> ResolveIdsAsync(WindfarmIdentity windfarm,
        IReadOnlyList<int> ids, DateOnly dataDate, CancellationToken cancellationToken);
}
