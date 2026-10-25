using Harmonizer.Core.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Interfaces;

public interface IOutageIdentityCatalog
{
    Task<IReadOnlyList<TurbineIdentity>> ReadAsync(WindfarmIdentity windfarm, CancellationToken cancellationToken);
}
