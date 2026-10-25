using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Interfaces;

public interface IFarmCatalog
{
    Task<FarmCatalog> LoadAsync(FarmCatalogRequest request, CancellationToken cancellationToken);
}
