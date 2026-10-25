using System.Collections.Generic;
using CAT.Messaging.Contracts;

namespace CAT.RawFanOut.Models;

public sealed record FarmCatalog(int WindfarmId, string WindfarmName, IReadOnlyList<TurbineIdentity> Turbines);