using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Models;
namespace CAT.RawFanOut.Interfaces;

public interface IFanOutPlanner
{
    Task<IReadOnlyList<OutgoingMessage>> PlanAsync(RawEnvelope artifact, CancellationToken cancellationToken);
}
