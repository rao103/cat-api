using System.Collections.Generic;
using System.Threading;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Interfaces;

public interface IRawIdentityReader
{
    IAsyncEnumerable<RawIdentity> ReadAsync(RawEnvelope artifact, CancellationToken cancellationToken);
}