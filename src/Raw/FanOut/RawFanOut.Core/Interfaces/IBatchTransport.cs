using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Interfaces;

public interface IBatchTransport
{
    Task<PublishResult> SendAsync(IReadOnlyList<OutgoingMessage> messages, CancellationToken cancellationToken);
}