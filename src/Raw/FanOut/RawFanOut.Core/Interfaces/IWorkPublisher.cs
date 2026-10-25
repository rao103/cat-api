using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Models;
namespace CAT.RawFanOut.Interfaces;

public interface IWorkPublisher
{
    Task PublishAsync(IReadOnlyList<OutgoingMessage> messages, CancellationToken cancellationToken);
}
