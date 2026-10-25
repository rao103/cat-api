using System.Threading;
using System.Threading.Tasks;

namespace CAT.RawFanOut.Interfaces;

public interface IMessageProcessor
{
    Task ProcessAsync(string body, CancellationToken cancellationToken);
}