using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Interfaces;

public interface IInputQueue
{
    Task<QueueDelivery?> ReceiveAsync(CancellationToken cancellationToken);
    Task RenewAsync(QueueDelivery delivery, CancellationToken cancellationToken);
    Task DeleteAsync(QueueDelivery delivery, CancellationToken cancellationToken);
}