using Harmonizer.Core.Models;
namespace Harmonizer.Core.Interfaces;

public interface IWorkQueue
{
    Task<IReadOnlyList<QueueDelivery>> ReceiveAsync(int maximumMessages, CancellationToken cancellationToken);
    Task RenewVisibilityAsync(QueueDelivery delivery, CancellationToken cancellationToken);
    Task DeleteAsync(QueueDelivery delivery, CancellationToken cancellationToken);
}
