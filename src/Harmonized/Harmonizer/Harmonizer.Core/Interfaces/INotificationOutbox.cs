using Harmonizer.Core.Models;
namespace Harmonizer.Core.Interfaces;

public interface INotificationOutbox
{
    Task<IReadOnlyList<Notification>> ReadPendingAsync(int limit, CancellationToken cancellationToken);
    Task MarkSentAsync(long id, CancellationToken cancellationToken);
}
public interface INotificationPublisher
{
    Task PublishAsync(Notification notification, CancellationToken cancellationToken);
}
