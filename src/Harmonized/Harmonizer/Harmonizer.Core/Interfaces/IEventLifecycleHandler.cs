namespace Harmonizer.Core.Interfaces;

/// <summary>A lifecycle source registers its own durable polling use case without changing the host loop.</summary>
public interface IEventLifecycleHandler
{
    string SourceType { get; }
    Task ProcessDueAsync(CancellationToken cancellationToken);
}
