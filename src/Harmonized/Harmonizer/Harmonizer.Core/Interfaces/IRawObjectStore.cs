namespace Harmonizer.Core.Interfaces;

/// <summary>Reads immutable source artifacts from Raw storage.</summary>
public interface IRawObjectStore
{
    Task<Stream> OpenReadAsync(string path, CancellationToken cancellationToken);
}
