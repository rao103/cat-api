namespace Harmonizer.Core.Interfaces;

/// <summary>Reads committed state and creates immutable candidates in Harmonized storage.</summary>
public interface IHarmonizedArtifactStore
{
    Task<Stream> OpenReadAsync(string path, CancellationToken cancellationToken);
    Task WriteNewAsync(string path, Stream content, string contentType, CancellationToken cancellationToken);
}
