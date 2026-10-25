using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CAT.RawFanOut.Interfaces;

public interface IRawObjectStore
{
    Task<Stream> OpenSeekableAsync(string path, CancellationToken cancellationToken);
    IAsyncEnumerable<string> ListPathsAsync(string prefix, CancellationToken cancellationToken);
}