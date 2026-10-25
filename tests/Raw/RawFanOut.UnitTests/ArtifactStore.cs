using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.UnitTests;
internal sealed class ArtifactStore : IArtifactDispatchStore
{
    public Task<IReadOnlyList<OutgoingMessage>?> LoadPlanAsync(RawEnvelope artifact, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<OutgoingMessage>?>(null);
    public int Saves {get;private set;}
    public int Dispatched {get;private set;}
    public bool Fail {get;init;}
    public bool FailMark {get;init;}
    public Task<IReadOnlyList<OutgoingMessage>> SavePlanAsync(RawEnvelope a,IReadOnlyList<OutgoingMessage> p,CancellationToken ct)
    { Saves++; if(Fail) throw new IOException("Database unavailable"); return Task.FromResult(p); }
    public Task MarkDispatchedAsync(string id,CancellationToken ct)
    { ct.ThrowIfCancellationRequested(); if(FailMark) throw new IOException("Commit failed"); Dispatched++; return Task.CompletedTask; }
}
