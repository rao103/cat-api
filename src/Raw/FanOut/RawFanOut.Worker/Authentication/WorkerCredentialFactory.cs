using Azure.Core;
using Azure.Identity;

namespace CAT.RawFanOut.Worker.Authentication;

internal static class WorkerCredentialFactory
{
    public static TokenCredential CreateBlobCredential() => new DefaultAzureCredential();
}