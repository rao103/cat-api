using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
namespace CAT.Messaging.Contracts;

// Coordinator identity helper; ingestors need not call it. Paths must use the
// consumer's normalized container-relative form: no leading slash, no duplicates.
public static class RawArtifactIdentityV1
{
    public static string Compute(string sourceType,string windfarmId,string? dataDate,long extractionToken,
        string runId,IEnumerable<string> normalizedPaths)
    {
        var identity=JsonSerializer.Serialize(new {SourceType=sourceType,WindfarmId=windfarmId,DataDate=dataDate,
            ExtractionToken=extractionToken,RunId=runId,Paths=normalizedPaths.Order(StringComparer.Ordinal).ToArray()});
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant();
    }
}
