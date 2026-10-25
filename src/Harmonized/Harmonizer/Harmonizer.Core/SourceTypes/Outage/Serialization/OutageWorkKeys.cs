using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Serialization;

public static class OutageWorkKeys
{
    public static string Event(string eventId) => "event:" + eventId;
    public static string Scope(OutageWork work) => "snapshot:" + work.Message.ArtifactId + ":" + work.Message.BatchId;
}
