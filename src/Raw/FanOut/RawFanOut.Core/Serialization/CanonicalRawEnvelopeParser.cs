using System;
using System.IO;
using CAT.RawFanOut.Models;
using System.Globalization;
using System.Text.Json;
using CAT.Messaging.Contracts;

namespace CAT.RawFanOut.Serialization;

internal static class CanonicalRawEnvelopeParser
{
    public static RawEnvelope Parse(string body)
    {
        var envelope = JsonSerializer.Deserialize<RawArtifactAvailableV2>(body)
            ?? throw new InvalidDataException("Missing canonical envelope.");
        envelope.Validate(allowLegacyPayloadSchema: true);
        var refresh = envelope.ExtractionMode == "Refresh";
        var path = RawEnvelope.ValidatePath((refresh ? envelope.ManifestPath : envelope.BlobPath)!);
        var production = envelope.SourceType == "Production";
        return new RawEnvelope
        {
            CanonicalEnvelope = envelope,
            SourceType = envelope.SourceType, WindfarmId = envelope.WindfarmId,
            WindfarmName = envelope.WindfarmName, ExtractionToken = envelope.ExtractionToken,
            RunId = envelope.RunId, RequestId = envelope.RunId, AttemptId = envelope.AttemptId,
            TraceId = envelope.TraceId, OperationId = envelope.OperationId, OriginalBody = body,
            ArtifactId = RawArtifactIdentityV1.Compute(envelope.SourceType, envelope.WindfarmId.ToString(CultureInfo.InvariantCulture), null,
                envelope.ExtractionToken, envelope.RunId, [path]),
            Paths = refresh ? [] : [path], ManifestPath = refresh ? path : null,
            IsFullSnapshot = refresh, IsExtractionReady = production && refresh,
            IsIncrementalChanges = production && !refresh,
            ExtractionMode = envelope.ExtractionMode, PayloadKind = envelope.PayloadKind,
            PayloadSchemaVersion = envelope.PayloadSchemaVersion, QueryAxis = envelope.QueryAxis,
            ExpectedTotalRowCount = envelope.RowCount,
            EventSnapshotScope = refresh && !production ? new EventSnapshotScopeV3
            {
                Kind = RawArtifactConventions.EventsOverlappingInterval,
                StartInclusiveUtc = envelope.QueryStartInclusiveUtc,
                EndExclusiveUtc = envelope.QueryEndExclusiveUtc
            } : null
        };
    }
}
