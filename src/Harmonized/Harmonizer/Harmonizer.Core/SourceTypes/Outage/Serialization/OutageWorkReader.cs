using System.Security.Cryptography;
using System.Text.Json;
using CAT.Messaging.Contracts;
using TurbineIdentity = Harmonizer.Core.Models.TurbineIdentity;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Outage.Serialization;

public sealed class OutageWorkReader(IOutageIdentityCatalog identities, IOptions<HarmonizerOptions> options,
    ITelemetry<OutageWorkReader> telemetry)
{
    public async Task<OutageWork> ReadAsync(string body, CancellationToken cancellationToken)
    {
        using var document = JsonDocument.Parse(body, new() { MaxDepth = 32 });
        RejectDuplicateProperties(document.RootElement);
        var message = document.RootElement.Deserialize<EventWorkV3>(HarmonizerJson.Options)
            ?? throw new WorkValidationException("Missing EventWorkV3 envelope.");
        if (message.Members is null || message.EventIds is null || message.RawPaths is null ||
            message.ExpectedEventBatchIds is null || message.OriginalMessageBody is null ||
            message.Members.Any(member => member is null || member.RawPaths is null) ||
            message.SnapshotScope is { WtgIds: null })
            throw new WorkValidationException("Outage work is missing required collections or its original envelope.");
        if (message.PayloadSchemaVersion is not (null or 1 or 2))
            throw new UnsupportedWorkException("Unsupported Outage Raw payload schema.");
        if (message.SchemaVersion != 3 || message.SourceType != "Outage" || message.WindfarmId <= 0 ||
            string.IsNullOrWhiteSpace(message.WindfarmName) || message.ExtractionToken < 0 ||
            string.IsNullOrWhiteSpace(message.RunId) || string.IsNullOrWhiteSpace(message.ArtifactId) ||
            !IsHash(message.ArtifactId) || !IsHash(message.BatchId) || message.Members.Length > options.Value.MaximumRowsPerArtifact)
            throw new WorkValidationException("Invalid Outage V3 version, identity, or work bound.");
        ValidatePaths(message.RawPaths, allowEmpty: message.IsFullSnapshot);
        var reconciliation = message.WorkKind == "EventSnapshotReconciliation";
        var observation = message.WorkKind == "ArtifactObserved";
        if (message.WorkKind is not ("CanonicalEvents" or "EventSnapshotReconciliation" or "ArtifactObserved") ||
            message.PayloadKind is not ("OutageEvents" or "OutageDeletedEvents" or "OutageSnapshot"))
            throw new UnsupportedWorkException("Unsupported Outage work or payload kind.");
        if (message.ExtractionMode is not (null or "Incremental" or "Refresh") ||
            message.IsFullSnapshot != (message.PayloadKind == "OutageSnapshot") ||
            message.IsFullSnapshot != (message.ExtractionMode == "Refresh") ||
            !message.IsFullSnapshot && message.SnapshotScope is not null)
            throw new WorkValidationException("Outage snapshot authority disagrees with mode/payload/scope.");
        if (message.SnapshotObservedAtUtc.HasValue && message.SnapshotObservedAtUtc.Value.Offset != TimeSpan.Zero ||
            message.SourceVersionCutoffUtc.HasValue && message.SourceVersionCutoffUtc.Value.Offset != TimeSpan.Zero ||
            reconciliation && message.ReconciliationMode is not (EventReconciliationModes.ReceiptOnly or EventReconciliationModes.Authoritative) ||
            reconciliation && message.ReconciliationMode == EventReconciliationModes.Authoritative && message.SourceVersionCutoffUtc is null ||
            reconciliation && message.ReconciliationMode == EventReconciliationModes.ReceiptOnly && message.SourceVersionCutoffUtc is not null ||
            !reconciliation && (message.ReconciliationMode is not null || message.SourceVersionCutoffUtc is not null))
            throw new WorkValidationException("Snapshot reconciliation mode, observation time, or source-version cutoff is invalid.");
        if ((reconciliation && (!message.IsFullSnapshot || message.Members.Length != 0 || message.EventIds.Length != 0)) ||
            (!reconciliation && !observation && (message.Members.Length == 0 || message.ExpectedEventBatchIds.Length != 0)) ||
            (observation && (message.IsFullSnapshot || message.Members.Length != 0 || message.EventIds.Length != 0 || message.ExpectedEventBatchIds.Length != 0)))
            throw new WorkValidationException("Canonical members and snapshot receipt barriers must remain separate.");
        if (message.ExpectedEventBatchIds.Any(value => !IsHash(value)) ||
            message.ExpectedEventBatchIds.Distinct(StringComparer.Ordinal).Count() != message.ExpectedEventBatchIds.Length ||
            message.ExpectedEventBatchIds.Contains(message.BatchId, StringComparer.Ordinal))
            throw new WorkValidationException("Invalid expected snapshot batch IDs.");
        var farm = new WindfarmIdentity(message.WindfarmId, message.WindfarmName);
        var turbines = await identities.ReadAsync(farm, cancellationToken);
        var byId = turbines.ToDictionary(turbine => turbine.WtgId);
        if (message.IsFullSnapshot)
        {
            var scope = message.SnapshotScope ?? throw new WorkValidationException("Snapshot requires an explicit scope.");
            if (scope.Kind != "EventsOverlappingInterval" || scope.StartInclusiveUtc.Offset != TimeSpan.Zero ||
                scope.EndExclusiveUtc.Offset != TimeSpan.Zero || scope.EndExclusiveUtc <= scope.StartInclusiveUtc ||
                scope.EndExclusiveUtc.UtcDateTime.Date == DateTime.MaxValue.Date || (scope.WtgIds.Length == 0 && (!reconciliation || message.ReconciliationMode != EventReconciliationModes.ReceiptOnly || message.RawPaths.Length != 0)) ||
                scope.WtgIds.Distinct().Count() != scope.WtgIds.Length || scope.WtgIds.Any(id => !byId.ContainsKey(id)))
                throw new WorkValidationException("Snapshot must contain a valid UTC interval and the frozen numeric WTG roster.");
        }
        var memberKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var member in message.Members)
        {
            if (string.IsNullOrWhiteSpace(member.EventId) || member.EventId.Length > 1024 || member.EventId.Any(char.IsControl) ||
                member.WtgId is not { } id || !byId.TryGetValue(id, out var turbine) || turbine.WtgName != member.WtgName ||
                message.IsFullSnapshot && !message.SnapshotScope!.WtgIds.Contains(id))
                throw new WorkValidationException("Outage member requires a classification identity and matching numeric WTG ID/name.");
            var eventKey = Hash(new { message.SourceType, message.WindfarmId, member.EventId });
            var memberKey = Hash(new { EventKey = eventKey, member.WtgId });
            if (member.EventKey != eventKey || member.MemberKey != memberKey || !memberKeys.Add(memberKey))
                throw new WorkValidationException("Outage event/member key is invalid or repeated.");
            ValidatePaths(member.RawPaths, allowEmpty: false);
            if (member.RawPaths.Any(path => !message.RawPaths.Contains(path, StringComparer.Ordinal)))
                throw new WorkValidationException("Member Raw paths are outside the frozen batch paths.");
        }
        if (message.Members.GroupBy(member => member.EventId, StringComparer.Ordinal)
            .Any(group => group.Select(member => member.WtgId).Distinct().Count() != 1))
            throw new WorkValidationException("An Outage event must belong to exactly one WTG within a batch.");
        if (!message.EventIds.ToHashSet(StringComparer.Ordinal).SetEquals(message.Members.Select(member => member.EventId)) ||
            message.EventIds.Distinct(StringComparer.Ordinal).Count() != message.EventIds.Length ||
            !reconciliation && !observation && !message.RawPaths.ToHashSet(StringComparer.Ordinal).SetEquals(message.Members.SelectMany(member => member.RawPaths)))
            throw new WorkValidationException("Outage event IDs and Raw paths disagree with exact member selection.");
        var operation = OperationIdentityReader.Read(document.RootElement, message.OriginalMessageBody);
        telemetry.Information("Outage V3 work validated. WorkKind={WorkKind} BatchId={BatchId} MemberCount={MemberCount}",
            message.WorkKind, message.BatchId, message.Members.Length);
        return new(message, farm, turbines, operation, body);
    }

    private void ValidatePaths(string[] paths, bool allowEmpty)
    {
        if ((!allowEmpty && paths.Length == 0) || paths.Length > options.Value.MaximumRawPaths ||
            paths.Any(path => !ObjectPath.IsSafe(path)) || paths.Distinct(StringComparer.Ordinal).Count() != paths.Length)
            throw new WorkValidationException("Outage requires bounded distinct container-relative Raw paths.");
    }

    private static bool IsHash(string? value) => value is not null && value.Length == 64 && value.All(character => character is >= '0' and <= '9' or >= 'a' and <= 'f');

    private static string Hash<T>(T value) => Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(value)));

    private static void RejectDuplicateProperties(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in value.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new WorkValidationException("Duplicate JSON property in event work.");
                RejectDuplicateProperties(property.Value);
            }
        }
        else if (value.ValueKind == JsonValueKind.Array)
            foreach (var item in value.EnumerateArray()) RejectDuplicateProperties(item);
    }
}
