using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Models;
using ProductionIngestor.Contracts;
using CAT.Messaging.Contracts;
using System;
using System.IO;
using System.Text.Json;
using OutageIngestor.Contracts;
namespace CAT.RawFanOut.UnitTests;
public sealed class EnvelopeTests
{
    private readonly FanOutOptions options = new();
    [Fact]
    public void ProductionArtifactIntegerFarmIdIsNormalizedToString()
    {
        var message = new ProductionIngestor.Contracts.RawArtifactAvailableV1
        {
            SourceType = "Production",
            WindfarmId = 7,
            WindfarmName = "WF01",
            DataDate = "2026-06-03",
            ExtractionToken = 41,
            BlobPath = "raw/test.parquet",
            RunId = "run-1"
        };

        var value = RawEnvelope.Parse(JsonSerializer.Serialize(message), options);

        Assert.Equal(7, value.WindfarmId);
    }
    [Fact] public void ProductionPascalCaseContractIsAccepted()
    {
        var value = RawEnvelope.Parse(Fixtures.Body(), options);
        Assert.Equal(new DateOnly(2026,6,3), value.DataDate);
        Assert.Equal("run-1", value.RunId);
        Assert.Contains("WindowSequence", value.OriginalBody);
    }
    [Theory] [InlineData("Alarm")] [InlineData("Curtailment")]
    public void EventsDoNotRequireBusinessDate(string source) =>
        Assert.Null(RawEnvelope.Parse(Fixtures.Body(source, null), options).DataDate);
    [Fact]
    public void OutageProducerContractDoesNotRequireBusinessDate() =>
        Assert.Null(RawEnvelope.Parse(Fixtures.OutageEventsBody(), options).DataDate);
    [Fact]
    public void OutageProducerSnapshotContractNormalizesManifestAndLegacyWindowBounds()
    {
        var body = JsonSerializer.Serialize(new RawOutageBatchAvailableV1
        {
            SourceType = "Outage",
            WindfarmId = "7",
            ExtractionToken = 42,
            PayloadKind = "OutageSnapshot",
            ManifestPath = "raw/outage/windfarm=WF01/mode=refresh/extraction=42/snapshot/manifest.json",
            RunId = "snapshot-run",
            IsFullSnapshot = true,
            WindowStartUtc = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
            WindowEndUtc = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc)
        });

        var value = RawEnvelope.Parse(body, options);

        Assert.Equal("raw/outage/windfarm=WF01/mode=refresh/extraction=42/snapshot/manifest.json", value.ManifestPath);
        Assert.Equal(new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero), value.EventSnapshotScope!.StartInclusiveUtc);
        Assert.Equal(new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero), value.EventSnapshotScope.EndExclusiveUtc);
        Assert.True(value.IsFullSnapshot);
    }
    [Fact] public void ProductionRequiresDate() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body(date:null), options));
    [Fact] public void EventExtractionDateCannotBecomeBusinessDate() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.OutageEventsBody("2026-06-03"), options));
    [Fact] public void BothPathFormsAreRejected() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body().Replace("\"ShardPaths\":null", "\"ShardPaths\":[\"raw/b.parquet\"]"), options));
    [Fact] public void NeitherPathFormIsRejected() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body().Replace("\"raw/test.parquet\"", "null"), options));
    [Fact] public void EmptyShardArrayIsRejected() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body(shards:[]), options));
    [Fact] public void DuplicateNormalizedShardIsRejected() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body(shards:["/raw/a.parquet","raw/a.parquet"]), options));
    [Fact] public void ShardOrderDoesNotChangeArtifactIdentity()
    {
        var a = RawEnvelope.Parse(Fixtures.Body(shards:["raw/a.parquet","raw/b.parquet"]), options);
        var b = RawEnvelope.Parse(Fixtures.Body(shards:["raw/b.parquet","raw/a.parquet"]), options);
        Assert.Equal(a.ArtifactId,b.ArtifactId);
    }
    [Theory] [InlineData("https://example/a?token=secret")] [InlineData("raw/../a")] [InlineData("raw//a")]
    public void UnsafeObjectReferencesAreRejected(string path) => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body(shards:[path]), options));
    [Fact] public void DuplicateJsonPropertiesAreRejected() => Assert.Throws<InvalidDataException>(() =>
        RawEnvelope.Parse(Fixtures.Body().Replace("{", "{\"SourceType\":\"Alarm\","), options));
    [Fact]
    public void ExtractionReadyAcceptsBoundedSealedPrefixInsteadOfExplicitParts()
    {
        var ready = JsonSerializer.Deserialize<ProductionExtractionReadyV1>(Fixtures.ExtractionReadyBody())! with
        {
            Artifacts = [],
            ArtifactPrefix = "raw/production/windfarm=WF01/mode=refresh/extraction=41/",
            ExpectedArtifactCount = 2,
            ArtifactSelectionRule = ProductionArtifactSelectionRules.DirectParquetFilesV1,
        };

        var value = RawEnvelope.Parse(JsonSerializer.Serialize(ready), options);

        Assert.True(value.IsExtractionReady);
        Assert.Empty(value.Paths);
        Assert.Equal(ready.ArtifactPrefix, value.ArtifactPrefix);
        Assert.Equal(2, value.ExpectedArtifactCount);
    }
    [Fact]
    public void ExtractionReadyRejectsSealedPrefixAboveConfiguredFileLimit()
    {
        var ready = JsonSerializer.Deserialize<ProductionExtractionReadyV1>(Fixtures.ExtractionReadyBody())! with
        {
            Artifacts = [],
            ArtifactPrefix = "raw/production/windfarm=WF01/mode=refresh/extraction=41/",
            ExpectedArtifactCount = 2,
            ArtifactSelectionRule = ProductionArtifactSelectionRules.DirectParquetFilesV1,
        };

        Assert.Throws<InvalidDataException>(() => RawEnvelope.Parse(JsonSerializer.Serialize(ready),
            new FanOutOptions { MaxFilesPerExtraction = 1 }));
    }

        [Fact]
        public void ExtractionReadyV2AcceptsNumericFarmIdAndWtgNameScope()
        {
                const string body = """
                        {
                            "EnvelopeVersion": 2,
                            "MessageKind": "ExtractionReady",
                            "SourceType": "Production",
                            "WindfarmId": 2,
                            "WindfarmName": "WOW02",
                            "ExtractionMode": "Refresh",
                            "PayloadKind": "ProductionSnapshot",
                            "PayloadSchemaVersion": 2,
                            "QueryAxis": "ProductionTimestamp",
                            "RequestId": "22d8364f49d547c98cd3372b0574f697",
                            "AttemptId": "188",
                            "ExtractionToken": 49,
                            "RequestedScope": [
                                { "StartDate": "2025-01-28", "EndDateInclusive": "2025-01-31" }
                            ],
                            "CompletedWindows": [
                                {
                                    "WindowNumber": 1,
                                    "QueryStartInclusiveUtc": "2025-01-28T00:00:00Z",
                                    "QueryEndExclusiveUtc": "2025-02-01T00:00:00Z",
                                    "SelectedScope": [
                                        { "StartDate": "2025-01-28", "EndDateInclusive": "2025-01-31" }
                                    ],
                                    "RowCount": 29376,
                                    "ArtifactCount": 1,
                                    "Completed": true
                                }
                            ],
                            "Artifacts": [
                                {
                                    "BlobPath": "raw/production/windfarm=WOW02/mode=refresh/extraction=49/window-20250128T000000Z-to-20250201T000000Z-part-0001.parquet",
                                    "QueryWindowNumber": 1,
                                    "PartNumber": 1,
                                    "RowCount": 29376
                                }
                            ],
                            "TotalRowCount": 29376,
                            "IsEmptyResult": false,
                            "ScopeWtgIds": [ "WOW02A12", "WOW02A13" ],
                            "SourceVersionEvidence": null,
                            "TraceId": null,
                            "ArtifactPrefix": null,
                            "ExpectedArtifactCount": null,
                            "ArtifactSelectionRule": null
                        }
                        """;

                var value = RawEnvelope.Parse(body, options);

                Assert.Equal(2, value.WindfarmId);
                Assert.Equal(new[] { "WOW02A12", "WOW02A13" }, value.ScopeWtgIds);
        }
}
