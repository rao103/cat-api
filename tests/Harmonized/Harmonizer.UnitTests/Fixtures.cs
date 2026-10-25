using Harmonizer.Core.Observability;
using Microsoft.Extensions.Logging.Abstractions;
using Harmonizer.Core.SourceTypes.Production.Models;
using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.SourceTypes.Production.Serialization;
using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Microsoft.Extensions.Options;
namespace Harmonizer.UnitTests;

internal static class Fixtures
{
    internal static HarmonizerTelemetry<T> Telemetry<T>() => new(NullLogger<T>.Instance);

    internal static ProductionWorkMessageReader Reader(IIdentityCatalog? catalog = null) =>
        new(Options, new(catalog ?? new RejectingIdentityCatalog(), Options, Telemetry<ProductionWorkV2Reader>()), Telemetry<ProductionWorkMessageReader>());

    internal static readonly DateOnly Day = new(2026, 9, 1);
    internal static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
    internal static readonly WtgDayKey Key = new(2, 11, Day);
    internal static readonly IOptions<HarmonizerOptions> Options = Microsoft.Extensions.Options.Options.Create(new HarmonizerOptions());
    internal static ProductionRow Row(int slot = 0, int version = 1, double? actual = 10, long extraction = 1, int wtgId = 11) =>
        new(wtgId, $"WTG{wtgId}", Start.AddMinutes(slot * 10), Start.AddMinutes((slot + 1) * 10), Start.AddDays(version),
            new(actual, 20, null, 30, 4), new($"raw/part-{extraction}.parquet", extraction, "run", "attempt"));
    internal static ProductionWtgDayWorkV3 Wire() => new(3, "ProductionWtgDays", new('a', 64), new('b', 64),
        "Production", 2, "WOW02", "2026-09-01", "Incremental", "ProductionChanges", 2, "run", "attempt", 1,
        ["raw/part-1.parquet"], [new(11, "WTG11")], [], false, false, false, null, "{}", null);
    internal static string Json(ProductionWtgDayWorkV3 wire) => JsonSerializer.Serialize(wire, HarmonizerJson.Options);
    internal static ProductionWork Work() => new(new(new('a', 64), new('b', 64)), new(2, "WOW02"), Day,
        [new(11, "WTG11")], ["raw/part-1.parquet"], 1, "run", "attempt", false, false, false, null,
        "{}", Json(Wire()), null);

    private sealed class RejectingIdentityCatalog : IIdentityCatalog
    {
        public Task<IReadOnlyList<Harmonizer.Core.Models.TurbineIdentity>> ResolveAsync(WindfarmIdentity windfarm,
            IReadOnlyList<string> names, DateOnly dataDate, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("V3 must not call the V2 identity catalog.");

        public Task<IReadOnlyList<Harmonizer.Core.Models.TurbineIdentity>> ResolveIdsAsync(WindfarmIdentity windfarm,
            IReadOnlyList<int> ids, DateOnly dataDate, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("V3 must not call the V2 identity catalog.");
    }
}
