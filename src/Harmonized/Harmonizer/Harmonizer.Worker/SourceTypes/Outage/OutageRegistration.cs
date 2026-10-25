using Harmonizer.Core.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Adapters;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Operations;
using Harmonizer.Core.SourceTypes.Outage.Repositories;
using Harmonizer.Core.SourceTypes.Outage.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Harmonizer.Worker.SourceTypes.Outage;

public static class OutageRegistration
{
    public static IServiceCollection AddOutageHarmonization(this IServiceCollection services)
    {
        services.AddSingleton<IOutageIdentityCatalog, PostgresOutageIdentityCatalog>();
        services.AddSingleton<IOutageControlRepository, PostgresOutageControlRepository>();
        services.AddSingleton<IOutageArtifacts, ParquetOutageArtifacts>();
        services.AddSingleton<NormalizedOutageRowMapper>();
        services.AddSingleton<OutageMergeService>();
        services.AddSingleton<OutageRangePlanner>();
        services.AddSingleton<OutageDayMaterializer>();
        services.AddSingleton<OutageScopeProjector>();
        services.AddSingleton<OutageCanonicalReader>();
        services.AddSingleton<OutageWorkReader>();
        services.AddSingleton<OutageLifecycleProcessor>();
        services.AddSingleton<IEventLifecycleHandler, OutageLifecycleHandler>();
        services.AddSingleton<OutageBatchProcessor>();
        services.AddSingleton<IHarmonizationHandler, OutageHarmonizationHandler>();
        return services;
    }
}
