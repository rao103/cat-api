using Harmonizer.Core.SourceTypes.Production.Repositories;
using Harmonizer.Core.SourceTypes.Production.Adapters;
using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.SourceTypes.Production.Operations;
using Harmonizer.Core.SourceTypes.Production.Serialization;
using Harmonizer.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace Harmonizer.Worker.SourceTypes.Production;

/// <summary>Registers the currently supported Production handler and its artifacts.</summary>
public static class ProductionRegistration
{
    public static IServiceCollection AddProductionHarmonization(this IServiceCollection services)
    {
        services.AddSingleton<PostgresProductionControlStateRepository>();
        services.AddSingleton<IProductionControlStateRepository>(provider =>
            provider.GetRequiredService<PostgresProductionControlStateRepository>());
        services.AddSingleton<IProductionArtifactStore, ParquetProductionArtifactStore>();
        services.AddSingleton<IIdentityCatalog, PostgresIdentityCatalog>();
        services.AddSingleton<ProductionWorkV2Reader>();
        services.AddSingleton<ProductionWorkMessageReader>();
        services.AddSingleton<ProductionMergeService>();
        services.AddSingleton<ProductionNotificationFactory>();
        services.AddSingleton<ProductionBatchProcessor>();
        services.AddSingleton<IHarmonizationHandler, ProductionHarmonizationHandler>();
        return services;
    }
}
