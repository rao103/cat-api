using Microsoft.Extensions.Options;
using ProductionIngestor.Adapters.Storage;
using ProductionIngestor.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Operations.Discovery;
using ProductionIngestor.Operations.Execution;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Monitoring;
using ProductionIngestor.Operations.Execution.Changes;
using ProductionIngestor.Resolvers;
using ProductionIngestor.Serialization;
using ProductionIngestor.Validators;
using ProductionIngestor.Worker;
using ProductionIngestor.Worker.Configuration;
using ProductionIngestor.Worker.Operations;

var builder = Host.CreateApplicationBuilder(args);

// Explicit opt-in: development launch stays alive to receive requests from the local API.
var localRefresh = builder.Environment.IsDevelopment()
    && builder.Configuration.GetValue<bool>("LocalRefresh:Enabled");

builder.Services.AddSingleton<IValidateOptions<IngestionOptions>, IngestionOptionsValidator>();
builder.Services.AddOptions<IngestionOptions>()
    .Bind(builder.Configuration.GetSection(IngestionOptions.SectionName))
    .ValidateOnStart();
builder.Services.AddProductionSource(builder.Configuration);
builder.Services.AddProductionMessaging(builder.Configuration);
builder.AddProductionTelemetry();

builder.Services.AddSingleton<IRawObjectStore>(services =>
{
    var source = services.GetRequiredService<IOptions<SourceOptions>>().Value;
    var settings = BlobConnectionSettingsResolver.Resolve(source);
    return settings.UsesConnectionString
        ? new AzureBlobRawObjectStore(settings.ConnectionString!, settings.ContainerName)
        : new AzureBlobRawObjectStore(settings.ServiceUri!, new Azure.Identity.DefaultAzureCredential(), settings.ContainerName);
});

builder.Services.AddSingleton<ProductionParquetWriter>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new ProductionParquetWriter(services.GetRequiredService<IRawObjectStore>(), options.MaxRowsPerShard,
        options.ParquetRowGroupSize, options.MaxArtifactShardSizeBytes);
});

builder.Services.AddSingleton(typeof(IIngestionTelemetry<>), typeof(IngestionTelemetry<>));

builder.Services.AddSingleton<ChangeDiscoveryReader>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new ChangeDiscoveryReader(services.GetRequiredService<IProductionSourceAdapter>(), options.MaxAffectedDatesPerWindow);
});

builder.Services.AddSingleton<WindfarmDayExtractor>(services => new WindfarmDayExtractor(
    services.GetRequiredService<IProductionSourceAdapter>(),
    services.GetRequiredService<IOptions<IngestionOptions>>().Value.MaxSnapshotRows));

builder.Services.AddSingleton<ProductionChangesPartWriter>(services =>
    new ProductionChangesPartWriter(services.GetRequiredService<IRawObjectStore>()));

builder.Services.AddSingleton<ProductionChangeWindowProcessor>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new ProductionChangeWindowProcessor(
        services.GetRequiredService<IProductionSourceAdapter>(),
        services.GetRequiredService<ProductionChangesPartWriter>(),
        services.GetRequiredService<IRawArtifactPublisher>(),
        services.GetRequiredService<IIngestionTelemetry<ProductionChangeWindowProcessor>>(),
        options.MaxRowsPerShard);
});

builder.Services.AddSingleton<ProductionChangesIncrementalStrategy>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new ProductionChangesIncrementalStrategy(
        services.GetRequiredService<IProductionSourceAdapter>(),
        services.GetRequiredService<IProductionScopeRepository>(),
        services.GetRequiredService<ProductionChangeWindowProcessor>(),
        services.GetRequiredService<IIngestionTelemetry<ProductionChangesIncrementalStrategy>>(),
        options, "ProductionIngestor");
});

builder.Services.AddScoped<ProductionIngestionRunner>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new ProductionIngestionRunner(
        services.GetRequiredService<IExtractionCursorRepository>(),
        services.GetRequiredService<IProductionSourceAdapter>(),
        services.GetRequiredService<WindfarmDayExtractor>(),
        services.GetRequiredService<ProductionParquetWriter>(),
        services.GetRequiredService<IRawArtifactPublisher>(),
        services.GetRequiredService<ChangeDiscoveryReader>(),
        services.GetRequiredService<IIngestionTelemetry<ProductionIngestionRunner>>(),
        options, $"{Environment.MachineName}-{Guid.NewGuid():N}",
        services.GetRequiredService<IExtractionSchedulingRepository>(),
        services.GetRequiredService<ProductionChangesIncrementalStrategy>(),
        services.GetRequiredService<IProductionScopeRepository>());
});

builder.Services.AddSingleton<IProductionAssignmentExecutor, ScopedProductionAssignmentExecutor>();
builder.Services.AddSingleton<ScheduledIngestionWorker>();
builder.Services.AddSingleton<IngestionExecutionService>();
if (localRefresh)
    builder.Services.AddHostedService<LocalRefreshListener>();

using var host = builder.Build();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("ProductionIngestor.Worker");
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
try
{
    if (localRefresh)
    {
        await host.RunAsync();
        return 0;
    }

    await host.StartAsync();
    var execution = host.Services.GetRequiredService<IngestionExecutionService>();
    return (await execution.RunAsync(lifetime.ApplicationStopping)).ExitCode;
}
catch (OperationCanceledException) when (lifetime.ApplicationStopping.IsCancellationRequested)
{
    logger.LogInformation("Ingestion cancelled during shutdown.");
    return 130;
}
catch (Exception error) when (error is ArgumentException or OptionsValidationException)
{
    logger.LogError(error, "Invalid ingestion configuration or request ({ErrorType}).", error.GetType().Name);
    return 2;
}
catch (Exception error)
{
    logger.LogError(error, "Ingestion failed with {ErrorType}.", error.GetType().Name);
    return 1;
}
finally
{
    using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    try
    {
        await host.StopAsync(shutdown.Token).WaitAsync(shutdown.Token);
    }
    catch (Exception error)
    {
        logger.LogWarning("Host shutdown failed or exceeded the 10-second limit ({ErrorType}); preserving the ingestion outcome.",
            error.GetType().Name);
    }
}
