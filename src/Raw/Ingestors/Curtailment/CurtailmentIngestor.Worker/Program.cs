using Microsoft.Extensions.Options;
using CurtailmentIngestor.Configuration;
using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Monitoring;
using CurtailmentIngestor.Operations.Discovery;
using CurtailmentIngestor.Operations.Execution;
using CurtailmentIngestor.Serialization;
using CurtailmentIngestor.Validators;
using CurtailmentIngestor.Worker.Configuration;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IValidateOptions<IngestionOptions>, IngestionOptionsValidator>();
builder.Services.AddOptions<IngestionOptions>()
    .Bind(builder.Configuration.GetSection(IngestionOptions.SectionName))
    .ValidateOnStart();
builder.Services.AddCurtailmentSource(builder.Configuration);
builder.Services.AddCurtailmentMessaging();

builder.Services.AddSingleton<CurtailmentBatchWriter>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new CurtailmentBatchWriter(services.GetRequiredService<IRawObjectStore>(),
        options.MaxEventsPerShard, options.ParquetRowGroupSize);
});

builder.Services.AddSingleton<CatchUpWindowPlanner>();
builder.Services.AddSingleton<IIngestionTelemetry, NullIngestionTelemetry>();

builder.Services.AddSingleton<ChangeDiscoveryReader>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new ChangeDiscoveryReader(services.GetRequiredService<ICurtailmentSourceAdapter>(), options.MaxEventsPerWindow);
});

builder.Services.AddSingleton<CurtailmentIngestionRunner>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new CurtailmentIngestionRunner(
        services.GetRequiredService<IExtractionCursorRepository>(),
        services.GetRequiredService<ICurtailmentSourceAdapter>(),
        services.GetRequiredService<CurtailmentBatchWriter>(),
        services.GetRequiredService<IRawArtifactPublisher>(),
        services.GetRequiredService<CatchUpWindowPlanner>(),
        services.GetRequiredService<ChangeDiscoveryReader>(),
        services.GetRequiredService<IIngestionTelemetry>(),
        options,
        $"{Environment.MachineName}-{Guid.NewGuid():N}");
});

using var host = builder.Build();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("CurtailmentIngestor.Worker");
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
try
{
    var request = IngestionRequestParser.Parse(builder.Configuration["Ingestion:WindfarmId"],
        builder.Configuration["REFRESH_WINDFARM_ID"], builder.Configuration["REFRESH_RUN_ID"],
        builder.Configuration["OPERATION_ID"]);
    await host.StartAsync();

    // Deferred until the curtailment source table schema is confirmed; see CurtailmentSourceConfiguration.
    if (host.Services.GetService<ICurtailmentSourceAdapter>() is null)
    {
        logger.LogError("No ICurtailmentSourceAdapter is registered; wire the source adapter before running ingestion.");
        return 2;
    }

    var runner = host.Services.GetRequiredService<CurtailmentIngestionRunner>();
    var result = await runner.RunAsync(request, lifetime.ApplicationStopping);
    logger.LogInformation("Run {RunId} for {WindfarmId} finished with {Outcome}; published {EventCount} events.",
        request.RunId, request.WindfarmId, result.Outcome, result.EventsWritten);
    return result.Outcome is RunOutcome.Success or RunOutcome.LeaseHeldByOther ? 0 : 1;
}
catch (OperationCanceledException) when (lifetime.ApplicationStopping.IsCancellationRequested)
{
    logger.LogInformation("Ingestion cancelled during shutdown.");
    return 130;
}
catch (Exception error) when (error is ArgumentException or OptionsValidationException)
{
    logger.LogError("Invalid ingestion configuration or request ({ErrorType}).", error.GetType().Name);
    return 2;
}
catch (Exception error)
{
    logger.LogError("Ingestion failed ({ErrorType}).", error.GetType().Name);
    return 1;
}
finally
{
    using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await host.StopAsync(shutdown.Token);
}
