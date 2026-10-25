using Microsoft.Extensions.Options;
using AlarmIngestor.Configuration;
using AlarmIngestor.Enums;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Monitoring;
using AlarmIngestor.Operations.Discovery;
using AlarmIngestor.Operations.Execution;
using AlarmIngestor.Operations.Polling;
using AlarmIngestor.Serialization;
using AlarmIngestor.Validators;
using AlarmIngestor.Worker.Configuration;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IValidateOptions<IngestionOptions>, IngestionOptionsValidator>();
builder.Services.AddOptions<IngestionOptions>()
    .Bind(builder.Configuration.GetSection(IngestionOptions.SectionName))
    .ValidateOnStart();
builder.Services.AddAlarmSource(builder.Configuration);
builder.Services.AddAlarmMessaging();

builder.Services.AddSingleton<AlarmBatchWriter>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new AlarmBatchWriter(services.GetRequiredService<IRawObjectStore>(), options.MaxEventsPerShard);
});

builder.Services.AddSingleton<IIngestionTelemetry, NullIngestionTelemetry>();

builder.Services.AddSingleton<AlarmDiscoveryReader>(services =>
    new AlarmDiscoveryReader(services.GetRequiredService<IAlarmSourceAdapter>()));

builder.Services.AddSingleton<KnownOpenAlarmPoller>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new KnownOpenAlarmPoller(services.GetRequiredService<IAlarmSourceAdapter>(), options.PollBatchSize);
});

builder.Services.AddSingleton<AlarmIngestionRunner>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new AlarmIngestionRunner(
        services.GetRequiredService<IExtractionCursorRepository>(),
        services.GetRequiredService<IAlarmSourceAdapter>(),
        services.GetRequiredService<AlarmDiscoveryReader>(),
        services.GetRequiredService<KnownOpenAlarmPoller>(),
        services.GetRequiredService<AlarmBatchWriter>(),
        services.GetRequiredService<IRawArtifactPublisher>(),
        services.GetRequiredService<IIngestionTelemetry>(),
        options,
        $"{Environment.MachineName}-{Guid.NewGuid():N}");
});

using var host = builder.Build();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("AlarmIngestor.Worker");
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
try
{
    var request = IngestionRequestParser.Parse(
        builder.Configuration["Ingestion:WindfarmId"],
        builder.Configuration["REFRESH_WINDFARM_ID"],
        builder.Configuration["REFRESH_RUN_ID"],
        builder.Configuration["REFRESH_MODE"],
        builder.Configuration["REFRESH_HISTORICAL_START_UTC"],
        builder.Configuration["REFRESH_HISTORICAL_END_UTC"],
        builder.Configuration["OPERATION_ID"]);
    await host.StartAsync();
    var runner = host.Services.GetRequiredService<AlarmIngestionRunner>();
    var result = await runner.RunAsync(request, lifetime.ApplicationStopping);
    logger.LogInformation("Run {RunId} for {WindfarmId} finished with {Outcome}; wrote {EventCount} events.",
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
    logger.LogError(error, "Ingestion failed ({ErrorType}).", error.GetType().Name);
    return 1;
}
finally
{
    using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await host.StopAsync(shutdown.Token);
}
