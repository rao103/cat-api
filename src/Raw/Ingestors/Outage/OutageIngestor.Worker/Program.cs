using Microsoft.Extensions.Options;
using OutageIngestor.Adapters.Storage;
using OutageIngestor.Configuration;
using OutageIngestor.Interfaces;
using OutageIngestor.Monitoring;
using OutageIngestor.Operations.Discovery;
using OutageIngestor.Operations.Execution;
using OutageIngestor.Resolvers;
using OutageIngestor.Serialization;
using OutageIngestor.Validators;
using OutageIngestor.Worker.Configuration;
using OutageIngestor.Worker.Operations;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddSimpleConsole(options =>
{
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff 'UTC' ";
    options.UseUtcTimestamp = true;
    options.SingleLine = true;
});

// Explicit opt-in: development launch stays alive to receive requests from the local API.
var localRefresh = builder.Environment.IsDevelopment()
    && builder.Configuration.GetValue<bool>("LocalRefresh:Enabled");

builder.Services.AddSingleton<IValidateOptions<IngestionOptions>, IngestionOptionsValidator>();
builder.Services.AddOptions<IngestionOptions>()
    .Bind(builder.Configuration.GetSection(IngestionOptions.SectionName))
    .ValidateOnStart();
builder.Services.AddOutageSource(builder.Configuration);
builder.Services.AddOutageMessaging();
builder.AddOutageTelemetry();

builder.Services.AddSingleton<IRawObjectStore>(services =>
{
    var source = services.GetRequiredService<IOptions<SourceOptions>>().Value;
    var settings = BlobConnectionSettingsResolver.Resolve(source);
    return settings.UsesConnectionString
        ? new AzureBlobRawObjectStore(settings.ConnectionString!, settings.ContainerName)
        : new AzureBlobRawObjectStore(settings.ServiceUri!, new Azure.Identity.DefaultAzureCredential(), settings.ContainerName);
});

builder.Services.AddSingleton<OutageBatchWriter>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new OutageBatchWriter(services.GetRequiredService<IRawObjectStore>(), options.MaxEventsPerShard);
});

builder.Services.AddSingleton<CatchUpWindowPlanner>();
builder.Services.AddSingleton(typeof(IIngestionTelemetry<>), typeof(IngestionTelemetry<>));

builder.Services.AddSingleton<ChangeDiscoveryReader>(services =>
    new ChangeDiscoveryReader(services.GetRequiredService<IOutageSourceAdapter>()));

builder.Services.AddScoped<OutageIngestionRunner>(services =>
{
    var options = services.GetRequiredService<IOptions<IngestionOptions>>().Value;
    return new OutageIngestionRunner(
        services.GetRequiredService<IExtractionCursorRepository>(),
        services.GetRequiredService<IOutageSourceAdapter>(),
        services.GetRequiredService<OutageBatchWriter>(),
        services.GetRequiredService<IRawArtifactPublisher>(),
        services.GetRequiredService<CatchUpWindowPlanner>(),
        services.GetRequiredService<ChangeDiscoveryReader>(),
        services.GetRequiredService<IIngestionTelemetry<OutageIngestionRunner>>(),
        options, $"{Environment.MachineName}-{Guid.NewGuid():N}",
        services.GetRequiredService<IOutageScopeRepository>(),
        services.GetRequiredService<IExtractionSchedulingRepository>());
});

builder.Services.AddSingleton<IOutageAssignmentExecutor, ScopedOutageAssignmentExecutor>();
builder.Services.AddSingleton<ScheduledIngestionWorker>();
builder.Services.AddSingleton<IngestionExecutionService>();
if (localRefresh)
    builder.Services.AddHostedService<LocalRefreshListener>();

using var host = builder.Build();
var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("OutageIngestor.Worker");
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
logger.LogInformation("OutageIngestor.Worker starting.");
try
{
    if (localRefresh)
    {
        await host.RunAsync();
        return 0;
    }

    await host.StartAsync();
    var execution = host.Services.GetRequiredService<IngestionExecutionService>();
    var exitCode = (await execution.RunAsync(lifetime.ApplicationStopping)).ExitCode;
    logger.LogInformation("OutageIngestor.Worker run finished with exit code {ExitCode}.", exitCode);
    return exitCode;
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
    logger.LogInformation("OutageIngestor.Worker closing.");
}
