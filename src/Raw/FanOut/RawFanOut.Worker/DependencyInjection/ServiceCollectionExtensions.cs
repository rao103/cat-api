using System;
using Amazon;
using Amazon.SQS;
using Azure.Identity;
using Azure.Storage.Blobs;
using CAT.RawFanOut.Adapters.Messaging;
using CAT.RawFanOut.Adapters.Parquet;
using CAT.RawFanOut.Adapters.Storage;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Factories;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Observability;
using CAT.RawFanOut.Operations.DeadLetters;
using CAT.RawFanOut.Operations.Delivery;
using CAT.RawFanOut.Operations.FanOut;
using CAT.RawFanOut.Operations.FanOut.Strategies;
using CAT.RawFanOut.Operations.Retention;
using CAT.RawFanOut.Repositories;
using CAT.RawFanOut.Serialization;
using CAT.RawFanOut.Worker.Authentication;
using CAT.RawFanOut.Worker.Configuration;
using CAT.RawFanOut.Worker.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace CAT.RawFanOut.Worker.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static WorkerOptions AddWorkerConfiguration(this HostApplicationBuilder builder)
    {
        var options = builder.Configuration.GetSection("RawFanOut").Get<WorkerOptions>() ?? new();
        options.Validate();
        return options;
    }

    public static IServiceCollection AddControlPlaneConfiguration(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddSingleton<IValidateOptions<ControlPlaneOptions>, ControlPlaneOptionsValidator>();
        services.AddOptions<ControlPlaneOptions>()
            .Bind(configuration.GetSection(ControlPlaneOptions.SectionName))
            .ValidateOnStart();
        return services;
    }

    public static IServiceCollection AddFanOutWorkerServices(
        this IServiceCollection services,
        WorkerOptions options,
        string[] args)
    {
        return services
            .AddWorkerRuntime(options)
            .AddProviderAdapters(options)
            .AddControlPlaneServices(options)
            .AddFanOutProcessing(options)
            .AddQueueExecution(options, args);
    }

    public static IServiceCollection AddWorkerRuntime(this IServiceCollection services, WorkerOptions options)
    {
        services.Configure<HostOptions>(host => host.ShutdownTimeout = TimeSpan.FromSeconds(30));
        services.AddSingleton(typeof(ITelemetry<>), typeof(FanOutTelemetry<>));
        services.AddSingleton(options);
        services.AddSingleton(options.Processing);
        return services;
    }

    public static IServiceCollection AddProviderAdapters(this IServiceCollection services, WorkerOptions options)
    {
        services.AddSingleton<IAmazonSQS>(_ => new AmazonSQSClient(RegionEndpoint.GetBySystemName(options.Region)));
        services.AddSingleton(_ => new BlobContainerClient(new Uri(options.RawContainerUri), WorkerCredentialFactory.CreateBlobCredential()));
        services.AddSingleton<IRawObjectStore>(provider => new AzureBlobObjectStore(
            provider.GetRequiredService<BlobContainerClient>(), options.Processing, options.RawPathPrefixToStrip));
        services.AddSingleton<IRawIdentityReader, ParquetIdentityReader>();
        services.AddSingleton<IBatchTransport>(provider => new SqsBatchTransport(
            provider.GetRequiredService<IAmazonSQS>(), options.HarmonizationQueueUrl));
        return services;
    }

    public static IServiceCollection AddControlPlaneServices(this IServiceCollection services, WorkerOptions options)
    {
        services.AddSingleton<IPostgresConnectionFactory>(provider => new PostgresConnectionFactory(
            provider.GetRequiredService<IOptions<ControlPlaneOptions>>().Value));
        services.AddSingleton<IDeadLetterStore, PostgresDeadLetterStore>();
        services.AddSingleton<IArtifactDispatchStore, PostgresArtifactDispatchStore>();
        services.AddSingleton<IArtifactRetentionCleanupStore, PostgresArtifactRetentionCleanupStore>();
        services.AddSingleton<IDeadLetterRetentionStore, PostgresDeadLetterRetentionStore>();
        services.AddSingleton(options.Retention);
        services.AddSingleton<RetentionCleanupJob>();
        services.AddSingleton<IFarmCatalog, PostgresFarmCatalog>();
        return services;
    }

    public static IServiceCollection AddFanOutProcessing(this IServiceCollection services, WorkerOptions options)
    {
        services.AddSingleton<IRawFanOutStrategy>(new ProductionFanOutStrategy(options.Processing));
        services.AddSingleton<IRawFanOutStrategy>(new CanonicalEventFanOutStrategy("Alarm"));
        services.AddSingleton<IRawFanOutStrategy>(new CanonicalEventFanOutStrategy("Outage"));
        services.AddSingleton<IRawFanOutStrategy>(new CanonicalEventFanOutStrategy("Curtailment"));
        services.AddSingleton<IRawFanOutStrategy>(new CanonicalEventFanOutStrategy("Adjustment"));
        services.AddSingleton<IFanOutPlanner>(provider => new FanOutPlanner(
            provider.GetRequiredService<IRawIdentityReader>(),
            provider.GetRequiredService<IFarmCatalog>(),
            options.Processing,
            provider.GetServices<IRawFanOutStrategy>(),
            provider.GetRequiredService<ITelemetry<FanOutPlanner>>(),
            provider.GetRequiredService<IRawObjectStore>()));
        services.AddSingleton<IWorkPublisher, ReliableBatchPublisher>();
        services.AddSingleton<IMessageProcessor, RawFanOutProcessor>();
        services.AddSingleton<DeadLetterProcessor>();
        services.AddSingleton<DeliveryHandler>();
        services.AddSingleton<DeliveryObserver>();
        return services;
    }

    public static IServiceCollection AddQueueExecution(
        this IServiceCollection services,
        WorkerOptions options,
        string[] args)
    {
        services.AddSingleton(provider => new QueueBindings(
            new SqsInputQueue(provider.GetRequiredService<IAmazonSQS>(), options.RawQueueUrl, options.Processing),
            new SqsInputQueue(provider.GetRequiredService<IAmazonSQS>(), options.RawDlqUrl, options.Processing),
            options.RawDlqUrl));
        services.AddSingleton<ReplayIncident>();
        services.AddSingleton<ResolveIncident>();
        if (Array.IndexOf(args, "--replay") < 0 && Array.IndexOf(args, "--cleanup") < 0 && Array.IndexOf(args, "--resolve") < 0)
            services.AddHostedService<QueueWorker>();
        return services;
    }
}