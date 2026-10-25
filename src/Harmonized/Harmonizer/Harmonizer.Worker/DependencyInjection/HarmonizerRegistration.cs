using System.ComponentModel.DataAnnotations;
using Amazon;
using Amazon.SQS;
using Azure.Identity;
using Azure.Storage.Blobs;
using Harmonizer.Core.Adapters;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Operations;
using Harmonizer.Core.Repositories;
using Harmonizer.Core.Serialization;
using Harmonizer.Worker.Hosting;
using Harmonizer.Worker.SourceTypes.Outage;
using Harmonizer.Worker.SourceTypes.Production;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Harmonizer.Worker.DependencyInjection;

public static class HarmonizerRegistration
{
    public static IServiceCollection AddHarmonizer(this IServiceCollection services, IConfiguration configuration)
    {
        RegisterOptions(services, configuration);
        RegisterProviderClients(services);
        RegisterSharedAdapters(services);
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<HarmonizationDispatcher>();
        services.AddProductionHarmonization();
        services.AddOutageHarmonization();
        services.AddSingleton<EventFootprintPlanner>();
        services.AddSingleton<HarmonizedArtifactPathBuilder>();
        services.AddHostedService<HarmonizerWorker>();
        services.AddHostedService<EventLifecycleWorker>();
        if (configuration.GetValue<bool>("Harmonizer:PublishNotifications"))
        {
            services.AddHostedService<NotificationWorker>();
        }

        return services;
    }

    private static void RegisterOptions(IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<HarmonizerOptions>()
            .Bind(configuration.GetSection(HarmonizerOptions.Section))
            .Validate(HasValidProperties, "Harmonizer configuration is missing required values or exceeds a bound.")
            .Validate(options => options.HeartbeatSeconds * 3 < options.LeaseSeconds &&
                                 options.HeartbeatSeconds * 3 < options.VisibilitySeconds,
                "Lease and visibility durations must exceed three heartbeat intervals.")
            .Validate(options => IsHttpUri(options.BlobContainerUrl) &&
                                 IsHttpUri(options.OutputBlobContainerUrl) &&
                                 IsHttpUri(options.QueueUrl),
                "BlobContainerUrl, OutputBlobContainerUrl, and QueueUrl must be absolute HTTP(S) URIs.")
            .Validate(options => !options.PublishNotifications ||
                                 Uri.TryCreate(options.NotificationQueueUrl, UriKind.Absolute, out _),
                "NotificationQueueUrl is required when publishing is enabled.")
            .ValidateOnStart();
    }

    private static bool IsHttpUri(string value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https";

    private static bool HasValidProperties(HarmonizerOptions options) =>
        Validator.TryValidateObject(options, new ValidationContext(options), new List<ValidationResult>(),
            validateAllProperties: true) &&
        Validator.TryValidateObject(options.EventLifecycle, new ValidationContext(options.EventLifecycle),
            new List<ValidationResult>(), validateAllProperties: true);

    private static void RegisterProviderClients(IServiceCollection services)
    {
        services.AddSingleton(provider => PostgresDataSourceFactory.Create(
            provider.GetRequiredService<IOptions<HarmonizerOptions>>().Value));
        services.AddSingleton<IRawObjectStore>(provider =>
        {
            var options = provider.GetRequiredService<IOptions<HarmonizerOptions>>().Value;
            return CreateBlobObjectStore(provider, options.BlobContainerUrl, options.BlobConnectionString, "Raw");
        });
        services.AddSingleton<IHarmonizedArtifactStore>(provider =>
        {
            var options = provider.GetRequiredService<IOptions<HarmonizerOptions>>().Value;
            return CreateBlobObjectStore(provider, options.OutputBlobContainerUrl, options.OutputBlobConnectionString, "Harmonized");
        });
        services.AddSingleton<IAmazonSQS>(provider => CreateSqsClient(
            provider.GetRequiredService<IOptions<HarmonizerOptions>>().Value));
    }

    private static BlobObjectStore CreateBlobObjectStore(IServiceProvider provider, string containerUrl,
        string? connectionString, string storageRole) => new(CreateBlobClient(containerUrl, connectionString),
        provider.GetRequiredService<ITelemetry<BlobObjectStore>>(), storageRole);

    private static BlobContainerClient CreateBlobClient(string containerUrl, string? connectionString)
    {
        var containerUri = new Uri(containerUrl);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return new BlobContainerClient(containerUri, new DefaultAzureCredential());
        }

        var containerName = containerUri.AbsolutePath.Trim('/').Split('/').Last();
        return new BlobContainerClient(connectionString, containerName);
    }

    private static AmazonSQSClient CreateSqsClient(HarmonizerOptions options)
    {
        var configuration = new AmazonSQSConfig { RegionEndpoint = RegionEndpoint.GetBySystemName(options.AwsRegion) };
        if (!string.IsNullOrWhiteSpace(options.SqsServiceUrl))
        {
            configuration.ServiceURL = options.SqsServiceUrl;
        }

        return new AmazonSQSClient(configuration);
    }

    private static void RegisterSharedAdapters(IServiceCollection services)
    {
        services.AddSingleton<INotificationOutbox, PostgresNotificationOutbox>();
        services.AddSingleton<IWorkQueue, SqsWorkQueue>();
        services.AddSingleton<INotificationPublisher, SqsNotificationPublisher>();
    }
}
