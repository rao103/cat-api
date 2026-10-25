using Amazon;
using Amazon.SQS;
using Microsoft.Extensions.Options;
using ProductionIngestor.Adapters.Messaging;
using ProductionIngestor.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Validators;

namespace ProductionIngestor.Worker.Configuration;

public static class ProductionMessagingConfiguration
{
    public static IServiceCollection AddProductionMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<IValidateOptions<SqsOptions>, SqsOptionsValidator>();
        services.AddOptions<SqsOptions>()
            .Bind(configuration.GetSection(SqsOptions.SectionName))
            .ValidateOnStart();

        services.AddSingleton<AmazonSQSConfig>(provider => new()
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(provider.GetRequiredService<IOptions<SqsOptions>>().Value.Region)
        });
        services.AddSingleton<IAmazonSQS>(provider => new AmazonSQSClient(provider.GetRequiredService<AmazonSQSConfig>()));
        services.AddSingleton<IRawArtifactPublisher>(provider => new SqsRawArtifactPublisher(
            provider.GetRequiredService<IAmazonSQS>(),
            provider.GetRequiredService<IOptions<SqsOptions>>().Value.RawFanOut.QueueUrl));
        return services;
    }
}