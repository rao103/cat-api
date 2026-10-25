using Amazon.SQS;
using Microsoft.Extensions.Options;
using OutageIngestor.Adapters.Messaging;
using OutageIngestor.Configuration;
using OutageIngestor.Interfaces;

namespace OutageIngestor.Worker.Configuration;

public static class OutageMessagingConfiguration
{
    public static IServiceCollection AddOutageMessaging(this IServiceCollection services)
    {
        services.AddSingleton<IAmazonSQS, AmazonSQSClient>();
        services.AddSingleton<IRawArtifactPublisher>(provider => new SqsRawArtifactPublisher(
            provider.GetRequiredService<IAmazonSQS>(),
            provider.GetRequiredService<IOptions<SourceOptions>>().Value.SqsQueueUrl));
        return services;
    }
}
