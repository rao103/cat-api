using Amazon.SQS;
using Microsoft.Extensions.Options;
using CurtailmentIngestor.Adapters.Messaging;
using CurtailmentIngestor.Configuration;
using CurtailmentIngestor.Interfaces;

namespace CurtailmentIngestor.Worker.Configuration;

public static class CurtailmentMessagingConfiguration
{
    public static IServiceCollection AddCurtailmentMessaging(this IServiceCollection services)
    {
        services.AddSingleton<IAmazonSQS, AmazonSQSClient>();
        services.AddSingleton<IRawArtifactPublisher>(provider => new SqsRawArtifactPublisher(
            provider.GetRequiredService<IAmazonSQS>(),
            provider.GetRequiredService<IOptions<SourceOptions>>().Value.SqsQueueUrl));
        return services;
    }
}
