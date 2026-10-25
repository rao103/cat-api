using Amazon.SQS;
using Microsoft.Extensions.Options;
using AlarmIngestor.Adapters.Messaging;
using AlarmIngestor.Configuration;
using AlarmIngestor.Interfaces;

namespace AlarmIngestor.Worker.Configuration;

public static class AlarmMessagingConfiguration
{
    public static IServiceCollection AddAlarmMessaging(this IServiceCollection services)
    {
        services.AddSingleton<IAmazonSQS, AmazonSQSClient>();
        services.AddSingleton<IRawArtifactPublisher>(provider => new SqsRawArtifactPublisher(
            provider.GetRequiredService<IAmazonSQS>(),
            provider.GetRequiredService<IOptions<SourceOptions>>().Value.SqsQueueUrl));
        return services;
    }
}
