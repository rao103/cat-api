using Amazon;
using Amazon.RDS.Util;
using Harmonizer.Core.Configuration;
using Npgsql;
namespace Harmonizer.Worker.DependencyInjection;

public static class PostgresDataSourceFactory
{
    public static NpgsqlDataSource Create(HarmonizerOptions options)
    {
        var builder = new NpgsqlDataSourceBuilder(options.PostgresConnectionString);
        if (options.UseRdsIamAuthentication)
        {
            if (builder.ConnectionStringBuilder.SslMode != SslMode.VerifyFull)
            {
                throw new InvalidOperationException("RDS IAM authentication requires Ssl Mode=VerifyFull and a trusted RDS certificate chain.");
            }

            if (!string.IsNullOrEmpty(builder.ConnectionStringBuilder.Password))
            {
                throw new InvalidOperationException("Do not combine a static password with RDS IAM authentication.");
            }

            builder.UsePeriodicPasswordProvider(async (connection, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var token = await RDSAuthTokenGenerator.GenerateAuthTokenAsync(
                    RegionEndpoint.GetBySystemName(options.AwsRegion),
                    connection.Host ?? throw new InvalidOperationException("RDS host is required."), connection.Port,
                    connection.Username ?? throw new InvalidOperationException("RDS database username is required."));
                cancellationToken.ThrowIfCancellationRequested();
                return token;
            }, TimeSpan.FromMinutes(10), TimeSpan.FromSeconds(20));
        }
        return builder.Build();
    }
}
