using System.Data.Common;
using Npgsql;
using ProductionIngestor.Authentication;
using ProductionIngestor.Configuration;
using ProductionIngestor.Core.Constants;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Validators;

namespace ProductionIngestor.Factories;

public sealed class PostgresConnectionFactory : IDatabaseConnectionFactory, IDisposable, IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    private bool _disposed;

    public PostgresConnectionFactory(ControlPlaneOptions options,
        Func<CancellationToken, Task<string>>? generateToken = null)
    {
        ControlPlaneOptionsValidator.ValidateAndThrow(options);
        var builder = new NpgsqlDataSourceBuilder(BuildConnectionString(options));
        if (string.Equals(options.Authentication, Constant.AwsIam, StringComparison.OrdinalIgnoreCase))
        {
            var passwordProvider = new RdsIamPasswordProvider(options, generateToken);
            builder.UsePasswordProvider(
                _ => throw new NotSupportedException("Use OpenAsync for RDS IAM connections."),
                (_, cancellationToken) => passwordProvider.GetPasswordAsync(cancellationToken));
        }
        _dataSource = builder.Build();
    }

    private static string BuildConnectionString(ControlPlaneOptions options)
    {
        if (string.Equals(options.Authentication, Constant.ConnectionString, StringComparison.OrdinalIgnoreCase))
            return new NpgsqlConnectionStringBuilder(options.ConnectionString).ConnectionString;

        return new NpgsqlConnectionStringBuilder
        {
            Host = options.Host,
            Port = options.Port,
            Database = options.Database,
            Username = options.Username,
            SslMode = SslMode.VerifyFull,
            RootCertificate = options.RootCertificate,
            PersistSecurityInfo = false,
            IncludeErrorDetail = false
        }.ConnectionString;
    }

    public NpgsqlConnection CreateConnection()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _dataSource.CreateConnection();
    }

    DbConnection IDatabaseConnectionFactory.CreateConnection() => CreateConnection();

    public void Dispose()
    {
        _disposed = true;
        _dataSource.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return _dataSource.DisposeAsync();
    }
}