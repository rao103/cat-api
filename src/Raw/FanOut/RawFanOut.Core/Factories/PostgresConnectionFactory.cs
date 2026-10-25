using System;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Authentication;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using Npgsql;

namespace CAT.RawFanOut.Factories;

public sealed class PostgresConnectionFactory : IPostgresConnectionFactory, IAsyncDisposable
{
    private readonly NpgsqlDataSource dataSource;
    private bool disposed;

    public PostgresConnectionFactory(ControlPlaneOptions options)
    {
        var connectionString = new NpgsqlConnectionStringBuilder
        {
            Host = options.Host,
            Port = options.Port,
            Database = options.Database,
            Username = options.Username,
            SslMode = SslMode.VerifyFull,
            RootCertificate = Environment.ExpandEnvironmentVariables(options.RootCertificate),
            PersistSecurityInfo = false,
            IncludeErrorDetail = false
        }.ConnectionString;

        var builder = new NpgsqlDataSourceBuilder(connectionString);
        var passwordProvider = new RdsIamPasswordProvider(options);
        builder.UsePasswordProvider(
            _ => throw new NotSupportedException("Use OpenAsync for RDS IAM connections."),
            (_, cancellationToken) => passwordProvider.GetPasswordAsync(cancellationToken));
        dataSource = builder.Build();
    }

    public ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        return dataSource.OpenConnectionAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        disposed = true;
        return dataSource.DisposeAsync();
    }
}