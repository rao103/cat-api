using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using ProductionIngestor.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Resolvers;
using System.Data.Common;

namespace ProductionIngestor.Factories;

public sealed class SqlProductionConnectionFactory : IDatabaseConnectionFactory, IDisposable
{
    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _refreshThreshold;
    private readonly SemaphoreSlim _tokenLock = new(1, 1);
    private readonly Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>>? _tokenCallback;
    private SqlAuthenticationToken? _cachedToken;

    public SqlProductionConnectionFactory(SqlSourceOptions options, string? legacyConnectionString,
        Func<CancellationToken, Task<SqlAuthenticationToken>>? acquireToken = null, TimeProvider? timeProvider = null)
    {
        var resolved = SqlSourceSettingsResolver.Resolve(options, legacyConnectionString);
        _connectionString = resolved.ConnectionString;
        _timeProvider = timeProvider ?? TimeProvider.System;
        if (resolved.Principal is { } principal)
        {
            _refreshThreshold = TimeSpan.FromMinutes(principal.ExpirationThresholdInMinutes);
            var tokenAcquirer = acquireToken ?? CreateTokenAcquirer(options.Azure.TenantId, principal);
            _tokenCallback = (_, cancellationToken) => GetTokenAsync(tokenAcquirer, cancellationToken);
        }
    }

    public SqlConnection CreateConnection()
    {
        var connection = new SqlConnection(_connectionString);
        if (_tokenCallback is not null)
            connection.AccessTokenCallback = _tokenCallback;
        return connection;
    }

    DbConnection IDatabaseConnectionFactory.CreateConnection() => CreateConnection();

    private async Task<SqlAuthenticationToken> GetTokenAsync(
        Func<CancellationToken, Task<SqlAuthenticationToken>> acquireToken, CancellationToken cancellationToken)
    {
        await _tokenLock.WaitAsync(cancellationToken);
        try
        {
            if (_cachedToken is null || _cachedToken.ExpiresOn - _timeProvider.GetUtcNow() <= _refreshThreshold)
            {
                var token = await acquireToken(cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (string.IsNullOrWhiteSpace(token.AccessToken) || token.ExpiresOn <= _timeProvider.GetUtcNow())
                    throw new InvalidOperationException("The production source identity provider returned an empty or expired token.");
                _cachedToken = token;
            }
            return _cachedToken;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    private static Func<CancellationToken, Task<SqlAuthenticationToken>> CreateTokenAcquirer(
        string tenantId, SourceServicePrincipalOptions principal)
    {
        var application = ConfidentialClientApplicationBuilder.Create(principal.ClientId)
            .WithTenantId(tenantId)
            .WithClientSecret(principal.Secret)
            .Build();
        var scopes = new[] { principal.Resource.TrimEnd('/') + "/.default" };
        return async cancellationToken =>
        {
            var result = await application.AcquireTokenForClient(scopes)
                .WithForceRefresh(true)
                .ExecuteAsync(cancellationToken);
            return new SqlAuthenticationToken(result.AccessToken, result.ExpiresOn);
        };
    }

    public void Dispose() => _tokenLock.Dispose();
}