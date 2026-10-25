using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Authentication;
using ProductionIngestor.Factories;
using ProductionIngestor.Repositories;
using ProductionIngestor.Validators;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Worker.Configuration;

namespace ProductionIngestor.UnitTests;

public sealed class ControlPlaneAuthenticationTests : IDisposable
{
    private readonly string _certificate = Path.GetTempFileName();

    [Fact]
    public void TunnelPortIsSeparateFromSigningPortAndTlsCannotBeDowngraded()
    {
        var options = CreateOptions();
        Assert.True(new ControlPlaneOptionsValidator().Validate(null, options).Succeeded);
        using var factory = new PostgresConnectionFactory(options);
        using var databaseConnection = factory.CreateConnection();
        var connection = new NpgsqlConnectionStringBuilder(databaseConnection.ConnectionString);
        Assert.Equal(55434, connection.Port);
        Assert.Equal(5432, options.SigningPort);
        Assert.Equal(options.SigningHost, connection.Host);
        Assert.Equal(SslMode.VerifyFull, connection.SslMode);
        Assert.Equal(_certificate, connection.RootCertificate);
        Assert.True(string.IsNullOrEmpty(connection.Password));
        Assert.False(connection.PersistSecurityInfo);
        Assert.False(connection.IncludeErrorDetail);
    }

    [Theory]
    [InlineData("provider")]
    [InlineData("authentication")]
    [InlineData("connection-string")]
    [InlineData("host")]
    [InlineData("signing-host")]
    [InlineData("database")]
    [InlineData("username")]
    [InlineData("region")]
    [InlineData("certificate")]
    [InlineData("missing-certificate")]
    [InlineData("port")]
    [InlineData("signing-port")]
    public void InvalidIamSettingsAreRejected(string setting)
    {
        var options = CreateOptions();
        switch (setting)
        {
            case "provider": options.Provider = "AzureSql"; break;
            case "authentication": options.Authentication = "Password"; break;
            case "connection-string": options.ConnectionString = "Host=legacy;Password=test-only;"; break;
            case "host": options.Host = "localhost"; break;
            case "signing-host": options.SigningHost = "localhost"; break;
            case "database": options.Database = ""; break;
            case "username": options.Username = ""; break;
            case "region": options.Region = "us-east-1"; break;
            case "certificate": options.RootCertificate = ""; break;
            case "missing-certificate": options.RootCertificate = Path.Combine(_certificate, "missing.pem"); break;
            case "port": options.Port = 0; break;
            case "signing-port": options.SigningPort = 65536; break;
        }
        var result = new ControlPlaneOptionsValidator().Validate(null, options);
        Assert.True(result.Failed);
        Assert.False(string.IsNullOrWhiteSpace(result.FailureMessage));
        Assert.ThrowsAny<ArgumentException>(() => new PostgresConnectionFactory(options));
    }

    [Fact]
    public void LegacyFactoryUsesTheSameContractWithoutAwsCredentials()
    {
        using var factory = new PostgresConnectionFactory(new ControlPlaneOptions
        {
            Authentication = "ConnectionString", ConnectionString = "Host=localhost;Database=test;Username=test;Password=test-only;"
        }, _ => throw new InvalidOperationException("IAM should not be used."));
        IDatabaseConnectionFactory provider = factory;
        using var first = provider.CreateConnection();
        using var second = provider.CreateConnection();
        Assert.IsType<NpgsqlConnection>(first);
        Assert.NotSame(first, second);
        Assert.Equal(System.Data.ConnectionState.Closed, first.State);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("not-a-connection-string")]
    public async Task InvalidLegacyConnectionStringsFailValidationAndStartup(string? connectionString)
    {
        var options = new ControlPlaneOptions
        {
            Authentication = "ConnectionString",
            ConnectionString = connectionString!
        };
        Assert.True(new ControlPlaneOptionsValidator().Validate(null, options).Failed);
        Assert.ThrowsAny<ArgumentException>(() => new PostgresConnectionFactory(options));

        using var host = CreateHost(new() { ["Source:ControlPlaneConnectionString"] = connectionString });
        await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
    }

    [Fact]
    public void IamFactoryDoesNotAcquireCredentialsUntilAuthentication()
    {
        using var factory = new PostgresConnectionFactory(CreateOptions(),
            _ => throw new InvalidOperationException("Must not authenticate during construction."));
        using var connection = factory.CreateConnection();
        Assert.Equal(System.Data.ConnectionState.Closed, connection.State);
        Assert.True(string.IsNullOrEmpty(new NpgsqlConnectionStringBuilder(connection.ConnectionString).Password));
    }

    [Fact]
    public async Task MissingDatabaseProfileFailsWithoutUsingDefaultCredentials()
    {
        var options = CreateOptions();
        options.AwsProfile = $"missing-cat-test-{Guid.NewGuid():N}";
        var provider = new RdsIamPasswordProvider(options);
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.GetPasswordAsync(CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task EachAuthenticationAcquiresANewTokenRatherThanCachingAnExpiredOne()
    {
        var acquisitions = 0;
        var provider = new RdsIamPasswordProvider(CreateOptions(), _ => Task.FromResult($"test-token-{++acquisitions}"));
        Assert.Equal("test-token-1", await provider.GetPasswordAsync(CancellationToken.None));
        Assert.Equal("test-token-2", await provider.GetPasswordAsync(CancellationToken.None));
    }

    [Fact]
    public async Task AuthenticationFailureIsPropagatedAndNotCached()
    {
        var acquisitions = 0;
        var failure = new InvalidOperationException("Test authentication failure.");
        var provider = new RdsIamPasswordProvider(CreateOptions(), _ =>
            ++acquisitions == 1 ? Task.FromException<string>(failure) : Task.FromResult("test-token"));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => provider.GetPasswordAsync(CancellationToken.None).AsTask()));
        Assert.Equal("test-token", await provider.GetPasswordAsync(CancellationToken.None));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public async Task EmptyTokensAreRejected(string token)
    {
        var provider = new RdsIamPasswordProvider(CreateOptions(), _ => Task.FromResult(token));
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.GetPasswordAsync(CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task CancellationInterruptsCredentialWaitingAndReachesTheProvider()
    {
        using var cancellation = new CancellationTokenSource();
        var pending = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new RdsIamPasswordProvider(CreateOptions(), token =>
        {
            Assert.Equal(cancellation.Token, token);
            return pending.Task;
        });
        var request = provider.GetPasswordAsync(cancellation.Token).AsTask();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request);
        pending.SetResult("test-token");
    }

    [Fact]
    public async Task PreCancelledAuthenticationDoesNotRequestCredentials()
    {
        var provider = new RdsIamPasswordProvider(CreateOptions(), _ => throw new InvalidOperationException("Must not run."));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.GetPasswordAsync(new CancellationToken(true)).AsTask());
    }

    [Fact]
    public async Task WorkerSupportsLegacySettingsWhenTheExplicitSectionIsAbsent()
    {
        using var host = CreateHost(new() { ["Source:ControlPlaneConnectionString"] = "Host=localhost;Database=test;" });
        await host.StartAsync();
        try
        {
            Assert.Equal("ConnectionString", host.Services.GetRequiredService<IOptions<ControlPlaneOptions>>().Value.Authentication);
            Assert.IsType<PostgresExtractionCursorRepository>(host.Services.GetRequiredService<IExtractionCursorRepository>());
        }
        finally { await host.StopAsync(); }
    }

    [Fact]
    public async Task ExplicitIamSettingsOverrideLegacyAndResolveOneSharedFactory()
    {
        var settings = CreateIamSettings();
        settings["Source:ControlPlaneConnectionString"] = "not-a-connection-string";
        using var host = CreateHost(settings);
        await host.StartAsync();
        try
        {
            var factory = host.Services.GetRequiredService<PostgresConnectionFactory>();
            Assert.Same(factory, host.Services.GetRequiredService<PostgresConnectionFactory>());
            Assert.IsType<PostgresExtractionCursorRepository>(host.Services.GetRequiredService<IExtractionCursorRepository>());
            using var connection = factory.CreateConnection();
            Assert.Equal(55434, new NpgsqlConnectionStringBuilder(connection.ConnectionString).Port);
        }
        finally { await host.StopAsync(); }
    }

    [Theory]
    [InlineData("ControlPlane:Provider", "AzureSql")]
    [InlineData("ControlPlane:Username", "")]
    [InlineData("ControlPlane:Authentication", "invalid")]
    [InlineData("ControlPlane:RootCertificate", "")]
    public async Task InvalidExplicitSettingsFailStartupInsteadOfFallingBack(string key, string value)
    {
        var settings = CreateIamSettings();
        settings["Source:ControlPlaneConnectionString"] = "Host=localhost;Database=test;";
        settings[key] = value;
        using var host = CreateHost(settings);
        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.Equal(typeof(ControlPlaneOptions), exception.OptionsType);
        Assert.NotEmpty(exception.Failures);
    }

    [Fact]
    public void DisposingTheFactoryPreventsCreatingMoreConnections()
    {
        var factory = new PostgresConnectionFactory(CreateOptions());
        factory.Dispose();
        Assert.Throws<ObjectDisposedException>(() => factory.CreateConnection());
    }

    private static IHost CreateHost(Dictionary<string, string?> settings)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddProductionControlPlane(builder.Configuration);
        return builder.Build();
    }

    private Dictionary<string, string?> CreateIamSettings() => new()
    {
        ["ControlPlane:Provider"] = "PostgreSql",
        ["ControlPlane:Authentication"] = "AwsIam",
        ["ControlPlane:Host"] = CreateOptions().Host,
        ["ControlPlane:SigningHost"] = CreateOptions().SigningHost,
        ["ControlPlane:Port"] = "55434",
        ["ControlPlane:Database"] = "cat_dev",
        ["ControlPlane:Username"] = "test_iam_user",
        ["ControlPlane:Region"] = "eu-north-1",
        ["ControlPlane:RootCertificate"] = _certificate
    };

    private ControlPlaneOptions CreateOptions() => new()
    {
        Host = "cat-dev-pg.cpggeso4i2wb.eu-north-1.rds.amazonaws.com",
        SigningHost = "cat-dev-pg.cpggeso4i2wb.eu-north-1.rds.amazonaws.com",
        Port = 55434,
        Database = "cat_dev",
        Username = "test_iam_user",
        Region = "eu-north-1",
        RootCertificate = _certificate
    };

    public void Dispose() => File.Delete(_certificate);
}