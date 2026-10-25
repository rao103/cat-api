using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Adapters.Source;
using ProductionIngestor.Factories;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Resolvers;
using ProductionIngestor.Worker.Configuration;

namespace ProductionIngestor.UnitTests;

public sealed class SourceAuthenticationTests
{
    private const string ConnectionString = "Server=source.example;Database=production;Encrypt=True;TrustServerCertificate=False;";

    [Fact]
    public void KeyedConfigurationTakesPrecedenceOverLegacyConnectionString()
    {
        var options = CreateOptions();
        var resolved = SqlSourceSettingsResolver.Resolve(options, "Server=legacy.example;Database=legacy;");
        Assert.Equal(ConnectionString, resolved.ConnectionString);
        Assert.Same(options.ServicePrincipals[0], resolved.Principal);
    }

    [Fact]
    public void LegacyConnectionStringIsSupportedWhenNoKeyedConnectionsExist()
    {
        var resolved = SqlSourceSettingsResolver.Resolve(new SqlSourceOptions(), ConnectionString);
        Assert.Equal(ConnectionString, resolved.ConnectionString);
        Assert.Null(resolved.Principal);
    }

    [Theory]
    [InlineData("missing-connection")]
    [InlineData("disabled-connection")]
    [InlineData("duplicate-connection")]
    [InlineData("missing-principal")]
    [InlineData("disabled-principal")]
    [InlineData("duplicate-principal")]
    [InlineData("missing-tenant")]
    [InlineData("missing-client")]
    [InlineData("missing-secret")]
    [InlineData("missing-reference")]
    [InlineData("invalid-resource")]
    [InlineData("negative-threshold")]
    [InlineData("invalid-connection")]
    public void InvalidKeyedConfigurationDoesNotFallBack(string invalidSetting)
    {
        var options = CreateOptions();
        switch (invalidSetting)
        {
            case "missing-connection": options.ConnectionStrings[0].Key = "OtherDatabase"; break;
            case "disabled-connection": options.ConnectionStrings[0].Enabled = false; break;
            case "duplicate-connection": options.ConnectionStrings.Add(options.ConnectionStrings[0]); break;
            case "missing-principal": options.ServicePrincipals.Clear(); break;
            case "disabled-principal": options.ServicePrincipals[0].Enabled = false; break;
            case "duplicate-principal": options.ServicePrincipals.Add(options.ServicePrincipals[0]); break;
            case "missing-tenant": options.Azure.TenantId = ""; break;
            case "missing-client": options.ServicePrincipals[0].ClientId = ""; break;
            case "missing-secret": options.ServicePrincipals[0].Secret = ""; break;
            case "missing-reference": options.ConnectionStrings[0].ServicePrincipalKey = ""; break;
            case "invalid-resource": options.ServicePrincipals[0].Resource = "not-a-resource"; break;
            case "negative-threshold": options.ServicePrincipals[0].ExpirationThresholdInMinutes = -1; break;
            case "invalid-connection": options.ConnectionStrings[0].Value = "not-a-connection"; break;
        }

        Assert.ThrowsAny<ArgumentException>(() => SqlSourceSettingsResolver.Resolve(options, ConnectionString));
    }

    [Theory]
    [InlineData("Integrated Security=True;")]
    [InlineData("User ID=someone;Password=not-a-real-secret;")]
    [InlineData("Authentication=Active Directory Default;")]
    public void TokenAuthenticationRejectsConflictingConnectionCredentials(string authentication)
    {
        var options = CreateOptions();
        options.ConnectionStrings[0].Value += authentication;
        Assert.Throws<ArgumentException>(() => SqlSourceSettingsResolver.Resolve(options, null));
    }

    [Fact]
    public void FactoryCreatesSeparateConnectionsWithTheSameCallback()
    {
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null);
        using var first = factory.CreateConnection();
        using var second = factory.CreateConnection();
        Assert.NotSame(first, second);
        Assert.Equal(ConnectionString, first.ConnectionString);
        Assert.NotNull(first.AccessTokenCallback);
        Assert.Same(first.AccessTokenCallback, second.AccessTokenCallback);
        Assert.Equal(System.Data.ConnectionState.Closed, first.State);
    }

    [Fact]
    public void SharedFactoryContractPreservesAzureSqlConnectionsAndCallback()
    {
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null);
        IDatabaseConnectionFactory provider = factory;
        using var first = provider.CreateConnection();
        using var second = provider.CreateConnection();
        var firstSql = Assert.IsType<SqlConnection>(first);
        var secondSql = Assert.IsType<SqlConnection>(second);
        Assert.NotSame(first, second);
        Assert.Same(firstSql.AccessTokenCallback, secondSql.AccessTokenCallback);
        Assert.NotNull(firstSql.AccessTokenCallback);
    }

    [Fact]
    public void LegacyConnectionsDoNotUseATokenCallback()
    {
        using var factory = new SqlProductionConnectionFactory(new SqlSourceOptions(), ConnectionString);
        using var connection = factory.CreateConnection();
        Assert.Null(connection.AccessTokenCallback);
    }

    [Fact]
    public async Task TokenIsReusedUntilTheConfiguredRefreshBoundary()
    {
        var clock = new TestClock();
        var acquisitions = 0;
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null, cancellationToken =>
        {
            acquisitions++;
            return Task.FromResult(new SqlAuthenticationToken($"test-token-{acquisitions}", clock.GetUtcNow().AddHours(3)));
        }, clock);
        using var connection = factory.CreateConnection();
        var first = await connection.AccessTokenCallback(null!, CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(59));
        Assert.Same(first, await connection.AccessTokenCallback(null!, CancellationToken.None));
        Assert.Equal(1, acquisitions);
        clock.Advance(TimeSpan.FromMinutes(1));
        var refreshed = await connection.AccessTokenCallback(null!, CancellationToken.None);
        Assert.Equal("test-token-2", refreshed.AccessToken);
        Assert.Equal(2, acquisitions);
    }

    [Fact]
    public async Task ThresholdLongerThanTokenLifetimeDoesNotCauseARefreshLoop()
    {
        var clock = new TestClock();
        var acquisitions = 0;
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null, cancellationToken =>
        {
            acquisitions++;
            return Task.FromResult(new SqlAuthenticationToken("test-token", clock.GetUtcNow().AddHours(1)));
        }, clock);
        using var connection = factory.CreateConnection();
        await connection.AccessTokenCallback(null!, CancellationToken.None);
        Assert.Equal(1, acquisitions);
        await connection.AccessTokenCallback(null!, CancellationToken.None);
        Assert.Equal(2, acquisitions);
    }

    [Theory]
    [InlineData("test-token", 0)]
    [InlineData("test-token", -1)]
    [InlineData(" ", 60)]
    public async Task InvalidTokensAreRejected(string token, int lifetimeMinutes)
    {
        var clock = new TestClock();
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null,
            cancellationToken => Task.FromResult(new SqlAuthenticationToken(token, clock.GetUtcNow().AddMinutes(lifetimeMinutes))), clock);
        using var connection = factory.CreateConnection();
        await Assert.ThrowsAsync<InvalidOperationException>(() => connection.AccessTokenCallback(null!, CancellationToken.None));
    }

    [Fact]
    public async Task EmptyTokenRejectionFromSqlClientIsPropagated()
    {
        var clock = new TestClock();
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null,
            cancellationToken => Task.FromResult(new SqlAuthenticationToken("", clock.GetUtcNow().AddHours(3))), clock);
        using var connection = factory.CreateConnection();
        await Assert.ThrowsAsync<ArgumentNullException>(() => connection.AccessTokenCallback(null!, CancellationToken.None));
    }

    [Fact]
    public async Task ConcurrentRequestsShareAnAcquisitionAndWaitingCanBeCancelled()
    {
        var clock = new TestClock();
        var completion = new TaskCompletionSource<SqlAuthenticationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var acquisitions = 0;
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null, cancellationToken =>
        {
            acquisitions++;
            return completion.Task;
        }, clock);
        using var first = factory.CreateConnection();
        using var second = factory.CreateConnection();
        var firstRequest = first.AccessTokenCallback(null!, CancellationToken.None);
        var secondRequest = second.AccessTokenCallback(null!, CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        var cancelledRequest = second.AccessTokenCallback(null!, cancellation.Token);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelledRequest);
        completion.SetResult(new SqlAuthenticationToken("test-token", clock.GetUtcNow().AddHours(3)));
        Assert.Same(await firstRequest, await secondRequest);
        Assert.Equal(1, acquisitions);
    }

    [Fact]
    public async Task AcquisitionReceivesCancellationAndCanBeRetriedAfterCancellation()
    {
        var clock = new TestClock();
        using var cancellation = new CancellationTokenSource();
        var acquisitions = 0;
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null, cancellationToken =>
        {
            acquisitions++;
            if (acquisitions == 1)
            {
                Assert.Equal(cancellation.Token, cancellationToken);
                cancellation.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }
            return Task.FromResult(new SqlAuthenticationToken("test-token", clock.GetUtcNow().AddHours(3)));
        }, clock);
        using var connection = factory.CreateConnection();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connection.AccessTokenCallback(null!, cancellation.Token));
        Assert.Equal("test-token", (await connection.AccessTokenCallback(null!, CancellationToken.None)).AccessToken);
        Assert.Equal(2, acquisitions);
    }

    [Fact]
    public async Task FailedRefreshPropagatesInsteadOfReturningTheOldToken()
    {
        var clock = new TestClock();
        var acquisitions = 0;
        var failure = new InvalidOperationException("Test authentication failure.");
        using var factory = new SqlProductionConnectionFactory(CreateOptions(), null, cancellationToken =>
        {
            acquisitions++;
            if (acquisitions == 2) throw failure;
            return Task.FromResult(new SqlAuthenticationToken("test-token", clock.GetUtcNow().AddHours(3)));
        }, clock);
        using var connection = factory.CreateConnection();
        await connection.AccessTokenCallback(null!, CancellationToken.None);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => connection.AccessTokenCallback(null!, CancellationToken.None)));
        await connection.AccessTokenCallback(null!, CancellationToken.None);
        Assert.Equal(3, acquisitions);
    }

    [Fact]
    public async Task WorkerBindsKeyedSettingsAndValidatesWithoutALegacyConnectionString()
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(CreateWorkerSettings());
        builder.Services.AddProductionMessaging(builder.Configuration).AddProductionSource(builder.Configuration);
        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var options = host.Services.GetRequiredService<IOptions<SqlSourceOptions>>().Value;
            Assert.Equal(120, options.ServicePrincipals[0].ExpirationThresholdInMinutes);
            Assert.Equal(ConnectionString, SqlSourceSettingsResolver.Resolve(options, null).ConnectionString);
            Assert.IsType<SqlProductionSourceAdapter>(host.Services.GetRequiredService<IProductionSourceAdapter>());
            var factory = host.Services.GetRequiredService<SqlProductionConnectionFactory>();
            Assert.Same(factory, host.Services.GetRequiredService<SqlProductionConnectionFactory>());
            using var connection = factory.CreateConnection();
            Assert.NotNull(connection.AccessTokenCallback);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Theory]
    [InlineData("Azure:TenantId")]
    [InlineData("ServicePrincipals:0:Secret")]
    [InlineData("Source:ControlPlaneConnectionString")]
    public async Task WorkerRejectsMissingConfigurationAtStartup(string key)
    {
        var settings = CreateWorkerSettings();
        settings[key] = "";
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddProductionMessaging(builder.Configuration).AddProductionSource(builder.Configuration);
        using var host = builder.Build();
        var failure = await Record.ExceptionAsync(() => host.StartAsync());
        Assert.NotNull(failure);
        var errors = failure is AggregateException aggregate ? aggregate.Flatten().InnerExceptions.ToArray() : [failure];
        if (key == "Source:ControlPlaneConnectionString")
        {
            Assert.All(errors, error =>
            {
                var validation = Assert.IsType<OptionsValidationException>(error);
                Assert.Equal(typeof(ControlPlaneOptions), validation.OptionsType);
                Assert.NotEmpty(validation.Failures);
            });
        }
        else
        {
            Assert.All(errors, error =>
            {
                var validation = Assert.IsType<OptionsValidationException>(error);
                Assert.Equal(typeof(SqlSourceOptions), validation.OptionsType);
                Assert.NotEmpty(validation.Failures);
            });
        }
    }

    [Fact]
    public async Task WorkerAcceptsIamControlPlaneWithoutALegacyConnectionString()
    {
        var certificate = Path.GetTempFileName();
        try
        {
            var settings = CreateWorkerSettings();
            settings.Remove("Source:ControlPlaneConnectionString");
            settings["ControlPlane:Host"] = "cat-dev-pg.cpggeso4i2wb.eu-north-1.rds.amazonaws.com";
            settings["ControlPlane:SigningHost"] = settings["ControlPlane:Host"];
            settings["ControlPlane:Database"] = "cat_dev";
            settings["ControlPlane:Username"] = "test_iam_user";
            settings["ControlPlane:Region"] = "eu-north-1";
            settings["ControlPlane:RootCertificate"] = certificate;
            var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
            builder.Configuration.AddInMemoryCollection(settings);
            builder.Services.AddProductionMessaging(builder.Configuration).AddProductionSource(builder.Configuration);
            using var host = builder.Build();
            await host.StartAsync();
            await host.StopAsync();
        }
        finally { File.Delete(certificate); }
    }

    [Fact]
    public void EnvironmentVariablesOverrideTheIndexedSecretAndTenant()
    {
        var prefix = $"PRODUCTION_AUTH_TEST_{Guid.NewGuid():N}_";
        Environment.SetEnvironmentVariable(prefix + "ServicePrincipals__0__Secret", "test-environment-secret");
        Environment.SetEnvironmentVariable(prefix + "Azure__TenantId", "33333333-3333-3333-3333-333333333333");
        try
        {
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(CreateWorkerSettings())
                .AddEnvironmentVariables(prefix).Build();
            using var services = new ServiceCollection().AddSingleton<IConfiguration>(configuration)
                .AddProductionMessaging(configuration).AddProductionSource(configuration).BuildServiceProvider();
            var options = services.GetRequiredService<IOptions<SqlSourceOptions>>().Value;
            Assert.Equal("test-environment-secret", options.ServicePrincipals[0].Secret);
            Assert.Equal("33333333-3333-3333-3333-333333333333", options.Azure.TenantId);
        }
        finally
        {
            Environment.SetEnvironmentVariable(prefix + "ServicePrincipals__0__Secret", null);
            Environment.SetEnvironmentVariable(prefix + "Azure__TenantId", null);
        }
    }

    private static Dictionary<string, string?> CreateWorkerSettings() => new()
    {
        ["Azure:TenantId"] = "11111111-1111-1111-1111-111111111111",
        ["ConnectionStrings:0:Key"] = "ProductionDatabase",
        ["ConnectionStrings:0:Enabled"] = "true",
        ["ConnectionStrings:0:Value"] = ConnectionString,
        ["ConnectionStrings:0:ServicePrincipalKey"] = "production-source",
        ["ServicePrincipals:0:Key"] = "production-source",
        ["ServicePrincipals:0:Enabled"] = "true",
        ["ServicePrincipals:0:ClientId"] = "22222222-2222-2222-2222-222222222222",
        ["ServicePrincipals:0:Secret"] = "test-only-secret",
        ["ServicePrincipals:0:Resource"] = "https://database.windows.net/",
        ["ServicePrincipals:0:ExpirationThresholdInMinutes"] = "120",
        ["Source:SourceSystemName"] = "test-source",
        ["Source:ControlPlaneConnectionString"] = "Host=control.example;Database=test;",
        ["Source:BlobConnectionString"] = "UseDevelopmentStorage=true",
        ["Source:BlobContainerName"] = "test-container",
        ["Sqs:Region"] = "eu-north-1",
        ["Sqs:RawFanOut:QueueUrl"] = "https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev"
    };

    private sealed class TestClock : TimeProvider
    {
        private DateTimeOffset _utcNow = new(2026, 9, 15, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => _utcNow;
        public void Advance(TimeSpan duration) => _utcNow += duration;
    }

    private static SqlSourceOptions CreateOptions() => new()
    {
        Azure = new() { TenantId = "11111111-1111-1111-1111-111111111111" },
        ConnectionStrings = [new()
        {
            Key = "ProductionDatabase", Enabled = true, Value = ConnectionString,
            ServicePrincipalKey = "production-source"
        }],
        ServicePrincipals = [new()
        {
            Key = "production-source", Enabled = true,
            ClientId = "22222222-2222-2222-2222-222222222222", Secret = "test-only-secret",
            Resource = "https://database.windows.net/", ExpirationThresholdInMinutes = 120
        }]
    };
}