using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text.Json;
using Centralize.Api.Controllers.v1;
using Centralize.Core.Interfaces;
using Centralize.Core.Models;
using Centralize.Core.Validators;
using k8s;
using k8s.Models;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Centralize.UnitTests;

public sealed class AuthenticatedTriggerFlowTests
{
    [Fact]
    public async Task BearerHttpFlow_RejectsAnonymousAndInvalidTokens_ThenCreatesCorrelatedJobs()
    {
        const string audience = "00000000-0000-0000-0000-000000000002";
        const string issuer = "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000001/v2.0";
        using var rsa = RSA.Create(2048);
        var key = new RsaSecurityKey(rsa) { KeyId = "test-signing-key" };
        var repository = new RecordingRepository();
        var jobs = new List<V1Job>();
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = "Production", ApplicationName = typeof(RefreshController).Assembly.GetName().Name,
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.Configuration.Sources.Clear();
        var settings = new Dictionary<string, string?>
        {
            ["AzureAd:Enabled"] = "true", ["AzureAd:Instance"] = "https://login.microsoftonline.com/",
            ["AzureAd:TenantId"] = "00000000-0000-0000-0000-000000000001", ["AzureAd:ClientId"] = audience,
            ["Source:ControlPlaneConnectionString"] = "Host=unit.invalid;Database=unused;Username=unused",
            ["JobTrigger:Namespace"] = "unit"
        };
        foreach (var source in OperationRequestValidator.SupportedSources)
        {
            settings[$"JobTrigger:Sources:{source}:CronJobName"] = source.ToLowerInvariant() + "-ingestor";
            settings[$"JobTrigger:Sources:{source}:WorkerContainerName"] = source.ToLowerInvariant() + "-ingestor-worker";
        }
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        typeof(RefreshController).Assembly.GetType("Centralize.Api.Configurators.ServicesConfigurator")!
            .GetMethod("ConfigureServices", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, [builder.WebHost]);
        builder.Services.Replace(ServiceDescriptor.Singleton<IOperationRepository>(repository));
        string? address = null;
        builder.Services.Replace(ServiceDescriptor.Singleton<IKubernetes>(_ =>
            new Kubernetes(new KubernetesClientConfiguration { Host = address! })));
        builder.Services.PostConfigure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme, options =>
        {
            var metadata = new OpenIdConnectConfiguration { Issuer = issuer };
            metadata.SigningKeys.Add(key);
            options.Configuration = metadata;
            options.ConfigurationManager = new StaticConfigurationManager<OpenIdConnectConfiguration>(metadata);
            options.TokenValidationParameters.ValidAudience = audience;
            options.TokenValidationParameters.IssuerValidator = (value, _, _) =>
                value == issuer ? value : throw new SecurityTokenInvalidIssuerException();
        });
        await using var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapControllers();
        app.MapGet("/apis/batch/v1/namespaces/unit/cronjobs/{name}", (string name) =>
            Results.Text(KubernetesJson.Serialize(Template(name)), "application/json")).AllowAnonymous();
        app.MapPost("/apis/batch/v1/namespaces/unit/jobs", async (HttpRequest request) =>
        {
            using var reader = new StreamReader(request.Body);
            var job = KubernetesJson.Deserialize<V1Job>(await reader.ReadToEndAsync());
            jobs.Add(job);
            return Results.Text(KubernetesJson.Serialize(job), "application/json", statusCode: 201);
        }).AllowAnonymous();
        await app.StartAsync();
        try
        {
            address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
            using var client = new HttpClient { BaseAddress = new Uri(address) };
            var dates = new { startDate = "2026-09-01", endDate = "2026-09-02" };
            using var anonymous = await client.PostAsJsonAsync("/v1/windfarms/12/refresh", dates);
            Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
            Assert.Empty(repository.Operations);
            Assert.Empty(jobs);
            client.DefaultRequestHeaders.Authorization = new("Bearer", Token(key, issuer, "wrong-audience"));
            using var invalid = await client.PostAsJsonAsync("/v1/windfarms/12/refresh", dates);
            Assert.Equal(HttpStatusCode.Unauthorized, invalid.StatusCode);
            Assert.Empty(repository.Operations);
            client.DefaultRequestHeaders.Authorization = new("Bearer", Token(key, issuer, audience));
            using var all = await client.PostAsJsonAsync("/v1/windfarms/12/refresh", dates);
            Assert.Equal(HttpStatusCode.Accepted, all.StatusCode);
            var json = await all.Content.ReadFromJsonAsync<JsonElement>();
            var operationId = json.GetProperty("operationId").GetGuid();
            Assert.Equal(operationId, Assert.Single(repository.Operations));
            Assert.Equal(4, jobs.Count);
            Assert.All(jobs, job =>
            {
                Assert.Equal(operationId.ToString("D"), job.Metadata.Labels["operation-id"]);
                var env = job.Spec.Template.Spec.Containers[0].Env.ToDictionary(item => item.Name, item => item.Value);
                Assert.Equal(operationId.ToString("D"), env["OPERATION_ID"]);
                Assert.Equal("http-caller", env["REFRESH_REQUESTED_BY"]);
            });
            using var single = await client.PostAsJsonAsync("/v1/windfarms/12/sources/Outage/refresh", dates);
            Assert.Equal(HttpStatusCode.Accepted, single.StatusCode);
            Assert.Equal(5, jobs.Count);
            Assert.StartsWith("outage-ingestor-refresh-", jobs[^1].Metadata.Name);
            using var adjustment = await client.PostAsJsonAsync("/v1/windfarms/12/sources/Adjustment/refresh", dates);
            Assert.Equal(HttpStatusCode.BadRequest, adjustment.StatusCode);
            Assert.Equal(2, repository.Operations.Count);
            Assert.Equal(5, jobs.Count);
        }
        finally { await app.StopAsync(); }
    }

    private static string Token(SecurityKey key, string issuer, string audience) =>
        new JwtSecurityTokenHandler().WriteToken(new JwtSecurityToken(issuer, audience,
            [new Claim("oid", "http-caller"), new Claim("scp", "access_as_user")],
            notBefore: DateTime.UtcNow.AddMinutes(-1), expires: DateTime.UtcNow.AddMinutes(5),
            signingCredentials: new SigningCredentials(key, SecurityAlgorithms.RsaSha256)));

    private static V1CronJob Template(string name) => new()
    {
        Metadata = new V1ObjectMeta { Name = name },
        Spec = new V1CronJobSpec
        {
            Schedule = "* * * * *",
            JobTemplate = new V1JobTemplateSpec
            {
                Spec = new V1JobSpec
                {
                    Template = new V1PodTemplateSpec
                    {
                        Spec = new V1PodSpec
                        {
                            RestartPolicy = "Never",
                            Containers = [new V1Container { Name = name + "-worker", Image = "unit:test" }]
                        }
                    }
                }
            }
        }
    };

    private sealed class RecordingRepository : IOperationRepository
    {
        public List<Guid> Operations { get; } = [];
        public Task<OperationModel> CreateAsync(string caller, CancellationToken token)
        {
            Assert.Equal("http-caller", caller);
            var operation = new OperationModel(Guid.NewGuid(), DateTimeOffset.UtcNow, caller, [], []);
            Operations.Add(operation.OperationId);
            return Task.FromResult(operation);
        }
        public Task<IReadOnlyList<RefreshSourceModel>> GetRefreshSourcesAsync(OperationRequestModel request, CancellationToken token) =>
            Task.FromResult<IReadOnlyList<RefreshSourceModel>>((request.SourceType is null
                ? OperationRequestValidator.SupportedSources : new[] { request.SourceType })
                .Select(source => new RefreshSourceModel(request.WindfarmId, "WOW02", source)).ToArray());
        public Task<OperationModel?> GetAsync(Guid id, string caller, CancellationToken token) => Task.FromResult<OperationModel?>(null);
        public Task<IReadOnlyList<RefreshAttempt>> GetRefreshAttemptsAsync(string id, CancellationToken token) =>
            Task.FromResult<IReadOnlyList<RefreshAttempt>>([]);
    }
}
