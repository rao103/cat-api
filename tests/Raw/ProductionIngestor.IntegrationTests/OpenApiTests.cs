using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace ProductionIngestor.IntegrationTests;

public sealed class OpenApiTests
{
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task Document_preserves_versioning_and_authentication(bool production, bool authenticationEnabled)
    {
        if (production)
        {
            await VerifyDocument<ProductionIngestor.Api.Configuration.AzureAdOptions>("ProductionIngestor.Api", authenticationEnabled);
        }
        else
        {
            await VerifyDocument<OutageIngestor.Api.Configuration.AzureAdOptions>("OutageIngestor.Api", authenticationEnabled);
        }
    }

    private static async Task VerifyDocument<TEntryPoint>(string title, bool authenticationEnabled) where TEntryPoint : class
    {
        await using var factory = new WebApplicationFactory<TEntryPoint>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, configuration) => configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["AzureAd:Enabled"] = authenticationEnabled.ToString(),
                ["AzureAd:Instance"] = "https://login.microsoftonline.com/",
                ["AzureAd:TenantId"] = "11111111-1111-1111-1111-111111111111",
                ["AzureAd:ClientId"] = "22222222-2222-2222-2222-222222222222",
                ["AzureAd:Scopes:0"] = "api://test/ingestion.read",
                ["Source:ControlPlaneConnectionString"] = "Host=localhost;Database=openapi_test;Username=test",
                ["JobTrigger:Namespace"] = "openapi-test",
                ["JobTrigger:CronJobName"] = "ingestor"
            }));
        });
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions { BaseAddress = new Uri("https://localhost") });
        using var response = await client.GetAsync("/openapi/v1.json");
        response.EnsureSuccessStatusCode();
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = document.RootElement;
        Assert.Equal(title, root.GetProperty("info").GetProperty("title").GetString());
        var paths = root.GetProperty("paths").EnumerateObject().ToArray();
        Assert.NotEmpty(paths);
        Assert.All(paths, path => Assert.StartsWith("/v1/", path.Name));
        Assert.DoesNotContain(paths, path => path.Name.Contains("{version", StringComparison.OrdinalIgnoreCase));
        if (title == "ProductionIngestor.Api")
        {
            var requestSchemaReference = root.GetProperty("paths").GetProperty("/v1/refresh").GetProperty("post")
                .GetProperty("requestBody").GetProperty("content").GetProperty("application/json")
                .GetProperty("schema").GetProperty("$ref").GetString()!;
            var requestSchemaName = requestSchemaReference.Split('/').Last();
            var requestSchema = root.GetProperty("components").GetProperty("schemas").GetProperty(requestSchemaName);
            var requestProperties = requestSchema.GetProperty("properties").EnumerateObject()
                .Select(property => property.Name).ToHashSet(StringComparer.Ordinal);
            var requiredProperties = requestSchema.GetProperty("required").EnumerateArray()
                .Select(property => property.GetString()).ToHashSet(StringComparer.Ordinal);

            Assert.Contains("startDate", requestProperties);
            Assert.Contains("endDate", requestProperties);
            Assert.Contains("startDate", requiredProperties);
            Assert.Contains("endDate", requiredProperties);
            Assert.DoesNotContain("runId", requestProperties);
            Assert.DoesNotContain("requestedBy", requestProperties);
            Assert.DoesNotContain("dataDates", requestProperties);
        }

        var healthOperation = root.GetProperty("paths").GetProperty("/v1/health").GetProperty("get");
        Assert.False(healthOperation.TryGetProperty("security", out var healthSecurity) && healthSecurity.GetArrayLength() > 0);
        using var health = await client.GetAsync("/v1/health");
        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        var operations = paths.SelectMany(path => path.Value.EnumerateObject())
            .Where(operation => operation.Name is "get" or "post" or "put" or "delete" or "patch")
            .Select(operation => operation.Value).ToArray();
        Assert.NotEmpty(operations);
        var secured = operations.Where(operation => operation.TryGetProperty("security", out var security) && security.GetArrayLength() > 0).ToArray();
        if (authenticationEnabled)
        {
            var flows = root.GetProperty("components").GetProperty("securitySchemes").GetProperty("aad-jwt")
                .GetProperty("flows");
            JsonElement scopes;
            if (title == "ProductionIngestor.Api")
            {
                var authorizationCodeFlow = flows.GetProperty("authorizationCode");
                Assert.EndsWith("/oauth2/v2.0/token", authorizationCodeFlow.GetProperty("tokenUrl").GetString());
                scopes = authorizationCodeFlow.GetProperty("scopes");
            }
            else
            {
                scopes = flows.GetProperty("implicit").GetProperty("scopes");
            }
            Assert.True(scopes.TryGetProperty("api://test/ingestion.read", out _));
            if (title == "ProductionIngestor.Api")
            {
                Assert.True(scopes.TryGetProperty("openid", out _));
                Assert.True(scopes.TryGetProperty("profile", out _));
            }
            Assert.NotEmpty(secured);
            Assert.All(secured, operation => Assert.Equal("api://test/ingestion.read",
                operation.GetProperty("security")[0].GetProperty("aad-jwt")[0].GetString()));
            using var status = await client.GetAsync("/v1/status/test/Production");
            Assert.Equal(HttpStatusCode.Unauthorized, status.StatusCode);
        }
        else
        {
            Assert.Empty(secured);
        }
        using var unknownVersion = await client.GetAsync("/openapi/v999.json");
        Assert.Equal(HttpStatusCode.NotFound, unknownVersion.StatusCode);
        using var swagger = await client.GetAsync("/swagger/index.html");
        Assert.Equal(HttpStatusCode.NotFound, swagger.StatusCode);
    }
}