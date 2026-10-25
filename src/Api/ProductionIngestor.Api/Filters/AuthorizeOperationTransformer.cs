using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;
using ProductionIngestor.Api.Configuration;

namespace ProductionIngestor.Api.Filters;

internal sealed class AuthorizeOperationTransformer(IOptions<AzureAdOptions> azureAdOptions) : IOpenApiOperationTransformer
{
    public Task TransformAsync(OpenApiOperation operation, OpenApiOperationTransformerContext context, CancellationToken cancellationToken)
    {
        if (!azureAdOptions.Value.Enabled)
        {
            return Task.CompletedTask;
        }

        var metadata = context.Description.ActionDescriptor.EndpointMetadata;
        if (metadata.OfType<AllowAnonymousAttribute>().Any())
        {
            return Task.CompletedTask;
        }

        if (!metadata.OfType<AuthorizeAttribute>().Any())
        {
            return Task.CompletedTask;
        }

        operation.Responses ??= new OpenApiResponses();
        operation.Responses.TryAdd("401", new OpenApiResponse { Description = "Unauthorized" });
        operation.Responses.TryAdd("403", new OpenApiResponse { Description = "Forbidden" });

        var jwtBearerScheme = new OpenApiSecuritySchemeReference("aad-jwt", context.Document);
        var scopes = azureAdOptions.Value.Scopes.ToList();
        operation.Security =
        [
            new OpenApiSecurityRequirement { [jwtBearerScheme] = scopes }
        ];

        return Task.CompletedTask;
    }
}
