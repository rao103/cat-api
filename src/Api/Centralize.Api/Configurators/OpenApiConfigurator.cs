using Centralize.Api.Configuration;
using Centralize.Api.Filters;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;

namespace Centralize.Api.Configurators;

internal static class OpenApiConfigurator
{
    public static IServiceCollection AddOpenApiDocuments(this IServiceCollection services)
    {
        services.AddOpenApi("v1", options =>
        {
            options.AddDocumentTransformer((document, _, _) =>
            {
                document.Info = new OpenApiInfo
                {
                    Title = "Centralize.Api",
                    Version = "1.0"
                };
                return Task.CompletedTask;
            });

            options.AddDocumentTransformer((document, transformerContext, _) =>
            {
                var azureAd = transformerContext.ApplicationServices.GetRequiredService<IOptions<AzureAdOptions>>().Value;
                if (azureAd.Enabled)
                {
                    document.Components ??= new OpenApiComponents();
                    document.Components.SecuritySchemes ??= new Dictionary<string, IOpenApiSecurityScheme>();
                    document.Components.SecuritySchemes["aad-jwt"] = new OpenApiSecurityScheme
                    {
                        Type = SecuritySchemeType.OAuth2,
                        Flows = new OpenApiOAuthFlows
                        {
                            AuthorizationCode = new OpenApiOAuthFlow
                            {
                                AuthorizationUrl = new Uri($"{azureAd.Instance}{azureAd.TenantId}/oauth2/v2.0/authorize"),
                                TokenUrl = new Uri($"{azureAd.Instance}{azureAd.TenantId}/oauth2/v2.0/token"),
                                Scopes = azureAd.Scopes
                                    .Append("openid")
                                    .Append("profile")
                                    .Distinct(StringComparer.Ordinal)
                                    .ToDictionary(scope => scope,
                                        scope => scope switch
                                        {
                                            "openid" => "Sign in the user",
                                            "profile" => "Read the signed-in user's profile claims",
                                            _ => scope
                                        })
                            }
                        }
                    };
                }
                return Task.CompletedTask;
            });

            options.AddOperationTransformer<AuthorizeOperationTransformer>();
        });

        return services;
    }
}
