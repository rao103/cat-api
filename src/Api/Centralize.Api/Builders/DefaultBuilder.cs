using Centralize.Api.Configurators;
using Centralize.Api.Configuration;
using Scalar.AspNetCore;

namespace Centralize.Api.Builders;

/// <summary>A default provided class that is being used for building the WebApplication.</summary>
public static class DefaultBuilder
{
    public static async Task ExecuteAsync(this WebApplicationBuilder builder, CancellationToken cancellationToken = default)
    {
        builder.WebHost
            .ConfigureAppConfiguration()
            .ConfigureServices()
            .ConfigureLogging();

        builder.ConfigureTelemetry();

        var azureAd = builder.Configuration.GetSection(AzureAdOptions.SectionName).Get<AzureAdOptions>();

        var app = builder.Build();

        app.UseHttpsRedirection();

        app.UseAuthentication();
        app.UseAuthorization();

        app.MapControllers();
        app.MapOpenApi();

        if (app.Environment.IsDevelopment())
        {
            app.MapScalarApiReference(options =>
            {
                if (azureAd?.Enabled == true)
                {
                    options
                        .AddPreferredSecuritySchemes("aad-jwt")
                        .AddAuthorizationCodeFlow("aad-jwt", flow =>
                        {
                            flow.ClientId = azureAd.ClientId;
                            flow.Pkce = Pkce.Sha256;
                            flow.SelectedScopes = azureAd.Scopes
                                .Append("openid")
                                .Append("profile")
                                .Distinct(StringComparer.Ordinal)
                                .ToArray();
                        });
                }
            });
        }

        await app.RunAsync(cancellationToken);
    }
}
