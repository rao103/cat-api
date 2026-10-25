using Centralize.Api.Configuration;
using Centralize.Api.Security;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Versioning;
using Microsoft.Identity.Web;

namespace Centralize.Api.Configurators;

internal static class ServicesConfigurator
{
    public static IWebHostBuilder ConfigureServices(this IWebHostBuilder builder) =>
        builder.ConfigureServices(ConfigureServices);

    private static void ConfigureServices(WebHostBuilderContext context, IServiceCollection services)
    {
        services.AddOptions<AzureAdOptions>()
            .Bind(context.Configuration.GetSection(AzureAdOptions.SectionName))
            .Validate(options => !options.Enabled ||
                (!string.IsNullOrWhiteSpace(options.Instance) &&
                 !string.IsNullOrWhiteSpace(options.TenantId) &&
                 !string.IsNullOrWhiteSpace(options.ClientId)),
                "AzureAd Instance, TenantId and ClientId are required when authentication is enabled.")
            .ValidateOnStart();

        services.AddSingleton(typeof(Interfaces.ITelemetry<>), typeof(Monitoring.Telemetry<>));
        ProductionServicesConfigurator.Configure(context, services);

        services.AddControllers();
        services.AddApiVersioning(options =>
        {
            options.DefaultApiVersion = new ApiVersion(1, 0);
            options.AssumeDefaultVersionWhenUnspecified = true;
            options.ApiVersionReader = new UrlSegmentApiVersionReader();
        });
        services.AddVersionedApiExplorer(options =>
        {
            options.GroupNameFormat = "'v'VVV";
            options.SubstituteApiVersionInUrl = true;
        });
        services.AddEndpointsApiExplorer();
        services.AddOpenApiDocuments();

        var azureAd = context.Configuration.GetSection(AzureAdOptions.SectionName).Get<AzureAdOptions>();
        if (azureAd?.Enabled == true)
        {
            services.AddAuthentication(auth =>
                {
                    auth.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                    auth.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
                    auth.DefaultForbidScheme = JwtBearerDefaults.AuthenticationScheme;
                })
                .AddMicrosoftIdentityWebApi(context.Configuration.GetSection(AzureAdOptions.SectionName));
        }
        else if (context.HostingEnvironment.IsDevelopment())
        {
            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = DevelopmentAuthenticationHandler.SchemeName;
                options.DefaultChallengeScheme = DevelopmentAuthenticationHandler.SchemeName;
                options.DefaultForbidScheme = DevelopmentAuthenticationHandler.SchemeName;
            })
           .AddScheme<AuthenticationSchemeOptions, DevelopmentAuthenticationHandler>(
            DevelopmentAuthenticationHandler.SchemeName, _ => { });
        }
        else
        {
            throw new InvalidOperationException("AzureAd authentication must be enabled outside Development.");
        }

        services.AddAuthorization();
    }
}
