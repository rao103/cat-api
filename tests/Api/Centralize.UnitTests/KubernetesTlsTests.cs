using System.Net;
using System.Net.Http;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Centralize.Api.Triggering;
using k8s;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Centralize.UnitTests;

public sealed class KubernetesTlsTests
{
    [Theory]
    [InlineData(true, "localhost", true)]
    [InlineData(false, "localhost", false)]
    [InlineData(true, "wrong-host.invalid", false)]
    public async Task Tls_ValidatesConfiguredAuthorityAndHostname_OnRepeatedConnections(
        bool trusted, string certificateHost, bool succeeds)
    {
        using var rootKey = RSA.Create(2048);
        var rootRequest = new CertificateRequest("CN=Test root", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        using var root = rootRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var leafKey = RSA.Create(2048);
        var leafRequest = new CertificateRequest("CN=" + certificateHost, leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName(certificateHost);
        leafRequest.CertificateExtensions.Add(names.Build());
        leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new("1.3.6.1.5.5.7.3.1") }, true));
        using var publicLeaf = leafRequest.Create(root, DateTimeOffset.UtcNow.AddHours(-1),
            DateTimeOffset.UtcNow.AddHours(1), RandomNumberGenerator.GetBytes(16));
        using var leafWithKey = publicLeaf.CopyWithPrivateKey(leafKey);
        using var leaf = X509CertificateLoader.LoadPkcs12(leafWithKey.Export(X509ContentType.Pfx), null,
            X509KeyStorageFlags.Exportable);
        using var wrongKey = RSA.Create(2048);
        using var wrongRoot = new CertificateRequest("CN=Unrelated root", wrongKey, HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0, listen => listen.UseHttps(leaf)));
        await using var app = builder.Build();
        app.MapGet("/", () => Results.Ok());
        await app.StartAsync();
        try
        {
            var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single()
                .Replace("127.0.0.1", "localhost");
            var config = new KubernetesClientConfiguration
            {
                Host = address,
                SslCaCerts = new X509Certificate2Collection(trusted ? root : wrongRoot),
                FirstMessageHandlerSetup = handler => handler.PooledConnectionLifetime = TimeSpan.Zero
            };
            using var client = KubernetesClientFactory.Create(config);
            for (var attempt = 0; attempt < 3; attempt++)
            {
                if (succeeds)
                {
                    using var response = await client.HttpClient.GetAsync(address);
                    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                }
                else
                {
                    var error = await Assert.ThrowsAsync<HttpRequestException>(() => client.HttpClient.GetAsync(address));
                    Assert.IsType<AuthenticationException>(error.InnerException);
                }
            }
        }
        finally { await app.StopAsync(); }
    }
}
