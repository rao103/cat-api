using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using k8s;

namespace Centralize.Api.Triggering;

public static class KubernetesClientFactory
{
    public static Kubernetes Create(KubernetesClientConfiguration configuration)
    {
        SocketsHttpHandler? transport = null;
        var setup = configuration.FirstMessageHandlerSetup;
        configuration.FirstMessageHandlerSetup = handler =>
        {
            setup?.Invoke(handler);
            transport = handler;
        };
        Kubernetes client;
        try { client = new Kubernetes(configuration); }
        finally { configuration.FirstMessageHandlerSetup = setup; }
        if (!configuration.SkipTlsVerify && configuration.SslCaCerts is { Count: > 0 })
        {
            // Use native TLS validation; the legacy callback fails on repeated Windows connections.
            var policy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                RevocationMode = X509RevocationMode.NoCheck
            };
            policy.CustomTrustStore.AddRange(configuration.SslCaCerts);
            policy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
            transport!.SslOptions.CertificateChainPolicy = policy;
            transport.SslOptions.RemoteCertificateValidationCallback = null;
        }
        return client;
    }
}
