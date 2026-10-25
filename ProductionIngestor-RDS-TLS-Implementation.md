# Production Ingestor — AWS RDS PostgreSQL TLS Certificate Implementation

## Objective

Implement Amazon RDS PostgreSQL server-certificate validation for the **Production Ingestor** so that:

1. The application uses the official Amazon RDS CA bundle.
2. EKS receives the PEM through a Kubernetes **ConfigMap**, mounted read-only into the pod.
3. The C# application uses `SSL Mode=VerifyFull` and the mounted CA bundle.
4. The same CA bundle is available when debugging locally from Visual Studio.
5. IAM database authentication remains unchanged: the IAM token is the temporary database password.
6. No PEM is stored in AWS Secrets Manager because the RDS CA bundle is public trust material, not a credential.

This task is about **TLS trust only**. Do not replace or weaken the existing AWS SSO / IAM token / EKS workload identity design.

---

## Required final architecture

### Local development

Preferred supported local-debug path:

```text
Visual Studio
    ↓ F5 using WSL launch profile
ProductionIngestor running in WSL
    ↓
real-rds-endpoint:LOCAL_TUNNEL_PORT
    ↓ /etc/hosts resolves real RDS hostname to 127.0.0.1
SSM tunnel
    ↓
EC2 bastion
    ↓
real-rds-endpoint:5432
```

TLS validation must still use the **real RDS hostname**, so `VerifyFull` can validate the hostname in the server certificate.

For local debugging:

```text
ConnectionPort = local SSM tunnel port, e.g. 55432
IamTokenPort   = real RDS port, normally 5432
Host           = real RDS endpoint hostname
Root CA        = Certificates/global-bundle.pem
SSL Mode       = VerifyFull
```

### EKS

```text
ProductionIngestor pod
    ↓ EKS Pod Identity / IRSA
IAM role
    ↓ IAM DB auth token
Npgsql
    ↓ TLS + VerifyFull
real-rds-endpoint:5432
    ↓
RDS PostgreSQL
```

For EKS:

```text
ConnectionPort = 5432
IamTokenPort   = 5432
Host           = real RDS endpoint hostname
Root CA        = /app/certs/rds/global-bundle.pem
SSL Mode       = VerifyFull
```

There must be no SSM tunnel, bastion dependency, `/etc/hosts` modification, developer AWS profile, or browser SSO inside the EKS workload.

---

# Implementation instructions for Copilot

## 0. Inspect before editing

Before making changes:

1. Locate the actual `ProductionIngestor.csproj`.
2. Locate its current database configuration/options class.
3. Locate where `NpgsqlDataSource`, `NpgsqlConnection`, or the PostgreSQL connection string is created.
4. Locate the existing Kustomize structure for Production Ingestor, especially:
   - `deploy/ProductionIngestor/base/`
   - `kustomization.yaml`
   - Deployment manifest
   - existing application ConfigMap(s)
   - environment overlays
5. Preserve the current repository naming conventions.
6. Do not create duplicate ConfigMaps/options/classes if equivalent resources already exist; extend the existing implementation cleanly.

If actual paths differ from the examples below, adapt the paths while preserving the design.

---

## 1. Download the official Amazon RDS CA bundle

Use the AWS-published global RDS CA bundle:

```text
https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

Use this bundle rather than generating a self-signed certificate.

The canonical repository copy for this application should be:

```text
deploy/ProductionIngestor/base/certs/global-bundle.pem
```

Create the directory if required.

### Add a PowerShell refresh script

Create:

```text
scripts/update-rds-ca.ps1
```

It should:

1. Determine the repository root based on the script location; do not depend on the caller's current directory.
2. Download the official global bundle to a temporary file.
3. Fail if HTTP download fails.
4. Fail if the downloaded file is empty.
5. Fail if it does not contain `-----BEGIN CERTIFICATE-----`.
6. Replace `deploy/ProductionIngestor/base/certs/global-bundle.pem` only after validation succeeds.
7. Print the SHA-256 hash after successful update.
8. Never disable TLS validation in order to perform the download.

Suggested behavior:

```powershell
./scripts/update-rds-ca.ps1
```

The script should be idempotent.

Do not put the PEM in Secrets Manager.

---

## 2. Use Kustomize to generate a dedicated certificate ConfigMap

Prefer a dedicated ConfigMap for certificate material rather than embedding the large PEM directly into the Deployment manifest.

Update the Production Ingestor base `kustomization.yaml` with a `configMapGenerator` similar to:

```yaml
configMapGenerator:
  - name: production-ingestor-rds-ca
    files:
      - global-bundle.pem=certs/global-bundle.pem
```

Do **not** set `disableNameSuffixHash: true` unless the repository already has a deliberate convention requiring it.

The normal Kustomize hash suffix is desirable because a changed PEM produces a new ConfigMap name and therefore a Deployment rollout when the generated name reference changes.

If an equivalent certificate ConfigMap generator already exists, update it instead of adding another one.

---

## 3. Mount the certificate ConfigMap in the Deployment

Update the Production Ingestor Deployment base manifest.

Add a read-only ConfigMap volume:

```yaml
volumes:
  - name: rds-ca
    configMap:
      name: production-ingestor-rds-ca
```

Add a read-only mount to the Production Ingestor container:

```yaml
volumeMounts:
  - name: rds-ca
    mountPath: /app/certs/rds
    readOnly: true
```

The resulting file inside the container must be:

```text
/app/certs/rds/global-bundle.pem
```

Do not mount it over the entire `/app` directory.

If the Deployment already has `volumes` or `volumeMounts`, merge into the existing lists without removing current entries.

---

## 4. Add application configuration for the root certificate

Extend the application's database configuration model with an explicit root-certificate path.

Use a property equivalent to:

```csharp
public string RootCertificatePath { get; init; } = string.Empty;
```

Retain separate concepts for:

```text
Host
ConnectionPort
IamTokenPort
Database
Username
Region
RootCertificatePath
```

Do not collapse `ConnectionPort` and `IamTokenPort` into one setting because local SSM debugging requires:

```text
ConnectionPort = 55432
IamTokenPort   = 5432
```

while EKS normally uses:

```text
ConnectionPort = 5432
IamTokenPort   = 5432
```

---

## 5. Configure the EKS value

In the appropriate base ConfigMap or environment configuration used by the Production Ingestor, set:

```text
Database__RootCertificatePath=/app/certs/rds/global-bundle.pem
```

Use the repository's existing .NET configuration naming convention if it differs.

Do not put the PEM contents into the same application-settings ConfigMap if the dedicated generated ConfigMap from step 2 is being used. The settings ConfigMap contains the **path**; the certificate ConfigMap contains the **file**.

Production/EKS database settings should conceptually resolve to:

```text
Database__Host=<actual RDS endpoint>
Database__ConnectionPort=5432
Database__IamTokenPort=5432
Database__Database=<database name>
Database__Username=production_ingestor
Database__Region=eu-north-1
Database__RootCertificatePath=/app/certs/rds/global-bundle.pem
```

Do not store a PostgreSQL password.

---

## 6. Configure Npgsql for full server verification

Find the central place where the PostgreSQL connection string or `NpgsqlDataSource` is created.

Configure Npgsql with:

```text
SSL Mode = VerifyFull
Root Certificate = resolved path to global-bundle.pem
```

Do not use:

```text
SSL Mode=Require
Trust Server Certificate=true
```

Those settings do not provide the hostname + CA validation required here.

If using `NpgsqlConnectionStringBuilder`, the intended result is equivalent to:

```csharp
var connectionStringBuilder = new NpgsqlConnectionStringBuilder
{
    Host = options.Host,
    Port = options.ConnectionPort,
    Database = options.Database,
    Username = options.Username,
    SslMode = SslMode.VerifyFull,
    RootCertificate = ResolveRootCertificatePath(options.RootCertificatePath)
};
```

Preserve the existing IAM password/token provider implementation.

The IAM token must continue to be generated using:

```text
Host = actual RDS endpoint
Port = IamTokenPort (normally 5432)
User = configured PostgreSQL username
```

Do **not** generate the IAM token against local port `55432`.

Do not log the IAM token.

---

## 7. Resolve relative certificate paths safely

Implement one small helper near the database composition/bootstrap code.

Required behavior:

```text
If RootCertificatePath is absolute:
    use it unchanged.

If RootCertificatePath is relative:
    resolve it relative to AppContext.BaseDirectory.
```

Conceptual implementation:

```csharp
private static string ResolveRootCertificatePath(string configuredPath)
{
    if (string.IsNullOrWhiteSpace(configuredPath))
        throw new InvalidOperationException("Database root certificate path is not configured.");

    var path = Path.IsPathRooted(configuredPath)
        ? configuredPath
        : Path.Combine(AppContext.BaseDirectory, configuredPath);

    path = Path.GetFullPath(path);

    if (!File.Exists(path))
        throw new FileNotFoundException(
            $"RDS root certificate was not found at '{path}'.",
            path);

    return path;
}
```

Fail fast if the file is absent. Do not silently downgrade from `VerifyFull` to `Require`.

---

## 8. Make the same PEM available for Visual Studio local debugging

The repository must have **one authoritative PEM file**, the canonical copy from step 1:

```text
deploy/ProductionIngestor/base/certs/global-bundle.pem
```

Do not manually maintain a second independent copy.

Update `ProductionIngestor.csproj` so the canonical PEM is linked into the project and copied into the build output as:

```text
Certificates/global-bundle.pem
```

Use an MSBuild item equivalent to:

```xml
<ItemGroup>
  <Content Include="<relative-path-to-deploy-base>/certs/global-bundle.pem">
    <Link>Certificates/global-bundle.pem</Link>
    <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
  </Content>
</ItemGroup>
```

Copilot must calculate the correct relative path based on the actual location of the `.csproj`; do not blindly paste the placeholder path above.

This ensures that when Visual Studio builds locally, the file exists under:

```text
<AppContext.BaseDirectory>/Certificates/global-bundle.pem
```

---

## 9. Local development configuration

Update `appsettings.Development.json`, the local launch profile, or the repository's existing local configuration mechanism.

Do not hard-code environment-specific RDS hostnames into committed files if the repository currently injects them from launch settings/user secrets/environment variables. Follow the existing convention.

The local settings must conceptually be:

```json
{
  "Database": {
    "Host": "<real-rds-endpoint>.eu-north-1.rds.amazonaws.com",
    "ConnectionPort": 55432,
    "IamTokenPort": 5432,
    "Database": "<database-name>",
    "Username": "<iam-enabled-db-user>",
    "Region": "eu-north-1",
    "RootCertificatePath": "Certificates/global-bundle.pem"
  }
}
```

### Important local networking rule

For the existing company SSM-tunnel approach, keep using the **real RDS hostname** in the C# configuration.

Do not change local `Host` to `localhost` if `VerifyFull` is enabled.

Before F5 debugging, the developer should run from WSL:

```bash
./dbconnect.sh tunnel <target>
```

That existing script is responsible for:

```text
AWS SSO
→ resolving the bastion
→ mapping the real RDS endpoint to 127.0.0.1 in WSL /etc/hosts
→ opening local port 55432 (or the target's configured local port)
→ forwarding to the real RDS endpoint on 5432
```

Then run/debug the Production Ingestor from Visual Studio using the WSL launch profile so that it shares the WSL hostname mapping and tunnel environment.

If this repository already supports a different local network flow, retain it, but do not weaken hostname verification.

---

## 10. Preserve IAM authentication behavior

This task must not introduce a stored DB password.

Local expected identity flow:

```text
Developer AWS SSO profile
    ↓
Assume configured DB IAM role if required
    ↓
Generate RDS IAM authentication token
    ↓
Npgsql password provider
```

EKS expected identity flow:

```text
EKS Pod Identity / IRSA
    ↓
workload IAM role
    ↓
rds-db:connect
    ↓
Generate RDS IAM authentication token
    ↓
Npgsql password provider
```

If the existing code already uses `NpgsqlDataSourceBuilder.UsePeriodicPasswordProvider(...)`, preserve it.

If the current implementation generates an IAM token only once for a long-running worker, refactor it so newly created physical connections can receive refreshed IAM tokens. Do not make certificate work depend on a permanently cached IAM token.

---

# Kustomize target state

The base should end up conceptually similar to:

```text
deploy/
  ProductionIngestor/
    base/
      certs/
        global-bundle.pem
      deployment.yaml
      kustomization.yaml
      ...existing resources...
    overlays/
      dev/
      test/
      preprod/
      prod/
```

Example `kustomization.yaml` fragment:

```yaml
resources:
  - deployment.yaml
  # preserve existing resources

configMapGenerator:
  - name: production-ingestor-rds-ca
    files:
      - global-bundle.pem=certs/global-bundle.pem
```

Example Deployment fragment:

```yaml
spec:
  template:
    spec:
      containers:
        - name: production-ingestor
          volumeMounts:
            - name: rds-ca
              mountPath: /app/certs/rds
              readOnly: true
      volumes:
        - name: rds-ca
          configMap:
            name: production-ingestor-rds-ca
```

Merge this into the existing Deployment instead of replacing unrelated configuration.

---

# Docker requirements

The Production Ingestor container does not need to download the CA bundle at runtime.

Do not add runtime `curl`/`wget` solely for certificate retrieval.

The certificate is supplied by Kubernetes at:

```text
/app/certs/rds/global-bundle.pem
```

The container should run as its existing non-root user if already configured that way. Ensure that the ConfigMap-mounted file is readable by the application process.

---

# Validation and tests

## A. Repository validation

After implementation, run the repository's normal build/test commands plus:

```text
1. Verify deploy/ProductionIngestor/base/certs/global-bundle.pem exists.
2. Verify it contains one or more BEGIN CERTIFICATE blocks.
3. Verify the C# build output contains Certificates/global-bundle.pem.
4. Verify no password or IAM token was committed.
```

The PowerShell refresh script should also print the downloaded file's SHA-256.

---

## B. Kustomize validation

Render every relevant environment before committing:

```bash
kubectl kustomize deploy/ProductionIngestor/overlays/dev
kubectl kustomize deploy/ProductionIngestor/overlays/test
kubectl kustomize deploy/ProductionIngestor/overlays/preprod
kubectl kustomize deploy/ProductionIngestor/overlays/prod
```

Use only overlays that actually exist in the repository.

Verify the rendered Deployment:

1. Contains a ConfigMap volume.
2. References the generated certificate ConfigMap.
3. Mounts it at `/app/certs/rds`.
4. Configures `Database__RootCertificatePath=/app/certs/rds/global-bundle.pem` through the existing configuration mechanism.
5. Does not contain a database password.

---

## C. Local Visual Studio test

1. Run the existing tunnel:

```bash
./dbconnect.sh tunnel <dev-target>
```

2. Confirm the real RDS hostname resolves to `127.0.0.1` inside the WSL distribution used by Visual Studio.
3. Confirm the configured local tunnel port is listening.
4. Start Production Ingestor using the Visual Studio WSL launch profile.
5. Verify the application successfully opens a PostgreSQL connection using IAM authentication.
6. Execute a harmless query such as:

```sql
SELECT 1;
```

7. Confirm TLS is active:

```sql
SELECT ssl, version, cipher
FROM pg_stat_ssl
WHERE pid = pg_backend_pid();
```

Expected:

```text
ssl = true
```

8. Temporarily point `RootCertificatePath` to an invalid/test PEM and verify connection establishment fails.
9. Restore the valid AWS RDS bundle.

The invalid-certificate test is important: it proves the application is actually validating the RDS certificate instead of merely encrypting traffic.

---

## D. EKS dev-environment validation

Deploy to Dev first.

Verify the mounted file, using the repository/team's permitted Kubernetes troubleshooting process:

```bash
kubectl exec <production-ingestor-pod> -- ls -l /app/certs/rds/
```

Expected:

```text
global-bundle.pem
```

If shell access is restricted, verify through Deployment/Pod spec inspection instead.

Confirm application startup does not report certificate errors.

Confirm a DB connection succeeds and logs indicate only safe connection metadata such as host/database/user/SSL mode. Never log the IAM token.

If practical, use the same `pg_stat_ssl` query in a health/integration test to prove TLS is active.

---

# Security requirements

The implementation must satisfy all of the following:

- [ ] Use the official AWS RDS CA bundle.
- [ ] Do not generate a self-signed CA.
- [ ] Do not store the CA bundle in Secrets Manager.
- [ ] Do not store PostgreSQL passwords.
- [ ] Do not commit IAM tokens.
- [ ] Do not log IAM tokens.
- [ ] Use `SSL Mode=VerifyFull`.
- [ ] Do not use `Trust Server Certificate=true`.
- [ ] Do not silently fall back to weaker SSL behavior when the PEM is missing.
- [ ] Use the real AWS RDS endpoint hostname for certificate hostname verification.
- [ ] Use the real RDS endpoint and real RDS port when generating the IAM token.
- [ ] Mount the certificate ConfigMap read-only in EKS.
- [ ] Keep developer SSO credentials out of the EKS workload.
- [ ] Keep production AWS authentication on EKS Pod Identity / IRSA.

---

# Certificate maintenance

The CA bundle is public material but must still be maintained deliberately.

The source of truth is AWS:

```text
https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

To refresh the repository copy:

```powershell
./scripts/update-rds-ca.ps1
```

Commit the changed PEM through the normal pull-request process.

Because Kustomize generates a ConfigMap hash from its contents, changing the PEM should result in a new generated ConfigMap name and a Deployment rollout when references are transformed normally.

Do not build an application feature that downloads certificates from the internet every time a pod starts.

---

# Expected changed/created files

Copilot should adapt names to the actual repository but the final change set should be approximately:

```text
scripts/
  update-rds-ca.ps1                         NEW

deploy/ProductionIngestor/base/
  certs/
    global-bundle.pem                       NEW
  kustomization.yaml                        UPDATE
  deployment.yaml                           UPDATE
  <existing-configmap-or-config-files>      UPDATE if needed

src/.../ProductionIngestor/
  ProductionIngestor.csproj                 UPDATE
  <database-options-class>.cs               UPDATE
  <database-registration/bootstrap>.cs      UPDATE
  appsettings.Development.json              UPDATE only if consistent with repo policy
  Properties/launchSettings.json            UPDATE only if required by existing local-debug design
```

Do not rename or reorganize unrelated files as part of this task.

---

# Acceptance criteria

The task is complete only when all of these are true:

1. `global-bundle.pem` is downloaded from the official AWS RDS trust-store endpoint.
2. There is a repeatable PowerShell command to refresh it.
3. Kustomize creates a ConfigMap from the PEM.
4. The Production Ingestor pod mounts that ConfigMap read-only.
5. EKS config points `RootCertificatePath` to `/app/certs/rds/global-bundle.pem`.
6. Visual Studio local builds receive the same canonical PEM as `Certificates/global-bundle.pem` without maintaining an independent copy.
7. Npgsql uses `SslMode.VerifyFull`.
8. Npgsql is explicitly given the CA bundle through `RootCertificate` or the repository's equivalent supported mechanism.
9. Missing/invalid certificate fails the connection rather than downgrading verification.
10. Local debugging through the existing SSM tunnel succeeds.
11. Local IAM token generation continues to use the real RDS endpoint and port `5432`, not the local tunnel port.
12. EKS connects directly to the RDS endpoint on `5432`.
13. No DB password, AWS access key, IAM token, or private key is introduced.
14. Existing unit/integration tests pass.
15. All Kustomize overlays render successfully.

---

# Official references

Use these as implementation authorities:

- Amazon RDS SSL/TLS certificate bundles:
  `https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html`
- AWS global RDS CA bundle:
  `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`
- Amazon RDS PostgreSQL SSL/TLS verification:
  `https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.SSL.html`
- RDS PostgreSQL IAM authentication with `verify-full`:
  `https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.AWSCLI.PostgreSQL.html`
- Npgsql security/TLS modes:
  `https://www.npgsql.org/doc/security.html`
- Npgsql connection-string parameters:
  `https://www.npgsql.org/doc/connection-string-parameters`

---

# Copilot execution instruction

Implement this specification end-to-end in the existing repository.

Before editing, inspect the current Production Ingestor C# project, Npgsql setup, Kustomize manifests, ConfigMaps, overlays, Dockerfile, and local launch configuration. Reuse existing abstractions and naming conventions rather than creating parallel implementations.

After editing:

1. Download/refresh the official PEM using the new script.
2. Build the Production Ingestor.
3. Run relevant tests.
4. Render each existing Kustomize overlay.
5. Report every file changed.
6. Report any assumption you had to make.
7. Do not claim the local/EKS database connection was validated unless an actual connection test was successfully executed.
