# CAT database connections

All connection settings live in the Worker's
[appsettings.json](../src/Raw/Ingestors/Production/ProductionIngestor.Worker/appsettings.json).
`ControlPlane` holds PostgreSQL settings; `ControlPlane:LocalConnection` holds
Windows SSO/tunnel settings. There is no separate connection JSON file.

| Path | Network | Authentication |
|---|---|---|
| Windows Visual Studio | SSM tunnel through CAT bastion, local port 55434 | AWS SSO and optional database role |
| EKS Worker | Direct private RDS endpoint, port 5432 | EKS Pod Identity |
| Azure SQL source | Existing source network path | Existing Microsoft Entra service principal |

RDS target: `cat-dev-pg.cpggeso4i2wb.eu-north-1.rds.amazonaws.com:5432`, database
`cat_dev`, account `902707423845`, region `eu-north-1`. ProductionIngestor is the
workload name; this database is in the development environment.

## Fill the missing settings

Use actual CAT product outputs and the database/infrastructure owner's values:

| Worker setting | Required value |
|---|---|
| `ControlPlane:Username` | Approved PostgreSQL login with `rds_iam` and cursor privileges |
| `ControlPlane:LocalConnection:SsoStartUrl` | Confirmed organization SSO portal URL |
| `ControlPlane:LocalConnection:BastionInstanceId` | Running CAT SSM-managed EC2 instance ID |
| `ControlPlane:LocalConnection:DatabaseRoleArn` | Approved role the local SSO identity may assume |

If the SSO identity has direct database access, leave `DatabaseRoleArn` empty and
set `DatabaseProfile` equal to `SsoProfile`. Otherwise keep separate profiles.
Do not infer these values from Orqis. `orstedrds` is the master username, not the
default application IAM user; the provisioning SSO identity is not a DB username.
Product `pp-xwcn5upby7u3g` must finish provisioning with RDS IAM auth enabled.

The script reads the selected JSON file, not .NET's merged environment/user-secret
configuration. Its default is the Worker file above; `-ConfigPath` can select another
complete appsettings file. Tokens and secrets must never be written into that file.

Unlike the Orqis WSL helper, this CAT PowerShell script does not install AWS CLI, the
Session Manager plugin, or `psql`, and it does not discover a bastion from a Name tag.
Install those tools separately and provide the actual CAT infrastructure values in the
Worker settings before running `configure`.

## First-time Windows setup

Use native Windows PowerShell 5.1 or PowerShell 7. WSL tools, hosts mappings, profiles,
and SSO cache do not configure Windows Visual Studio.

1. Install [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
   the [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/install-plugin-windows.html),
   and optionally [psql](https://www.postgresql.org/download/windows/) for verification.
   Reopen PowerShell after PATH changes.
2. Install the official regional CA bundle (or obtain it through your approved
   internal distribution route):

   ```powershell
   New-Item -ItemType Directory -Force "$env:USERPROFILE\.aws\rds" | Out-Null
   Invoke-WebRequest -UseBasicParsing -Uri 'https://truststore.pki.rds.amazonaws.com/eu-north-1/eu-north-1-bundle.pem' -OutFile "$env:USERPROFILE\.aws\rds\eu-north-1-bundle.pem"
   ```

   `LocalConnection:RootCertificate` accepts `%USERPROFILE%`; `-RootCertificate`
   overrides it. The runtime `ControlPlane:RootCertificate` needs a resolved path.
   Maintain the bundle through CA rotations; never disable certificate verification.
3. Fill the settings above, then run the prerequisite check from this folder:

  ```powershell
  ./dbconnect.ps1 check
  ```

  If the CA bundle is outside the configured path, pass it explicitly:

   ```powershell
   ./dbconnect.ps1 check -RootCertificate "$env:USERPROFILE\.aws\rds\eu-north-1-bundle.pem"
   ```

   `check` does not contact AWS or PostgreSQL. It confirms the target settings, local
   tools, CA file, SSO values, bastion ID, and database username. Fix every reported
   prerequisite before continuing.
4. Using your own elevated editor, add this line to the **Windows** hosts file
   at `C:\Windows\System32\drivers\etc\hosts`:

   ```text
   127.0.0.1 cat-dev-pg.cpggeso4i2wb.eu-north-1.rds.amazonaws.com
   ```

   Preserve unrelated entries and remove conflicting mappings for this hostname.
   Remove this exact line when returning to direct/VPN access: it affects every
   Windows client using this hostname. The script never elevates or edits this file.
5. Configure the AWS profiles and complete the browser/device-code login:

  ```powershell
  ./dbconnect.ps1 configure
  ./dbconnect.ps1 login
  ```

  `configure` writes non-secret AWS profile values, refusing conflicting values.
  Use fresh profile names if existing profiles use another authentication mechanism.

## Daily Visual Studio debugging

In the first terminal:

```powershell
./dbconnect.ps1 login
./dbconnect.ps1 tunnel
```

Wait for SSM to report the port is open. Leave this terminal open. **Ctrl+C stops
the tunnel**; there is no background daemon. An occupied port is rejected, not
assumed to belong to a valid tunnel. Change `LocalConnection:Port` if necessary.

In a second terminal in this folder:

```powershell
./dbconnect.ps1 verify
./dbconnect.ps1 debug-env
```

`verify` runs a read-only `psql` query with TLS `verify-full`, printing the database,
user, and TLS status. It never prints the token. The temporary `PGPASSWORD` is
restored/removed afterward. Do not run credential commands with shell tracing or
capture process environments. This verifies the CLI path, not the .NET or EKS path.

`debug-env` reads appsettings and sets non-secret `ControlPlane__*` variables in
the current PowerShell process, overriding only what Windows needs: local port,
database profile, and CA path. **Start a new Visual Studio process from this same
terminal**; an existing instance cannot inherit the new environment. In a VS
Developer PowerShell, for example:

```powershell
devenv.exe ..\O.WP.CAT.LossCalculation.Platform.sln
```

Choose `ProductionIngestor.Worker` as startup project. Source SQL, Blob, ingestion
request, and SQS settings are still required; see the [main README](../README.md).
Starting the Worker performs actual ingestion and can publish messages and advance
cursors. Do not use ingestion as a connection smoke test.

If the CA bundle is not at the path in `LocalConnection:RootCertificate`, pass the
same `-RootCertificate` override to `debug-env`:

```powershell
./dbconnect.ps1 debug-env -RootCertificate "$env:USERPROFILE\.aws\rds\eu-north-1-bundle.pem"
```

`debug-env` changes only the current PowerShell process. Start Visual Studio from
that same terminal; an already-running Visual Studio instance will not inherit the
settings. The script deliberately does not set a global `AWS_PROFILE`; the database
profile is isolated in `ControlPlane__AwsProfile` and other AWS clients keep their
normal credential chain.

Database authentication uses `ControlPlane__AwsProfile`, leaving other AWS clients
unchanged. For local SQS access, set `$env:AWS_PROFILE='cat-dev'` before launching
Visual Studio only if that identity has queue permissions. Remove stale AWS static
credential environment overrides. Re-run `login` when the SSO session expires.

## EKS setup

The pod uses direct private RDS access: no SSM, hosts override, developer profile,
or static AWS keys. Keep `ControlPlane__AwsProfile` unset/empty. The Worker ignores
`LocalConnection`; the script selects its database profile only for Windows.

Infrastructure/database owners must provide:

- A confirmed service account/Pod Identity association and supported agent/compute.
  Preserve the existing [SQS role and permissions](../README.md#eks-pod-identity).
  Do not replace it with a database-only role that breaks publication.
- `rds-db:connect` on the exact resource
  `arn:aws:rds-db:eu-north-1:902707423845:dbuser:<actual-DbiResourceId>/<approved-db-login>`.
  Use the real RDS resource ID (`db-...`), not the Service Catalog ID, endpoint, or
  DB instance ARN. Local role assumption additionally needs restricted role trust
  and the SSO identity's `sts:AssumeRole` permission.
- IAM database authentication enabled, a PostgreSQL LOGIN with `rds_iam`, database
  `CONNECT`, schema `USAGE`, and `SELECT`/`UPDATE` on `extraction_cursor`. No master,
  superuser, role-management, or DDL permission is needed by the application.
  Schema creation and cursor-row initialization remain deployment/admin operations.
- Private routing/DNS, RDS TCP 5432 security-group access restricted to the approved
  workload source, and compatible pod security groups, network policies and NACLs.
  The local bastion independently needs access to RDS; SSM grants no database rights.
- A trusted RDS CA bundle distributed through the approved image/config process and
  mounted read-only, for example `/etc/rds/eu-north-1-bundle.pem`. Do not download
  trust roots dynamically in the pod.

Set these variables on the **existing CronJob worker container**, preserving its
service account and other configuration. API-triggered Jobs inherit that template:

```yaml
env:
  - name: ControlPlane__Authentication
    value: AwsIam
  - name: ControlPlane__Port
    value: "5432"
  - name: ControlPlane__AwsProfile
    value: ""
  - name: ControlPlane__Username
    valueFrom:
      configMapKeyRef:
        name: production-ingestor-database
        key: username
  - name: ControlPlane__RootCertificate
    value: /etc/rds/eu-north-1-bundle.pem
```

Provision the referenced username ConfigMap and CA volume/mount in your deployment.
The RDS host, database, region and signing port come from Worker appsettings. For
another environment override them together. These changes do not apply IAM, grants,
networking, ConfigMaps, mounts, or Pod Identity associations. Do not replace the
CronJob with this partial example.

## Provider behavior

IAM signs the real RDS endpoint and port 5432 even when connecting on local 55434.
TLS verifies the real hostname using the CA bundle, hence the local hosts mapping.
The singleton PostgreSQL factory owns the data source/pool; each operation owns
its connection. Each physical authentication callback gets a newly signed token
from refreshable AWS credentials. Active connections do not expire with the original
15-minute token. New physical connections use fresh tokens. Use `OpenAsync` for IAM.
The AWS signer has no cancellation overload: cancellation stops waiting, but an
in-flight SDK credential request can finish afterward.

`IDatabaseConnectionFactory` supplies provider-neutral `DbConnection` creation.
The Azure SQL factory retains its typed API and Entra token callback. Change its
existing keyed `ProductionDatabase` server/catalog and principal configuration to
connect to Azure SQL; source queries still require their existing schema contract.
**Connection providers are not interchangeable SQL dialects**: selecting
`ControlPlane:Provider=AzureSql` fails early. Azure cursor persistence requires its
own repository and schema, outside this change.

Explicit `ControlPlane` settings take precedence and never fall back on error.
For legacy/test password connections set `ControlPlane:Authentication=ConnectionString`
and supply `ControlPlane:ConnectionString` externally through your secret mechanism.
`Source:ControlPlaneConnectionString` applies only when `ControlPlane` is absent.

## Verification and troubleshooting

Unit tests use injected credentials and no cloud calls. Live acceptance requires a
read-only query through the .NET factory from Windows and the actual EKS image/service
account, including a new physical connection after 15 minutes. Confirm the database,
approved user and TLS. Do not invoke the ingestion entry point as a smoke test.
Live .NET, EKS, expiry and Azure verification remain outstanding until access exists.

| Symptom | Check |
|---|---|
| PAM authentication failed | IAM enabled, approved login/grants, exact dbuser ARN, signing port, clock and credentials |
| Certificate error | Trusted RDS CA and real hostname; never disable validation |
| Timeout | Tunnel/hosts, private route, security groups, network policy |
| Role assumption denied | SSO permission, database role ARN and trust |
| Profile missing/expired | Configure/login on Windows; WSL profiles are separate |
| SQS fails after DB setup | Database profile isolation and retained pod role queue permissions |

References: [RDS IAM](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html),
[dbuser policies](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html),
[PostgreSQL TLS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.SSL.html).