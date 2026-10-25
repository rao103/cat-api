# Windows/Visual Studio setup reusing a WSL tunnel (no session-manager-plugin on Windows)

Use this when you already connect to the database from WSL via `./dbconnect.sh` and want
Visual Studio / `dotnet test` on Windows to reuse that same tunnel, instead of installing
`session-manager-plugin` on Windows and running `dbconnect.ps1 tunnel`.

TLS verification (`SslMode.VerifyFull` + `RootCertificate` + `Host == SigningHost`) is never
relaxed for local runs — only the *tunneling mechanism* changes. EKS is unaffected either way.

## One-time per machine

1. Download the regional RDS CA bundle:
   ```powershell
   New-Item -ItemType Directory -Force "$env:USERPROFILE\.aws\rds" | Out-Null
   Invoke-WebRequest -UseBasicParsing -Uri 'https://truststore.pki.rds.amazonaws.com/<region>/<region>-bundle.pem' -OutFile "$env:USERPROFILE\.aws\rds\<region>-bundle.pem"
   ```
2. Add the RDS hostname to the **Windows** hosts file (elevated editor),
   `C:\Windows\System32\drivers\etc\hosts`:
   ```text
   127.0.0.1 <rdsHost from connections.json>
   ```
   Required because `VerifyFull` checks the hostname in the cert; WSL2 auto-forwards the
   tunnel's `localhost` port to Windows, so no extra network setup is needed beyond this.
3. Configure a Windows AWS SSO profile that can generate an IAM auth token for the
   application's DB user (this is separate from whatever `dbconnect.sh` uses in WSL):
   ```powershell
   cd C:\ors-dungeons-dragons-aa\cat-dl-backend-service\localdbconnection
   .\dbconnect.ps1 configure
   .\dbconnect.ps1 login
   ```
   This does not require `session-manager-plugin` — that tool is only needed by
   `dbconnect.ps1 tunnel`, which this workflow skips entirely.

## Every session

1. **WSL terminal** — start (or confirm) the tunnel for the target database:
   ```bash
   cd /path/to/cat-dl-backend-service/localdbconnection
   ./dbconnect.sh tunnel <target>
   ```
   Leave it running. Note the target's `localPort` from `connections.json`.

2. **Windows PowerShell terminal** — set the `ControlPlane__*` env vars for this process,
   pointing `Port` at the WSL tunnel's local port and `SigningHost`/`SigningPort` at the
   real RDS endpoint:
   ```powershell
   $env:ControlPlane__Provider = 'PostgreSql'
   $env:ControlPlane__Authentication = 'AwsIam'
   $env:ControlPlane__Host = '<rdsHost>'
   $env:ControlPlane__Port = '<localPort from connections.json>'
   $env:ControlPlane__Database = '<dbName>'
   $env:ControlPlane__Username = '<application DB user, e.g. production_ingestor>'
   $env:ControlPlane__Region = '<region>'
   $env:ControlPlane__AwsProfile = '<profile from the one-time configure step>'
   $env:ControlPlane__SigningHost = '<rdsHost>'
   $env:ControlPlane__SigningPort = '5432'
   $env:ControlPlane__RootCertificate = "$env:USERPROFILE\.aws\rds\<region>-bundle.pem"
   ```

3. (Optional) opt into the live E2E test, same terminal:
   ```powershell
   $env:PRODUCTION_E2E_LIVE = 'true'
   $env:PRODUCTION_E2E_WINDFARM_ID = '<id>'
   $env:PRODUCTION_E2E_DATE = '<yyyy-MM-dd>'
   $env:PRODUCTION_E2E_SQS_OBSERVATION_CONFIRMED = 'true'
   ```

4. Run/debug from that same terminal:
   ```powershell
   dotnet test tests/Raw/ProductionIngestor.EndToEndTests/ProductionIngestor.EndToEndTests.csproj
   ```
   or `devenv.exe ..\O.WP.CAT.LossCalculation.Platform.sln` then use Test Explorer → Debug.
   A new/already-open Visual Studio instance not launched from this terminal will not see
   these env vars.

## Reusing this for another database

Repeat the "every session" steps with that target's values from `connections.json`
(`rdsHost`, `dbName`, `localPort`, `region`) and the region's CA bundle URL. Add its
hostname to the Windows hosts file (step 2 of the one-time setup) the first time.
