$ErrorActionPreference = 'Stop'
$repo = 'C:\Users\XVENR\source\repos\cat-dl-backend-service'
$distribution = 'Ubuntu'
$configPath = Join-Path $repo 'localdbconnection\connections.json'
$certificate = Join-Path $repo 'deploy\production-ingestor\base\certs\global-bundle.pem'

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    throw 'WSL is not installed.'
}
if (-not (Test-Path -LiteralPath $configPath)) { throw "Missing file: $configPath" }
if (-not (Test-Path -LiteralPath $certificate)) { throw "Missing certificate: $certificate" }

$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
$target = $config.targets.'cat-dev'
if (-not $target -or $target.localPort -ne 55432) {
    throw 'Expected cat-dev configuration with localPort 55432.'
}
$addresses = [System.Net.Dns]::GetHostAddresses($target.rdsHost)
if ($addresses.Count -eq 0 -or @($addresses | Where-Object {
    -not [System.Net.IPAddress]::IsLoopback($_)
}).Count -gt 0) {
    throw "Windows hosts mapping is missing. Add: 127.0.0.1 $($target.rdsHost) to C:\Windows\System32\drivers\etc\hosts."
}

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw 'AWS CLI is missing on Windows. Visual Studio requires the configured Windows AWS profile.'
}
$workerConfigPath = Join-Path $repo 'src\Raw\Ingestors\Production\ProductionIngestor.Worker\appsettings.json'
$windowsConnection = (Get-Content -LiteralPath $workerConfigPath -Raw | ConvertFrom-Json).ControlPlane.LocalConnection
$windowsDatabaseProfile = $windowsConnection.DatabaseProfile
if ([string]::IsNullOrWhiteSpace($windowsDatabaseProfile)) {
    throw 'The Windows database AWS profile is not configured.'
}

Write-Host 'Checking the Windows AWS session used by Visual Studio...'
$ErrorActionPreference = 'Continue'
$null = & aws sts get-caller-identity --profile $windowsDatabaseProfile --no-cli-pager 2>$null
$windowsSessionExit = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
if ($windowsSessionExit -ne 0) {
    Write-Host 'Windows AWS login is required. Complete the browser/device-code sign-in.'
    & powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File (Join-Path $repo 'localdbconnection\dbconnect.ps1') -Command login -ConfigPath $workerConfigPath
    if ($LASTEXITCODE -ne 0) { throw 'Windows AWS login failed.' }
    $ErrorActionPreference = 'Continue'
    $null = & aws sts get-caller-identity --profile $windowsDatabaseProfile --no-cli-pager 2>$null
    $windowsSessionExit = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    if ($windowsSessionExit -ne 0) { throw 'The Windows database AWS profile is still unavailable after login.' }
}
Write-Host 'Windows AWS session verified.'

$installed = @(wsl.exe --list --quiet | ForEach-Object { ($_ -replace "`0", '').Trim() })
if ($LASTEXITCODE -ne 0 -or $installed -notcontains $distribution) {
    throw 'The Ubuntu WSL distribution was not found.'
}
$running = @(wsl.exe --list --running --quiet | ForEach-Object { ($_ -replace "`0", '').Trim() })
if ($LASTEXITCODE -ne 0) { throw 'Could not check WSL state.' }

if ($running -contains $distribution) {
    Write-Host 'Restarting Ubuntu and closing its existing processes...'
    wsl.exe --terminate $distribution
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop Ubuntu.' }
}
Write-Host 'Starting Ubuntu...'
wsl.exe -d $distribution -- true
if ($LASTEXITCODE -ne 0) { throw 'Could not start Ubuntu.' }

$bash = @'
#!/usr/bin/env bash
set -euo pipefail
set +x

if [[ "${1:-}" == "hosts" ]]; then
    host="$2"
    if ! grep -qE "^127\\.0\\.0\\.1[[:space:]]+${host}([[:space:]]|$)" /etc/hosts; then
        printf '\n127.0.0.1 %s\n' "$host" >> /etc/hosts
    fi
    exit 0
fi

cd /mnt/c/Users/XVENR/source/repos/cat-dl-backend-service/localdbconnection
for tool in aws python3 psql session-manager-plugin ss; do
    command -v "$tool" >/dev/null || { echo "Missing WSL tool: $tool" >&2; exit 1; }
done

IFS=$'\t' read -r profile region role host database username port remote < <(
    python3 - <<'PY'
import json
with open("connections.json") as f:
    config = json.load(f)
target = config["targets"]["cat-dev"]
print("\t".join(str(value) for value in (
    target["profile"], target["region"], target["roleArn"], target["rdsHost"],
    target["dbName"], target["dbUser"], target["localPort"],
    target.get("remotePort", config.get("defaults", {}).get("remotePort", 5432))
)))
PY
)

export AWS_PROFILE="$profile" AWS_REGION="$region" AWS_DEFAULT_REGION="$region"
bash ./dbconnect.sh tunnel cat-dev

ss -H -O -ltnp | grep ":$port " | grep -q '"session-manager",pid=' ||
    { echo "The expected SSM listener was not found." >&2; exit 1; }

echo "Checking PostgreSQL in read-only mode..."
credentials="$(aws sts assume-role --role-arn "$role" \
    --role-session-name "pg-iam-$(date +%Y%m%d%H%M%S)" \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)"
read -r access secret session <<< "$credentials"
PGPASSWORD="$(AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret" \
    AWS_SESSION_TOKEN="$session" aws rds generate-db-auth-token \
    --hostname "$host" --port "$remote" --region "$region" --username "$username")"
[[ -n "$PGPASSWORD" ]] || { echo "IAM token generation returned an empty result." >&2; exit 1; }
export PGPASSWORD
unset credentials access secret session
trap 'unset PGPASSWORD' EXIT

export PGSSLMODE=verify-full
export PGSSLROOTCERT=/mnt/c/Users/XVENR/source/repos/cat-dl-backend-service/deploy/production-ingestor/base/certs/global-bundle.pem
export PGCONNECT_TIMEOUT=10
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=10000'

psql -X --no-password --host "$host" --port "$port" --dbname "$database" \
    --username "$username" --set ON_ERROR_STOP=1 --command \
    "SELECT current_database() AS database, current_user AS username,
     current_setting('transaction_read_only') AS read_only, ssl
     FROM pg_stat_ssl WHERE pid = pg_backend_pid();"

echo "Database verified. Leave Ubuntu running, then start Centralize.Api."
'@

$tempScript = Join-Path ([IO.Path]::GetTempPath()) ('cat-tunnel-' + [guid]::NewGuid().ToString('N') + '.sh')
try {
    [IO.File]::WriteAllText($tempScript, $bash.Replace("`r`n", "`n"), (New-Object Text.UTF8Encoding($false)))
    $windowsScriptPath = $tempScript.Replace('\', '/')
    $linuxScript = wsl.exe -d $distribution -- wslpath -u $windowsScriptPath | Select-Object -Last 1
    if ($LASTEXITCODE -ne 0 -or -not $linuxScript) { throw 'Could not resolve the temporary WSL script.' }
    $linuxScript = $linuxScript.Trim()

    wsl.exe -d $distribution -u root -- bash $linuxScript hosts $target.rdsHost
    if ($LASTEXITCODE -ne 0) { throw 'Could not prepare the WSL hosts mapping.' }

    Write-Host 'Connecting. Complete the browser/device-code login if requested.'
    $ErrorActionPreference = 'Continue'
    wsl.exe -d $distribution -- bash $linuxScript
    $connectionExit = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    if ($connectionExit -ne 0) {
        throw 'Tunnel/database verification failed. Check the output above.'
    }
    Write-Host 'SUCCESS: tunnel and read-only database connection verified.' -ForegroundColor Green
} finally {
    $ErrorActionPreference = 'Stop'
    if (Test-Path -LiteralPath $tempScript) { Remove-Item -LiteralPath $tempScript -Force }
}
