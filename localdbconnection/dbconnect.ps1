[CmdletBinding()]
param(
    [ValidateSet('check', 'configure', 'login', 'tunnel', 'debug-env', 'verify')]
    [string] $Command = 'check',
    [string] $ConfigPath = (Join-Path $PSScriptRoot '..\src\Raw\Ingestors\Production\ProductionIngestor.Worker\appsettings.json'),
    [string] $RootCertificate = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$appSettings = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$controlPlane = $appSettings.ControlPlane
$local = $controlPlane.LocalConnection
$config = [pscustomobject]@{
    accountId = $local.AccountId
    region = $controlPlane.Region
    ssoStartUrl = $local.SsoStartUrl
    ssoRoleName = $local.SsoRoleName
    ssoProfile = $local.SsoProfile
    databaseProfile = $local.DatabaseProfile
    databaseRoleArn = $local.DatabaseRoleArn
    bastionInstanceId = $local.BastionInstanceId
    rdsHost = $controlPlane.Host
    remotePort = $controlPlane.SigningPort
    localPort = $local.Port
    database = $controlPlane.Database
    dbUser = $controlPlane.Username
}
if ([string]::IsNullOrWhiteSpace($RootCertificate)) {
    $RootCertificate = [Environment]::ExpandEnvironmentVariables($local.RootCertificate)
}
if ($controlPlane.Provider -ne 'PostgreSql' -or $controlPlane.Authentication -ne 'AwsIam') {
    throw 'This tool requires ControlPlane:Provider=PostgreSql and Authentication=AwsIam in the Worker appsettings.json.'
}
if ($controlPlane.Host -ne $controlPlane.SigningHost -or $controlPlane.Port -ne $controlPlane.SigningPort) {
    throw 'Worker settings must use the real RDS host/port. Set the tunnel port only in ControlPlane:LocalConnection:Port.'
}

function Require-Value([string] $Name) {
    $property = $config.PSObject.Properties[$Name]
    if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string] $property.Value)) {
        throw "Set '$Name' in the Worker appsettings.json ControlPlane/LocalConnection settings using actual CAT infrastructure outputs."
    }
    return $property.Value
}

function Require-Tool([string] $Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required tool '$Name' is missing. See localdbconnection/README.md."
    }
}

function Invoke-Aws([string[]] $Arguments) {
    $result = & aws @Arguments --no-cli-pager
    if ($LASTEXITCODE -ne 0) { throw 'AWS command failed. Check your profile, SSO session, and permissions.' }
    return $result
}

function Assert-Target {
    foreach ($name in @('accountId', 'region', 'rdsHost', 'database', 'ssoProfile', 'databaseProfile')) {
        $null = Require-Value $name
    }
    if ($config.accountId -notmatch '^\d{12}$') { throw 'accountId must have 12 digits.' }
    if ($config.rdsHost -notmatch ('^[a-zA-Z0-9.-]+\.' + [regex]::Escape($config.region) + '\.rds\.amazonaws\.com$')) {
        throw 'rdsHost must be the real RDS endpoint in the configured region.'
    }
    foreach ($name in @('localPort', 'remotePort')) {
        $port = [int] (Require-Value $name)
        if ($port -lt 1 -or $port -gt 65535) { throw "$name must be between 1 and 65535." }
    }
    if ($config.ssoProfile -notmatch '^[\w-]+$' -or $config.databaseProfile -notmatch '^[\w-]+$') {
        throw 'Profile names may contain only letters, digits, underscores and hyphens.'
    }
}

function Set-ProfileValue([string] $Profile, [string] $Name, [string] $Value) {
    $existing = & aws configure get $Name --profile $Profile
    $getExitCode = $LASTEXITCODE
    if ($getExitCode -notin @(0, 1)) { throw "Unable to inspect profile '$Profile'." }
    if ($getExitCode -eq 0 -and -not [string]::IsNullOrWhiteSpace($existing) -and $existing -ne $Value) {
        throw "Profile '$Profile' has a different '$Name'. Resolve it manually or choose another profile name."
    }
    $null = Invoke-Aws @('configure', 'set', $Name, $Value, '--profile', $Profile)
}

function Assert-LocalConnection {
    if (-not (Test-Path -LiteralPath $RootCertificate -PathType Leaf)) {
        throw 'Install the trusted RDS CA bundle and supply -RootCertificate. See README.md.'
    }
    $addresses = [System.Net.Dns]::GetHostAddresses($config.rdsHost)
    if ($addresses.Count -eq 0 -or @($addresses | Where-Object { -not [System.Net.IPAddress]::IsLoopback($_) }).Count -gt 0) {
        throw 'Configure the Windows hosts entry for this RDS hostname as 127.0.0.1. A WSL hosts entry is insufficient.'
    }
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $pending = $client.ConnectAsync('127.0.0.1', [int] $config.localPort)
        if (-not $pending.Wait(3000) -or -not $client.Connected) { throw 'The local SSM tunnel is not listening.' }
    }
    finally { $client.Dispose() }
}

Assert-Target
if ($Command -eq 'check') {
    [pscustomobject]@{
        Target = "$($config.database) at $($config.rdsHost):$($config.remotePort)"
        AwsCli = [bool] (Get-Command aws -ErrorAction SilentlyContinue)
        SsmPlugin = [bool] (Get-Command session-manager-plugin -ErrorAction SilentlyContinue)
        Psql = [bool] (Get-Command psql -ErrorAction SilentlyContinue)
        CaBundle = Test-Path -LiteralPath $RootCertificate -PathType Leaf
        SsoUrlConfigured = -not [string]::IsNullOrWhiteSpace($config.ssoStartUrl)
        BastionConfigured = -not [string]::IsNullOrWhiteSpace($config.bastionInstanceId)
        DatabaseUserConfigured = -not [string]::IsNullOrWhiteSpace($config.dbUser)
    }
    return
}

Require-Tool 'aws'
switch ($Command) {
    'configure' {
        $startUrl = [uri] (Require-Value 'ssoStartUrl')
        if (-not $startUrl.IsAbsoluteUri -or $startUrl.Scheme -ne 'https') { throw 'ssoStartUrl must be an HTTPS URL.' }
        $role = [string] (Require-Value 'ssoRoleName')
        $databaseRole = [string] $config.databaseRoleArn
        if ($databaseRole -and $databaseRole -notmatch ('^arn:aws:iam::' + $config.accountId + ':role/[\w+=,.@/-]+$')) {
            throw 'databaseRoleArn must be the approved IAM role ARN in the CAT account.'
        }
        if (-not $databaseRole -and $config.databaseProfile -ne $config.ssoProfile) {
            throw 'Set databaseRoleArn, or set databaseProfile equal to ssoProfile when that identity has direct database access.'
        }
        if ($databaseRole -and $config.databaseProfile -eq $config.ssoProfile) {
            throw 'A database role profile must differ from its SSO source profile.'
        }
        Set-ProfileValue $config.ssoProfile 'sso_start_url' $startUrl.AbsoluteUri
        Set-ProfileValue $config.ssoProfile 'sso_region' $config.region
        Set-ProfileValue $config.ssoProfile 'sso_account_id' $config.accountId
        Set-ProfileValue $config.ssoProfile 'sso_role_name' $role
        Set-ProfileValue $config.ssoProfile 'region' $config.region
        if ($databaseRole) {
            Set-ProfileValue $config.databaseProfile 'role_arn' $databaseRole
            Set-ProfileValue $config.databaseProfile 'source_profile' $config.ssoProfile
            Set-ProfileValue $config.databaseProfile 'region' $config.region
        }
        Write-Host 'Profiles configured. Run .\dbconnect.ps1 login next.'
    }
    'login' {
        & aws sso login --profile $config.ssoProfile --use-device-code --no-cli-pager
        if ($LASTEXITCODE -ne 0) { throw 'SSO login failed.' }
    }
    'tunnel' {
        Require-Tool 'session-manager-plugin'
        $bastion = [string] (Require-Value 'bastionInstanceId')
        if ($bastion -notmatch '^i-[a-f0-9]+$') { throw 'bastionInstanceId must be the actual CAT EC2 instance ID.' }
        $account = Invoke-Aws @('sts', 'get-caller-identity', '--profile', $config.ssoProfile, '--query', 'Account', '--output', 'text')
        if ($account -ne $config.accountId) { throw 'The SSO profile is signed into the wrong AWS account.' }
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, [int] $config.localPort)
        $listener.Server.ExclusiveAddressUse = $true
        try { $listener.Start() }
        catch { throw 'Local port is occupied. Stop the existing listener or select another localPort.' }
        finally { $listener.Stop() }
        Write-Host 'Leave this terminal open. Ctrl+C stops this tunnel; it is not a background service.'
        & aws ssm start-session --profile $config.ssoProfile --region $config.region --target $bastion `
            --document-name AWS-StartPortForwardingSessionToRemoteHost `
            --parameters "host=$($config.rdsHost),portNumber=$($config.remotePort),localPortNumber=$($config.localPort)" --no-cli-pager
        if ($LASTEXITCODE -ne 0) { throw 'SSM session ended unsuccessfully. Check login, bastion permissions and networking.' }
    }
    'debug-env' {
        $username = [string] (Require-Value 'dbUser')
        Assert-LocalConnection
        $settings = @{
            ControlPlane__Provider = 'PostgreSql'
            ControlPlane__Authentication = 'AwsIam'
            ControlPlane__ConnectionString = ''
            ControlPlane__Host = $config.rdsHost
            ControlPlane__Port = [string] $config.localPort
            ControlPlane__Database = $config.database
            ControlPlane__Username = $username
            ControlPlane__Region = $config.region
            ControlPlane__AwsProfile = $config.databaseProfile
            ControlPlane__SigningHost = $config.rdsHost
            ControlPlane__SigningPort = [string] $config.remotePort
            ControlPlane__RootCertificate = (Resolve-Path -LiteralPath $RootCertificate).Path
        }
        foreach ($entry in $settings.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
        }
        Write-Host 'ControlPlane settings applied to this PowerShell process. Start a new Visual Studio instance from here.'
        Write-Host 'Other AWS clients still use their own default credential chain; no global AWS_PROFILE was changed.'
    }
    'verify' {
        Require-Tool 'psql'
        $username = [string] (Require-Value 'dbUser')
        Assert-LocalConnection
        $previous = @{}
        foreach ($name in @('PGPASSWORD', 'PGSSLMODE', 'PGSSLROOTCERT', 'PGOPTIONS', 'PGCONNECT_TIMEOUT')) {
            $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        }
        try {
            $token = Invoke-Aws @('rds', 'generate-db-auth-token', '--profile', $config.databaseProfile,
                '--hostname', $config.rdsHost, '--port', [string] $config.remotePort,
                '--region', $config.region, '--username', $username)
            if ([string]::IsNullOrWhiteSpace($token)) { throw 'AWS returned an empty IAM token.' }
            $env:PGPASSWORD = $token
            $env:PGSSLMODE = 'verify-full'
            $env:PGSSLROOTCERT = (Resolve-Path -LiteralPath $RootCertificate).Path
            $env:PGOPTIONS = '-c default_transaction_read_only=on'
            $env:PGCONNECT_TIMEOUT = '10'
            & psql -X --no-password --host $config.rdsHost --port $config.localPort --dbname $config.database `
                --username $username --set ON_ERROR_STOP=1 `
                --command 'SELECT current_database(), current_user, ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();'
            if ($LASTEXITCODE -ne 0) { throw 'Read-only database verification failed.' }
        }
        finally {
            $token = $null
            foreach ($name in $previous.Keys) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
        }
    }
}