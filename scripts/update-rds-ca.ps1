<#
.SYNOPSIS
    Refreshes the canonical Amazon RDS global CA bundle used by the Production Ingestor.
.DESCRIPTION
    Downloads the official AWS RDS trust-store bundle, validates it, and replaces
    deploy/production-ingestor/base/certs/global-bundle.pem only after validation succeeds.
    Never disables TLS validation to perform the download.
#>
[CmdletBinding()]
param(
    [string] $SourceUrl = 'https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem'
)

$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$targetPath = Join-Path $repoRoot 'deploy/production-ingestor/base/certs/global-bundle.pem'
$tempPath = Join-Path ([System.IO.Path]::GetTempPath()) "global-bundle-$([Guid]::NewGuid()).pem"

try {
    Write-Host "Downloading RDS CA bundle from $SourceUrl ..."
    Invoke-WebRequest -Uri $SourceUrl -OutFile $tempPath -UseBasicParsing

    if (-not (Test-Path -LiteralPath $tempPath -PathType Leaf)) {
        throw "Download did not produce a file at '$tempPath'."
    }

    $content = Get-Content -LiteralPath $tempPath -Raw
    if ([string]::IsNullOrWhiteSpace($content)) {
        throw 'Downloaded RDS CA bundle is empty.'
    }

    if ($content -notmatch '-----BEGIN CERTIFICATE-----') {
        throw 'Downloaded file does not contain a PEM certificate block.'
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $targetPath) | Out-Null
    Copy-Item -LiteralPath $tempPath -Destination $targetPath -Force

    $hash = (Get-FileHash -LiteralPath $targetPath -Algorithm SHA256).Hash
    Write-Host "Updated $targetPath"
    Write-Host "SHA-256: $hash"
}
finally {
    Remove-Item -LiteralPath $tempPath -ErrorAction SilentlyContinue
}
