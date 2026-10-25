$ErrorActionPreference = 'Stop'
$repositoryRoot = Resolve-Path (Join-Path $PSScriptRoot "../..")
Set-Location $repositoryRoot
dotnet restore RawFanOut.slnx
if ($LASTEXITCODE -ne 0) { throw 'Restore failed' }
dotnet build RawFanOut.slnx -c Release --no-restore
if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
dotnet test tests/Raw/RawFanOut.UnitTests/RawFanOut.UnitTests.csproj -c Release --no-build --logger 'trx;LogFileName=unit.trx'
if ($LASTEXITCODE -ne 0) { throw 'Unit tests failed' }
dotnet test tests/Raw/RawFanOut.IntegrationTests/RawFanOut.IntegrationTests.csproj -c Release --no-build --logger 'trx;LogFileName=integration.trx'
if ($LASTEXITCODE -ne 0) { throw 'Integration tests failed' }
