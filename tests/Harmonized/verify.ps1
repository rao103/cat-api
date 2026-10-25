$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Push-Location $repoRoot
try {
    dotnet restore tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj --locked-mode
    if ($LASTEXITCODE -ne 0) { throw 'Unit-test restore failed' }
    dotnet restore tests/Harmonized/Harmonizer.IntegrationTests/Harmonizer.IntegrationTests.csproj --locked-mode
    if ($LASTEXITCODE -ne 0) { throw 'Integration-test restore failed' }
    dotnet build src/Harmonized/Harmonizer/Harmonizer.Worker/Harmonizer.Worker.csproj -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Harmonizer build failed' }
    dotnet test tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Harmonizer unit tests failed' }
    dotnet test tests/Harmonized/Harmonizer.IntegrationTests/Harmonizer.IntegrationTests.csproj -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Harmonizer integration tests failed' }
} finally { Pop-Location }