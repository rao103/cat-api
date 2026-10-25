compose.integration.yml starts the local services needed by your external integration tests using Docker Compose.

Service	Purpose
PostgreSQL	Tests artifact tracking, completion acknowledgements, and failure records
LocalStack	Emulates AWS SQS for publishing and consuming messages
Azurite	Emulates Azure Blob Storage for uploading and reading Parquet files

It lets you test the pipeline locally without connecting to company AWS or Azure resources.

Start the services from the package directory:

docker compose -f compose.integration.yml up -d --wait

Enable and run the integration tests:

$env:RAWFANOUT_INTEGRATION = "1"

dotnet test tests/Raw/RawFanOut.IntegrationTests/RawFanOut.IntegrationTests.csproj

When finished:

Remove-Item Env:RAWFANOUT_INTEGRATION
docker compose -f compose.integration.yml down

You don’t need it for unit tests or the standalone Parquet tests. It requires Docker with Compose support and permission to obtain the specified container images—use your company’s JFrog mirrors where required.


verify.ps1
is a local build-and-test script for the generated package.

It runs these steps in order:

Step	What it does
Restore	Downloads the required NuGet packages using your configured package sources
Build	Compiles the solution in Release mode
Unit tests	Tests fan-out logic, validation, retries, and completion handling
Integration tests	Runs the Parquet tests and, when enabled, PostgreSQL/Blob/SQS integration tests

It stops immediately if any step fails and generates TRX test-result files.

You need the .NET 10 SDK installed. External integration tests are skipped unless you start the supplied Docker Compose services and set:

$env:RAWFANOUT_INTEGRATION = "1"

It does not deploy to EKS or apply migrations to your company database. Enabled PostgreSQL integration tests apply test migrations only to the guarded local test database.

From the repository root:

.\scripts\raw-fanout\verify.ps1

Start the fan-out test dependencies with:

docker compose -f scripts/raw-fanout/compose.integration.yml up -d --wait

If several components eventually use the same local PostgreSQL, SQS, and Blob emulators, you can consolidate their Compose configuration later.
