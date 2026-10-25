# CAT Loss Calculation Platform — Target Architecture Plan

Status: **plan only — not yet implemented**

## Goal

Reorganize the repository so every worker/API in the platform lives under `src/<zone>/<component>/{Component.Worker|Component.API, Component.Core}`, grouped by processing zone (`Raw`, `Harmonized`, `Calculation`, `Api`), with shared contracts isolated under `Shared/` only when genuinely reused.

## Observed facts (current state)

- Solution: `ProductionIngestor.sln` (target rename: `O.WP.CAT.LossCalculation.Platform.sln`).
- `src/ProductionIngestor` — single worker project `O.WP.Raw.ProductionIngestor.csproj` (SDK: `Microsoft.Extensions.Hosting` worker), containing `Discovery/`, `Extraction/`, `Execution/`, `Refresh/`, `Cursor/`, `Monitoring/`, `Messaging/`, `Interfaces/`, `Configuration/`, `Raw/`. References `ProductionIngestor.Contracts`.
- `src/ProductionIngestor.Api` — ASP.NET-style API host: `Controllers/`, `Builders/`, `Configurators/`, `Filters/`, `Results/`, `Triggering/`, `Interfaces/`, `Configuration/`.
- `src/ProductionIngestor.Contracts` — small shared-contracts project (`RawArtifactAvailableV1.cs`), net8.0, referenced by the worker.
- `tests/ProductionIngestor.UnitTests`, `tests/ProductionIngestor.IntegrationTests`.
- No Outage, Curtailment, Alarm, Harmonizer, or LossCalculation code exists yet anywhere in the repo — these are net-new components.

## Target tree

```
O.WP.CAT.LossCalculation.Platform.sln

src/
  Raw/
    Ingestors/
      Production/
        ProductionIngestor.Worker/      <- current src/ProductionIngestor (host + DI), references Core
        ProductionIngestor.Core/        <- Discovery, Extraction, Execution, Refresh, Cursor, Monitoring,
                                            Messaging, Interfaces, Configuration, Models moved here
      Outage/       (placeholder only)
        OutageIngestor.Worker/
        OutageIngestor.Core/
      Curtailment/  (placeholder only)
        CurtailmentIngestor.Worker/
        CurtailmentIngestor.Core/
      Alarm/        (placeholder only)
        AlarmIngestor.Worker/
        AlarmIngestor.Core/

  Harmonized/
    Harmonizer/            (placeholder only — no code exists yet)
      Harmonizer.Worker/
      Harmonizer.Core/

  Calculation/
    LossCalculation/       (placeholder only — no code exists yet)
      LossCalculation.Worker/
      LossCalculation.Core/

  Api/
    LossCalculation.API/            <- current src/ProductionIngestor.Api, renamed
    LossCalculation.API.Core/       <- Builders/Configurators/Filters/Results business logic extracted from the API host

  Shared/
    Messaging.Contracts/    <- current ProductionIngestor.Contracts, renamed/moved
    Data.Contracts/         (created only when a second consumer needs it — not yet)

tests/
  Raw/
    ProductionIngestor.UnitTests/         <- moved from tests/
    ProductionIngestor.IntegrationTests/  <- moved from tests/
  Harmonized/   (created when Harmonizer exists)
  Calculation/  (created when LossCalculation exists)
  Api/
    LossCalculation.API.UnitTests/        (created if/when API tests are added)

deploy/
  ProductionIngestor/
    Dockerfile
    .kustomize/base, overlays/{development,testing,staging,production}
  (other component folders created when those components exist)
```

## Naming note

The current API host currently ingests/exposes "ProductionIngestor" concerns specifically, not generic loss-calculation concerns. Renaming it to `LossCalculation.API` implies it will grow to serve the whole platform's API surface — confirm that's the intent before the rename; otherwise keep it as `ProductionIngestor.API` under `src/Api/` and introduce `LossCalculation.API` later as a distinct project when that surface actually exists.

## Responsibilities & allowed references

| Project | Responsibility | May reference |
|---|---|---|
| `ProductionIngestor.Worker` | Host: `Program.cs`, DI wiring, `IHostedService`/timer loop, queue/message trigger plumbing | `ProductionIngestor.Core`, `Shared/Messaging.Contracts` |
| `ProductionIngestor.Core` | Discovery, extraction, execution, refresh, cursor, monitoring logic, repository/service interfaces + implementations | `Shared/Messaging.Contracts` only (no other component's Core) |
| `Harmonizer.Core` | Harmonization transforms per source (Production/Outage/etc. as subfolders) | `Shared/Messaging.Contracts`; **not** `ProductionIngestor.Core` |
| `LossCalculation.Core` | Calculation business rules | `Shared/Messaging.Contracts` |
| `*.API` | Thin controllers/triggers only | corresponding `*.API.Core` |
| `*.API.Core` | API business logic (builders, result shaping) | `Shared` contracts as needed |
| `Shared/Messaging.Contracts` | Cross-component wire messages (e.g. `RawArtifactAvailableV1`) | nothing project-specific |

Dependency rule enforced: `Worker → Core` one-way; Core projects never reference sibling components' Core; integration between zones happens only via `Shared` contracts/messages.

## One end-to-end flow (Production ingestion, current behavior preserved)

1. `ProductionIngestor.Worker` (host) starts hosted service → resolves `ProductionIngestor.Core` services via DI.
2. Core `Discovery` finds new raw artifacts → `Extraction` reads them → `Execution` processes → `Cursor`/`Refresh` persist progress → `Messaging` publishes `RawArtifactAvailableV1` (from `Shared/Messaging.Contracts`).
3. `LossCalculation.API` (future) or `Harmonizer.Worker` (future) will consume that same contract — this is why the contract lives in `Shared`, not inside `ProductionIngestor.Core`.

## Migration steps (when implementation is authorized)

1. Rename solution file; update all project paths inside it.
2. Create folder skeleton under `src/`, `tests/`, `deploy/` (only `Raw/Ingestors/Production` and `Api` get real content; others are empty placeholder folders with no `.csproj` until scaffolded — confirmed: scaffold empty placeholder projects too since "Both" was selected).
3. Move `src/ProductionIngestor` → split:
   - New `ProductionIngestor.Worker.csproj` (Sdk.Worker) keeping `Program.cs`, DI wiring, config binding, hosted-service loop.
   - New `ProductionIngestor.Core.csproj` (Sdk) holding `Discovery/`, `Extraction/`, `Execution/`, `Refresh/`, `Cursor/`, `Monitoring/`, `Messaging/`, `Interfaces/`, `Configuration/`, `Raw/`.
   - Worker references Core; both reference `Shared/Messaging.Contracts`.
4. Move `ProductionIngestor.Contracts` → `Shared/Messaging.Contracts`, update namespace and all references.
5. Move `src/ProductionIngestor.Api` → `src/Api/...`, extract business logic (`Builders/Configurators/Filters/Results`) into a new `*.API.Core` project if warranted, keep controllers thin in the API host.
6. Move `tests/ProductionIngestor.UnitTests` and `tests/ProductionIngestor.IntegrationTests` → `tests/Raw/`, update their project references and any moved namespaces.
7. Scaffold empty placeholder `Worker`+`Core` project pairs (minimal `Program.cs`/empty class, real `.csproj`, added to solution) for Outage, Curtailment, Alarm, Harmonizer, LossCalculation per the "scaffold placeholders" choice — mark them clearly as not-yet-implemented (e.g. a single-line comment) so they aren't mistaken for working components.
8. Update `.sln`, CI workflow paths, Dockerfile paths, and `.github/copilot-instructions.md` architecture section to match.
9. Build + run existing unit/integration tests to confirm no regression after the move.

## Open items to confirm before implementing

- Confirm whether `ProductionIngestor.Api` should really be renamed to `LossCalculation.API` now, or stay `ProductionIngestor.API` until a true platform-wide API exists (see Naming note above).
- Confirm whether placeholder projects should be added to the `.sln` immediately (increases solution load time) or created as bare folders until first real code lands.
- Confirm CI/CD (`azure-pipelines.yml` in cat-backend-library, and any workflow in cat-dl-backend-service) needs path updates as part of this move — none inspected yet.
</content>
