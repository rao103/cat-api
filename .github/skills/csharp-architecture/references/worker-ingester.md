# Worker / Ingester Reference

This reference preserves the useful project/folder convention from the older C# Copilot setup. Generic C#, async, messaging, database, telemetry, and testing rules remain authoritative under `docs/standards/`.

## Project responsibilities

For CAT-style ingestors and similar workers:

```text
<Name>.Core
<Name>.Worker
```

Optional API only when the component truly exposes HTTP behavior.

### Core

Owns:

- ingestion/processing operations,
- canonical/request models,
- capability interfaces,
- validation,
- settings resolution,
- persistence,
- external integration adapters,
- serialization.

### Worker

Owns:

- host lifecycle,
- configuration binding,
- DI composition,
- worker loop / one-shot execution entry,
- cancellation/shutdown,
- exit status.

Dependency direction:

```text
Worker -> Core
Core -X-> Worker
```

## Role-based Core folders

Create only folders that own real code:

```text
Configuration/
Validators/
Resolvers/
Interfaces/
Factories/
Authentication/
Repositories/
Adapters/
  Source/
  Messaging/
  Storage/
Operations/
  Discovery/
  Extraction/
  Execution/
  Refresh/
Serialization/
Raw/
Models/
Enums/
Constants/
Monitoring/
```

The folder tree is a responsibility map, not a required empty scaffold.

## Capability boundaries

Prefer specific capabilities, for example:

```text
source reader
control-state repository
message publisher
object store
serializer
```

Do not create a generic repository merely to wrap every SDK.

Typical flow:

```text
Host
→ Operation
→ capability interface
→ repository/adapter
→ external system
```

## Configuration separation

Options should primarily contain bindable configuration data.

Use dedicated validation/resolution where it prevents options objects from becoming active service classes.

For startup-critical values, validate before claiming work.

## Workload semantics

Explicitly decide whether the executable is:

- a bounded one-shot Job/CronJob, or
- a continuous queue consumer.

Do not accidentally turn one model into the other.

## Large-data semantics

Define:

- bounded source windows/pages,
- stable sort/tie-break keys,
- memory limits,
- concurrency limits,
- late/revised-data behavior,
- durable success point.

For ingestion-specific business rules, read:

`docs/components/ingestor/implementation-rules.md`
