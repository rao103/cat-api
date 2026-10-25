# RPI Routing

The older C# architecture package contained a general RPI checklist. The workflow is now split into dedicated Skills.

Use:

```text
Unknown architecture / unclear behavior
    -> architecture-discovery

Sufficient discovery; need implementation-ready plan
    -> create-micro-rpi

Accepted Micro-RPI; ready to code
    -> implement-micro-rpi
```

Use `database-migration` when schema change is a material part of the work.

Use `add-source-type` when extending a stable multi-source pipeline.

Each phase must explicitly carry forward the accepted artifact paths and relevant standards when context is not guaranteed to persist.
