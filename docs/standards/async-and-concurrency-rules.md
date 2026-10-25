# Async and Concurrency Rules

## Async I/O

Use asynchronous APIs for:

- database I/O,
- queue I/O,
- blob/object storage,
- HTTP,
- file I/O where supported.

Do not use:

```csharp
.Result
.Wait()
.GetAwaiter().GetResult()
```

in normal asynchronous application code.

## Cancellation

Pass `CancellationToken` through cancellable operations.

For worker code, cancellation normally begins at host shutdown and should flow through:

```text
worker loop
→ message handler
→ storage/database calls
→ downstream publish
```

Do not replace cancellation with arbitrary short timeouts.

## `Task.Run`

Do not wrap naturally asynchronous I/O in `Task.Run()`.

Use `Task.Run()` only for justified CPU-bound work that must be moved off a caller thread and where the runtime model actually benefits.

## Parallelism

Concurrency must be bounded.

Do not create unbounded task collections over:

- messages,
- WTGs,
- files,
- database rows,
- partitions.

Use explicit limits derived from operational constraints.

## Shared state

Avoid mutable process-wide shared state.

If shared state is required:

- define ownership,
- define synchronization,
- define lifecycle,
- test races where correctness matters.

## Worker concurrency

A worker that processes multiple messages concurrently must preserve:

- message-level isolation,
- correct visibility/lease handling,
- independent error handling,
- bounded resource use.

## Cancellation vs failure

Cancellation is not automatically an application error.

Do not convert expected host shutdown cancellation into error logs unless the operation violated an invariant.

## Timeouts

Use explicit operation timeouts for external dependencies where appropriate.

Timeout, cancellation, retry, and queue visibility must be designed together; do not independently choose values that cause duplicate concurrent work.
