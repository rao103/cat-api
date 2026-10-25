# ADR-0002 — SQS work delivery versus company Kafka

**Recorded:** 2026-10-11 (Asia/Kuala_Lumpur). **Evidence:** user-confirmed company constraints plus saved platform specifications; public technical references checked on this date. No live repository, service catalogue, company Kafka configuration or deployed infrastructure was inspected. Proposed rationale is not company approval or performance evidence.

## Status

**Proposed rationale / existing SQS design requires review against company Kafka.** Existing queue topology remains DL-010; this ADR does not replace it or authorize a Kafka migration. No exception from the company preference was confirmed.

## Context and available alternatives

Company Kafka is already running. The team can request an AD group authorized for a specific topic. Kafka is encouraged for cloud-agnostic use. Do not assume it is unavailable, must be deployed by this team, or has an unmanageable onboarding cost. Provider type, supported client/authentication, partition/retention policy, retry facilities, throughput and support terms remain unknown.

Current platform design uses stage-specific SQS work notifications and DLQs. Immutable lake artifacts carry business data; component-owned PostgreSQL state carries durable progress. Work can complete out of order; event/source revisions and database fences govern correctness. Raw/Fan-Out/Harmonizer code has SQS adapters, but adoption of a current implementation is not proof it is the best long-term transport.

## Required behavior

Deliver frozen artifact/batch references; retry after crash; isolate poison work; retain exact replay bodies; permit worker scaling; propagate OperationId; align transport acknowledgment with the stage's durable handoff. Business completion and source ordering must not depend solely on transport delivery.

No global business-event order is currently required. Replay of immutable captured artifacts is supported by control ledgers; this does not prove that an enterprise event-history/replay requirement will never arise.

## Technical basis

SQS provides message visibility periods, explicit deletion and configurable redrive/DLQ behavior. Standard delivery can duplicate, so visibility is not a correctness lock. Kafka consumer groups distribute partition consumption; committed offsets define restart position. Variable-duration asynchronous work requires safe offset tracking, rebalance handling and deliberate retry design. Kafka can serve competing workers; it is not limited to broadcasting events.

These documented mechanics inform the comparison; the company service may supply supported libraries or retry conventions that reduce the application work. Neither transport provides an atomic transaction across our external PostgreSQL/file side effects merely by being selected.

## Alternatives and trade-offs

| Criterion | SQS current design | Company Kafka |
|---|---|---|
| Work acknowledgment | Individual message deletion after durable stage boundary | Commit only safely processed offsets; map durable canonical acceptance separately from lifecycle completion |
| Fail/retry isolation | Native visibility/redrive facilities plus existing ledgers | Verify supported retry-topic/DLQ or retry-handler model; avoid skipping unfinished work |
| Worker parallelism | Work delivery independent of Kafka partition assignment; same-key DB fences still apply | Partition strategy plus in-process concurrency and safe offset tracking; not one pod per topic by definition |
| Independent subscribers/history | Existing design primarily dispatches stage work | Consumer-group subscriptions and retained log may better serve shared replay/fan-out requirements |
| Company fit | AWS-specific service; exception/preference review required | Existing service, topic-scoped AD-group access and encouraged cloud-agnostic standard |
| Adoption impact | Existing adapters/tests/configuration; live readiness still unverified | Change transport consumers/producers and operational recovery; measure rather than guess effort |
| Cost/support | Obtain actual use/charge and operational ownership | Obtain company chargeback/support terms; provided does not establish total application cost |

## Proposed decision rationale, not a settled justification

SQS can be defended as a work-delivery choice if its per-message retry/ACK model and current delivery investment materially reduce application complexity and the company accepts the AWS-specific dependency. Do not use AWS migration, “Kafka is complex,” presumed broker ownership, or assumed cheaper pricing as sufficient justification.

Conversely, if the supplied Kafka service includes supported retries/client patterns that satisfy these requirements, its company preference and portability may outweigh incremental SQS convenience. No platform Kafka proof of concept or comparative measurements were supplied, so a definitive rejection would be unsupported.

Keep work payload schemas, artifact identities, OperationId and domain handlers separate from transport SDKs. A Kafka adapter cannot simply translate SQS DeleteMessage into arbitrary offset commits; semantic compatibility requires tests. Do not introduce a second broker in production as speculative future-proofing.

## Evidence required to accept or supersede

Use one representative Fan-Out/Harmonizer route with long and short work, one poison item, duplicate delivery, crash after durable commit, consumer restart/rebalance, scale changes and exact replay. Compare supported retry/ACK behavior, observable failures, effort, throughput/latency and chargeback at realistic load. Obtain team-topic/group permissions and platform-owner advice.

Then retain SQS with recorded justification/accepted exception, or supersede this proposal and DL-010 with an explicit Kafka transition plan. Already persisted SQS bodies and legacy redrive must remain recoverable across any switch.

## References

- [SQS visibility and duplicate-delivery behavior](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [SQS DLQ/redrive](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Apache Kafka consumer groups, offsets and asynchronous processing](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

Public documentation does not establish the company's deployed Kafka version/capabilities. Related: DL-010, DL-026, Q-016.
