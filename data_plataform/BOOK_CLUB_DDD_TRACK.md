# Theory Track / Book Club — DDD (Khononov + Evans)

> Note: this file is new (the earlier version mentioned in past sessions wasn't
> found on disk). For now it only contains the content plan for the T3 Book Club (DDD),
> derived from and aligned with `docs/READING_SCHEDULE.md` (source of truth for the
> chapters and pages of the personal study plan). The rest of the Consolidated
> Strategy (personas, Fast Track, Deep Track, NexoMart) can be rebuilt here later if
> it makes sense to.

## Why Combine the Two Books

- **Evans (Domain-Driven Design, 2003 — the Blue Book)** — the original source, denser
  and more philosophical. It's the theoretical "base" of the T3 Book Club in the
  personal schedule.
- **Khononov (Learning Domain-Driven Design, 2021)** — a modern, practical map, covering
  patterns that weren't formalized yet in 2003 (Outbox, Saga, CQRS). It's the "T3 Book
  Club" itself in the schedule.

Decision: instead of a single T3 Book Club episode covering both books at once,
**expand into 4 sub-episodes (T3.1–T3.4)**, pairing Evans + Khononov by theme. This
keeps the `T` numbering already established on the channel (T0 Kimball, T1 DE
Fundamentals, T2 DDIA...) instead of creating a new prefix.

## Book Club T3.1 — "Language and Domain"

- **Evans** — Part I (chaps 1–3): Ubiquitous Language + a model to work with (~60p)
- **Khononov** — Chaps 1–2: Analyzing Business Domains + Discovering Domain Knowledge (~40p)
- **Central theme**: how to name NexoMart's domains correctly — `product`, `sales`,
  `warehouse`, `purchasing`, `marketing`, `person`, `humanresources` — before writing
  a single line of code. Ubiquitous language as a prerequisite, not a formality.

## Book Club T3.2 — "Bounded Contexts"

- **Khononov** — Chaps 3–4: Managing Domain Complexity + Integrating Bounded Contexts (~40p)
- **Evans** — Part IV (chaps 14–15): Strategic Design — Context Map, Bounded Contexts (~70p)
- **Central theme**: separating the project's real domains into true bounded contexts —
  where NexoMart's boundaries today are just Postgres schemas, and where they should
  actually be service boundaries.
- **Hook**: sets up the ground for any future ADR about service decomposition.

## Book Club T3.3 — "Tactical Building Blocks: Aggregates"

- **Evans** — Part II (chaps 5–7): Entities, Value Objects, Aggregates, Repositories (~90p)
  — the origin of these patterns
- **Khononov** — Chap 5: Implementing Aggregates (~40p) — the modern/practical version
- **Central theme**: a real Aggregate candidate in dbapi — `PurchaseOrderHeader` + detail
  as a case study.
- **Connects with ADR-007** (3-layer Repository/Service/Router architecture) already
  documented in the personal schedule.

## Book Club T3.4 — "Integration Patterns"

- **Khononov** — Chap 10: Design Patterns — Outbox, Saga, CQRS (~60p)
- **Evans**: no corresponding chapter — asynchronous integration patterns weren't
  formalized yet in the 2003 book. An episode that's practically 100% Khononov.
- **Central theme**: how this connects with Kafka as a buffer between the API and storage.
- **Connects with ADR-005** (Outbox Pattern for exactly-once with Kafka) already
  documented in the personal schedule.

## Open Next Steps

- Confirm whether the 4 sub-episodes all go out in the same week (1 week before the
  domain/decomposition Fast Track episode, as is already the rule for the other Book
  Clubs) or spread out.
- Script T3.1 first — it's the one that only depends on Khononov + Evans Part I, both
  already available.
- Rebuild (or not) the rest of the Consolidated Strategy in this file.
