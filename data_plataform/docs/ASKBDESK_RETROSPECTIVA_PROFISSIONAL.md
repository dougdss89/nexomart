# Professional and technical retrospective — Ask Bdesk project (GAB)

**Context:** this document looks back, impartially, at the body of work done in the
Ask Bdesk / Customer Success domain (Bronze->Silver->Gold pipeline, KPIs, 3 Genie Agents
in production, 4 Lakeview dashboards) over ~2026-08-25 to 2026-09-18. It complements
[`LESSONS_LEARNED_GAB_DATAOPS.md`](LESSONS_LEARNED_GAB_DATAOPS.md), which covers what to
port from GAB's DataOps pipeline to the IcebergLake. This one is about me: what the real
patterns in that work say about strengths and gaps, both technical and process.

---

## 1. Strengths confirmed by evidence (not self-assessment)

### 1.1 Root-cause discipline, not just symptom
Repeatedly, the reported problem was not the real cause, and that was found by
investigation, not assumed:
- Card 01 ("wrong group") -> the `orquestrador_silver_gold_sucesso_cliente` pipeline had
  been paused for 5 days, not a group-resolution bug.
- "Duplicate group" in Genie -> a collision of different `id_grupo_atual` resolving to the
  same name (`kpi_10_sla_backlog_por_grupo.py`), not a prompt problem.
- Incremental-load investigation -> the source was already near-real-time CDC; the lag was
  from the full `.createOrReplace()` pattern on Silver, not from the source.
- DQ-G-004 quarantine (13,529 rows) -> not a business-date inversion, but a precision/order
  artifact between two near-simultaneous automated event timestamps.

In none of these was the obvious explanation accepted at face value — all were confirmed
with a direct query against the real data before proposing a fix.

### 1.2 Knowing when NOT to decide alone
Several items were deliberately blocked on a missing business decision, rather than
inventing a plausible rule: exposing individual data per analyst (Card 02), redefining the
official Backlog KPI (Card 20), SLA methodology (Gold vs. BI, a residual of 204 tickets
never forced to reconcile), the "overload" criterion (never defined anywhere in the data).
This is the opposite of the common anti-pattern of "code decides the business rule by
omission".

### 1.3 Live testing as the closing criterion, not the written instruction
The consistent pattern — write the rule, run `genie start-conversation` with a real
question, check the generated SQL and the number — caught at least two failures a reading
of the instruction would not: Card 18 (a scope rule written but with no practical effect,
2 attempts) and the discovery that the V11 fix had not propagated to 4 other mentions of
`descricao_status` in the same space.

### 1.4 Documenting the gap instead of pretending it doesn't exist
Supplier, Viva+30, Time Tracking, Standout Analysts — all marked explicitly as "no data
source" on the dashboards instead of being estimated or silently omitted. Same principle
applied to ISO/Alerts (Nexa's FCR score excluded for lack of a base, not estimated).

---

## 2. Technical and process gaps (the part that requires being impartial)

### 2.1 Direct production changes with no review trail
All of the ~80+ instruction changes to the Genie Agents (25 cards + 12 VL + 16 D02 + 18
CARD04 + the most recent TS001 rule) were applied via `genie update-space` straight into
the production space, on the same day as a single person's verbal/chat confirmation
(usually Ewerton). There is no PR, no diff reviewed by a second person, no versioned
rollback beyond session memory. This is exactly the pattern
[`LESSONS_LEARNED_GAB_DATAOPS.md`](LESSONS_LEARNED_GAB_DATAOPS.md) describes as mature best
practice (dual approval, allowlist, auditable trail) — but the Ask Bdesk work itself did
not follow it, because no such pipeline existed for this domain. **The already-documented
lesson never closed the loop with practice.**

### 2.2 Structural duplication never resolved, only discovered
`genie_governanca/publish_instructions.py` and its README (2026-09-16) document that
Facilitador and Supervisao have ~15 identical business rules written by hand, duplicated,
in each space — and that Lideranca and Customer Success KPIs fell *light-years* behind in
coverage of the same rule. That kit was built but **never applied to any space** (confirmed
in its own README: "Not applied to any space"). Practical result: the Card 06 rule (a
metric-change disclaimer) was written in Lideranca, forgotten in Supervisao/Facilitador for
days, and only replicated when a *different* validation item (V02) reopened the same gap.
This is reactive, not systemic — the mechanism to prevent exactly this kind of oversight was
already designed and left on the shelf.

### 2.3 No regression harness
All validation is "1 live question, 1 checked answer". There is no benchmark set of
questions run automatically after each instruction change (the Genie Agents skill itself
recommends >=30 benchmark items before considering a space stable — never built here).
Observed consequence in practice: rules that worked broke again with nobody noticing until
the next batch of GAB cards flagged it again (e.g., Customer Success scope had 3 rounds of
"closed" before becoming a structural column instead of an instruction).

### 2.4 Text instruction as the first reflex, not the last resort
The very skill used in this project (`databricks-genie-agents`) documents "instruction text
is a last resort" — but the real pattern of the work was: try to solve via text first, and
only escalate to a structural change (new column, new JOIN) after one or two confirmed
failures (Card 18, "group vs. team", took 2 instruction attempts before becoming
`equipe_sucesso_cliente` in the table). The end result was good, but the path there cost
avoidable cycles — worth internalizing the rule "if a rule names a specific
table/column/join, it should probably be born structural, not as text".

### 2.5 Business-rule gathering done late, not before building
Official Backlog, "overload", SLA methodology, priority mapping, and now the TS001 rule
("operation today" excludes completed/future) — in every case the business definition only
appeared **after** someone had already used the KPI/agent and reported a discrepancy. None
of these had a formal "confirm the definition with GAB before implementing" step — the cycle
was always build -> user complains -> investigate -> formalize. Cheaper builds in the short
term, but the real cost showed up in rework (SLA was revisited in at least 4 different card
rounds without ever closing for good).

### 2.6 Two kinds of consumer, a single mental checklist
At least once (the `_sugestao`->official table rename on 2026-09-15), the Lakeview dashboard
broke silently because the "who consumes this table" checklist only covered the Genie Spaces,
not the dashboards. It was caught and fixed the same day, but by luck of review, not by a
process that guaranteed covering both kinds of consumer whenever a Gold table is renamed.

---

## 3. Technical-skill read (where the bar is today)

**Strong and demonstrable:**
- Data debugging in the Lakehouse layers (Bronze/Silver/Gold, DLT/Lakeflow, CDF/CDC,
  resolution cascades with fallback).
- Prompt/instruction engineering for conversational agents over structured data (Genie),
  including the real limits of what text instructions solve vs. what requires a schema change.
- Lakeview/AI-BI: calculated measures, multi-dataset filters, naming pitfalls
  (case-insensitive collision), all documented with root cause, not just "it worked".
- Technical communication for non-technical audiences: per-card closing PDFs, ER-model
  artifacts generated live from `information_schema`, always citing a real finding (number,
  ticket, screenshot) instead of generalities.

**Still nascent, with concrete evidence of the gap:**
- *Applied* platform/governance engineering (not just documented) — CI Gates, allowlist,
  dual approval all exist as a well-understood concept (sections 1-2 of the other document)
  but none of it was implemented in the Ask Bdesk domain itself.
- Automated/regression testing for prompt-driven systems — the "test live" discipline is
  good, but it doesn't scale and isn't repeatable; it's the clearest gap between "I can do
  it" and "I can systematize it".
- Requirements management before building — the builder-first pattern is fast to prototype
  but costly in rework when the domain (SLA, backlog, overload) has multiple plausible
  definitions and no clear owner before the fact.

---

## 4. Concrete improvement actions (prioritized, not a wish list)

1. **Apply my own consolidation kit** (`genie_governanca/`) to at least one space, starting
   with the most out-of-date (Customer Success KPIs) — close the loop between "I documented
   the duplication" and "I resolved the duplication". The work is already done, just not
   published.
2. **Build a minimal benchmark (10-15 questions) per space**, versioned in a file, run
   manually before closing any new card — it doesn't need to be the skill's full automated
   eval, but it needs to stop depending only on "I tested one question just now".
3. **Before writing a text rule that names a specific table/column/join**, explicitly ask
   "should this be a column?" before publishing — apply the section 2.4 rule at the start of
   the cycle, not after two failures.
4. **A versioned changelog per space** (even just one Markdown file per Genie Space, with
   date + rule + who confirmed) — it doesn't replace real CI Gates, but gives an auditable
   trail without waiting for the formal pipeline (which is blocked by 8 Tooling & Ops items,
   outside the Division's control).
5. **A "2 consumers" checklist** whenever a Gold table is renamed/removed: Genie Spaces
   (`data_sources.tables[]`) AND Lakeview dashboards (`queryLines`) — both, always, not one
   of the two from memory.
