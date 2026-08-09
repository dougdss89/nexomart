# Study Planner — Concrete Schedule

> **Start**: week of 07/07/2026
> **Reference**: `READING_SCHEDULE.md` (concepts) — this file is the executable calendar
> **Update**: at the end of each week, mark what got done and adjust the next block if needed

---

## How to Use This Planner

- **Weekdays**: 15–20 min. Not a study session — it's rhythm maintenance. Read without pressure.
- **Saturday**: 2–3h. Main session. This is where the deep reading + synthesis happens.
- **Sunday**: 1h optional. Lab, exercise, or review of highlights.
- **Phase deliverable**: at the end of each block, you have a concrete output (Book Club script, star schema drawn, AWS lab completed).

---

## MONTH 1 — JULY 2026
**Main focus**: Kimball Ch 1–4 (Pillar 4 — Reading)
**Secondary focus**: AWS Skill Builder — first modules (Pillar 1)
**Month deliverable**: NexoMart star schema drawn + Book Club T0 script draft

---

### WEEK 1 — July 07 to 13
**Kimball — Chapter 1: Dimensional Modeling Primer (~35p)**
Goal: understand why the dimensional model exists and what facts and dimensions are.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 07/07 | Weekday | Kimball Ch 1 — read up to the halfway point (~17p). Focus: what dimensional modeling is and why OLTP alone isn't enough | 20 min |
| Tue 08/07 | Weekday | Continue Ch 1 — facts, dimensions, grain. Highlight: the most important sentence so far | 20 min |
| Wed 09/07 | Weekday | Finish Ch 1. Note: which NexoMart tables would be facts and which would be dimensions | 15 min |
| Thu 10/07 | Weekday | Re-read Ch 1 highlights. Write the chapter synthesis in 3 bullets | 15 min |
| Fri 11/07 | — | Rest — no required reading | — |
| **Sat 12/07** | **Main session** | **Review Ch 1 (30min) → Start Ch 2 (~20p, first half) → Exercise: list NexoMart's 5 obvious dimensions** | **2h** |
| Sun 13/07 | Optional | AWS Skill Builder: create an account + complete the "AWS Cloud Practitioner Essentials" module (intro video) | 1h |

**Week deliverable**: 5 NexoMart dimensions identified + Ch 1 synthesis written

---

### WEEK 2 — July 14 to 20
**Kimball — Chapter 2: Retail Sales (~35p)**
Goal: apply the concepts to a retail case — directly analogous to NexoMart.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 14/07 | Weekday | Finish the first half of Ch 2 (if not done Saturday). Focus: how Kimball defines grain in retail | 20 min |
| Tue 15/07 | Weekday | Ch 2 second half — conformed dimensions, hierarchies, slowly changing preview | 20 min |
| Wed 16/07 | Weekday | Note: what would the grain of NexoMart's fact_sales be? (1 row per order_item? per order?) | 15 min |
| Thu 17/07 | Weekday | Write NexoMart's grain declaration in 1 sentence. Highlight 3 concepts from Ch 2 | 15 min |
| Fri 18/07 | — | Rest | — |
| **Sat 19/07** | **Main session** | **Review Ch 2 (30min) → Start Ch 3 — Inventory (~25p, full) → Exercise: sketch fact_sales with surrounding dimensions** | **2h30** |
| Sun 20/07 | Optional | AWS Skill Builder: IAM module — users, groups, roles, policies (hands-on in the Free Tier console) | 1h |

**Week deliverable**: fact_sales grain declaration written + fact_sales sketch with dimensions

---

### WEEK 3 — July 21 to 27
**Kimball — Chapter 4: Procurement + General Synthesis (~25p + review)**
Goal: wrap up Kimball, consolidate the star schema, and prepare Book Club T0.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 21/07 | Weekday | Ch 4 — Procurement (~12p, first half). Relate it to NexoMart's `purchasing` schema | 20 min |
| Tue 22/07 | Weekday | Finish Ch 4. Note: what NexoMart has in common with Kimball's procurement case | 20 min |
| Wed 23/07 | Weekday | General review: re-read all Ch 1–4 highlights. Answer the 3 synthesis questions | 20 min |
| Thu 24/07 | Weekday | Write the Kimball anchor sentence (the one that opens Book Club T0) | 15 min |
| Fri 25/07 | — | Rest | — |
| **Sat 26/07** | **Main session** | **Final Kimball synthesis (30min) → Draw the NexoMart Bus Matrix (5 facts × 9 dimensions) → Full Book Club T0 script draft** | **3h** |
| Sun 27/07 | Optional | Implement in the project: draft the gold tables (fact_sales, dim_customer, dim_product) in SQL or as code comments | 1h |

**Week deliverable**: Bus Matrix drawn + Book Club T0 script drafted

---

### July Summary

| Week | Reading | Lab/Practice | Deliverable |
|--------|---------|-------------|---------|
| 1 (07/07–13/07) | Kimball Ch 1 | AWS: create account + intro | 5 NexoMart dimensions identified |
| 2 (14/07–20/07) | Kimball Ch 2 + start Ch 3 | AWS: IAM in the console | Grain declaration + fact_sales sketch |
| 3 (21/07–27/07) | Kimball Ch 3 + Ch 4 + synthesis | Project: gold table sketches | Bus Matrix + Book Club T0 script |

---

## MONTH 2 — AUGUST 2026
**Main focus**: Fundamentals of Data Engineering — Reis & Housley (Pillar 4)
**Secondary focus**: AWS Phase A — S3 + VPC fundamentals (Pillar 1)
**Month deliverable**: lifecycle vocabulary consolidated + Book Club T1 script + S3 and VPC labs

---

### WEEK 4 — July 28 to Aug 03
**Fundamentals of DE — Ch 1 + 2 (~60p)**
Goal: understand the full lifecycle and the "undercurrents" that affect everything.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 28/07 | Weekday | Ch 1 — the data engineer's lifecycle. What phase of the cycle is NexoMart in? | 20 min |
| Tue 29/07 | Weekday | Continue Ch 1. Note: what "data generation" means for NexoMart | 20 min |
| Wed 30/07 | Weekday | Ch 2 — Undercurrents (security, quality, observability). Highlight: which ones are already in the project | 20 min |
| Thu 31/07 | Weekday | Finish Ch 2. Note: which undercurrents NexoMart still hasn't implemented | 15 min |
| Fri 01/08 | — | Rest | — |
| **Sat 02/08** | **Main session** | **Review Ch 1–2 (30min) → Start Ch 3 (~20p) → Exercise: map NexoMart's full lifecycle (landing → gold)** | **2h** |
| Sun 03/08 | Optional | AWS: S3 lab — create a bucket, configure versioning, lifecycle rule, server-side encryption | 1h |

---

### WEEK 5 — August 04 to 10
**Fundamentals of DE — Ch 3 + 8 (~90p)**
Goal: data architecture and transformations — connect to NexoMart's stack.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 04/08 | Weekday | Finish Ch 3 (if not done). Focus: what defines good data architecture | 20 min |
| Tue 05/08 | Weekday | Ch 8 — Queries and Transformations. Where dbt fits in here | 20 min |
| Wed 06/08 | Weekday | Continue Ch 8. Relate it to the project's Silver → Gold Spark jobs | 20 min |
| Thu 07/08 | Weekday | Ch 8 synthesis. How the Medallion Architecture shows up in the book | 15 min |
| Fri 08/08 | — | Rest | — |
| **Sat 09/08** | **Main session** | **Review Ch 3 + 8 (30min) → Full Ch 9 — Ingestion (~40p) → Relate it to NexoMart's FastAPI + Airflow** | **2h30** |
| Sun 10/08 | Optional | AWS: VPC lab — create a VPC with public/private subnets, NAT Gateway, Security Groups | 1h |

---

### WEEK 6 — August 11 to 17
**Fundamentals of DE — Ch 9 (wrap-up) + synthesis + Book Club T1**

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 11/08 | Weekday | Finish Ch 9. Compare the book's ingestion patterns with what you implemented | 20 min |
| Tue 12/08 | Weekday | General review of Fundamentals of DE: re-read all highlights | 20 min |
| Wed 13/08 | Weekday | Answer the 3 synthesis questions for the whole book | 15 min |
| Thu 14/08 | Weekday | Write the Fundamentals of DE anchor sentence (opens Book Club T1) | 15 min |
| Fri 15/08 | — | Rest | — |
| **Sat 16/08** | **Main session** | **Final Fundamentals of DE synthesis (30min) → Full Book Club T1 script → Update NexoMart's lifecycle with the book's vocabulary** | **2h** |
| Sun 17/08 | Optional | Databricks: create a Community Edition account + complete "Databricks Fundamentals Accreditation" (free, ~2h) | 1h30 |

### August Summary

| Week | Reading | Lab/Practice | Deliverable |
|--------|---------|-------------|---------|
| 4 (28/07–03/08) | Fund. of DE Ch 1–2 | AWS: S3 lab | NexoMart lifecycle mapped |
| 5 (04–10/08) | Fund. of DE Ch 3 + 8 | AWS: VPC lab | Medallion ↔ project connection |
| 6 (11–17/08) | Fund. of DE Ch 9 + synthesis | Databricks: Fundamentals Accreditation | Book Club T1 script + Fundamentals certificate |

---

## MONTH 3 — SEPTEMBER 2026
**Main focus**: DDIA — Kleppmann Ch 3–4 + 11 (Pillar 4)
**Secondary focus**: AWS Skill Builder — Glue + EMR + Athena intro (Pillar 1)
**Month deliverable**: Book Club T2 script + understanding of why Iceberg exists

---

### WEEK 7 — August 18 to 24 (transition)
**DDIA — Chapter 3: Storage and Retrieval (~35p)**
Goal: understand B-Tree vs LSM-Tree and why Parquet + Iceberg are the right choice.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 18/08 | Weekday | DDIA Ch 3 — first part: B-Tree (PostgreSQL indexes) vs LSM-Tree (Parquet/Iceberg) | 20 min |
| Tue 19/08 | Weekday | Continue Ch 3 — column-oriented storage (why Parquet is faster for analytics) | 20 min |
| Wed 20/08 | Weekday | Finish Ch 3. Note: why Iceberg is essentially an LSM-Tree | 20 min |
| Thu 21/08 | Weekday | Ch 3 synthesis + 3 bullets connecting it to the project | 15 min |
| Fri 22/08 | — | Rest | — |
| **Sat 23/08** | **Main session** | **Review Ch 3 (30min) → Full Ch 4 — Encoding and Evolution (~25p) → Relate to schema evolution in Iceberg** | **2h** |
| Sun 24/08 | Optional | AWS: Glue lab — create a Data Catalog, crawler over S3, schema evolution | 1h |

---

### WEEK 8 — August 25 to 31
**DDIA — Chapter 11: Stream Processing (~45p)**
Goal: understand why Kafka is at the center of event-driven architecture.

| Day | Session | What to do | Time |
|-----|--------|-------------|-------|
| Mon 25/08 | Weekday | DDIA Ch 11 — first part: what stream processing is vs batch | 20 min |
| Tue 26/08 | Weekday | Ch 11 — message brokers, log-based (Kafka) vs traditional MQ | 20 min |
| Wed 27/08 | Weekday | Ch 11 — stream joins, exactly-once semantics | 20 min |
| Thu 28/08 | Weekday | Finish Ch 11. Note: why the Outbox Pattern solves the "dual write problem" | 15 min |
| Fri 29/08 | — | Rest | — |
| **Sat 30/08** | **Main session** | **Full DDIA synthesis (30min) → Answer the 3 synthesis questions → Book Club T2 script draft** | **2h** |
| Sun 31/08 | Optional | AWS: Kinesis vs MSK lab — spin up a simple producer/consumer on the Free Tier | 1h |

---

### Weeks 9–10 — September (02 to 14/09)
**Buffer + AWS Phase A wrap-up + start of hands-on Databricks**

| Week | Focus | What to do |
|--------|------|-------------|
| 9 (02–07/09) | AWS Phase A | Complete CloudWatch + CloudTrail + KMS labs. Review IAM, S3, VPC hands-on |
| 10 (08–14/09) | Databricks | Databricks Community notebook: reproduce the project's Silver → Gold jobs in Databricks. Compare with the local code |

### September Summary

| Period | Reading | Lab/Practice | Deliverable |
|---------|---------|-------------|---------|
| 18–24/08 | DDIA Ch 3 + 4 | AWS: Glue lab | Parquet/Iceberg ↔ storage theory connection |
| 25–31/08 | DDIA Ch 11 | AWS: Kinesis lab | Book Club T2 script |
| 01–14/09 | — buffer — | AWS: close out Phase A / Databricks: jobs in Community | AWS Phase A complete |

---

## MONTH 4 — OCTOBER 2026
**Main focus**: DDD Evans (Pillar 4) + start of AWS Phase B
**Secondary focus**: Databricks — advanced Spark Core
**Month deliverable**: NexoMart's bounded contexts documented

---

### Weeks 11–13 — October (05 to 25/10)
**DDD Evans — Parts I + II + IV (~220p)**

| Week | Focus | Chapters | Deliverable |
|--------|------|-----------|---------|
| 11 (05–11/10) | Evans Part I | Ch 1–3: Ubiquitous Language | NexoMart domain glossary |
| 12 (12–18/10) | Evans Part II | Ch 5–6: Entities, Value Objects, Aggregates | Identify aggregate roots across the 7 schemas |
| 13 (19–25/10) | Evans Part II + IV | Ch 7: Repositories + Ch 14–15: Bounded Contexts | NexoMart Context Map drawn |

**Parallel AWS weeks 11–13**: AWS Skill Builder — Glue Studio + EMR fundamentals
**Parallel Databricks**: Advanced Spark — Catalyst optimizer, EXPLAIN, AQE

---

## MONTH 5 — NOVEMBER 2026
**Main focus**: Learning DDD — Khononov Ch 1–6 + 10 (Pillar 4)
**Secondary focus**: AWS Phase B — Athena + Lake Formation + Redshift
**Month deliverable**: Book Club T3 script + Outbox Pattern implemented in the project

---

### Weeks 14–16 — November (02 to 22/11)

| Week | Focus | Chapters | Deliverable |
|--------|------|-----------|---------|
| 14 (02–08/11) | Khononov Ch 1–2 | Business Domains + Domain Knowledge | Refine Context Map with updated vocabulary |
| 15 (09–15/11) | Khononov Ch 3–5 | Bounded Contexts + Aggregates | Aggregate implementation sketch in Python |
| 16 (16–22/11) | Khononov Ch 6 + 10 | Ch 6 (events) + Ch 10 (Outbox, Saga, CQRS) | Book Club T3 script + Outbox sketch in the project |

**Parallel AWS**: Athena v3 lab (native Iceberg), Lake Formation fine-grained access
**Parallel Databricks**: Delta Live Tables intro

---

## MONTH 6 — DECEMBER 2026
**Main focus**: Hard Parts + GoF (Pillar 4)
**Secondary focus**: AWS Phase B wrap-up — DMS + MWAA + MSK
**Month deliverable**: Book Club T4 + T5 scripts

---

### Weeks 17–20 — December

| Week | Focus | What to study | Deliverable |
|--------|------|--------------|---------|
| 17 (30/11–06/12) | Hard Parts Ch 1–2 | The hard parts + finding boundaries | Identify NexoMart's trade-offs |
| 18 (07–13/12) | Hard Parts Ch 3–4 + 6 + 9 | Decomposition + CQRS + data access | Book Club T4 script |
| 19 (14–20/12) | GoF — Intro + patterns | Intro + Repository + Factory + Observer | Name the patterns in the project's code |
| 20 (21–27/12) | GoF — synthesis | Review + synthesis + script | Book Club T5 script |

**December parallel AWS**: complete the DEA-C01 study plan (Maarek). Schedule the exam for Jan/Feb 2027
**December parallel Databricks**: Databricks Spark Developer study plan (Spark: The Definitive Guide + exercises)

---

## 2027 — Consolidation and Certifications

### Jan–Mar 2027
| Focus | What to do | Goal |
|------|-------------|------|
| Reading | Fluent Python (Ch 13 + 17 + 21) + Book Club T6 | Book Club T6 script |
| AWS | Schedule and take DEA-C01 (exam) | **DEA-C01 certification** |
| Databricks | Schedule and take Spark Developer (exam) | **Spark Developer certification** |
| Project | Async generators in the seeders + refactoring with protocols | More Pythonic code |

### Apr–Jun 2027
| Focus | What to do | Goal |
|------|-------------|------|
| AWS | AIF-C01 study plan (Maarek + AWS Skill Builder AI path) | AIF-C01 study |
| Databricks | Data Engineer Associate study plan (Databricks Academy) | Associate study |
| Project | Reproduce the full NexoMart pipeline in Community Edition | Databricks pipeline working |
| AWS Lab | Bedrock Workshop + RAG over the NexoMart catalog | Concrete AIF-C01 project |

### Jul–Sep 2027
| Focus | What to do | Goal |
|------|-------------|------|
| AWS | Schedule and take AIF-C01 (exam) | **AIF-C01 certification** |
| Databricks | Schedule and take Data Engineer Associate (exam) | **Data Engineer Associate certification** |
| Project | SageMaker: train a recommendation model over fact_sales | ML pipeline running |

### Oct–Dec 2027
| Focus | What to do | Goal |
|------|-------------|------|
| AWS | MLA-C01 study plan (AWS Skill Builder ML Engineer path) | MLA-C01 study |
| Databricks | Data Engineer Professional study plan (advanced labs) | Professional study |
| AWS Project | NexoMart on AWS — Phase C (full Terraform) | Real AWS deploy |
| Databricks Project | DLT + Workflows + DAB — NexoMart pipeline at Professional level | Prod-ready pipeline |

---

## 2028 — Full Professional Level

### Jan–Mar 2028
| Focus | What to do | Goal |
|------|-------------|------|
| AWS | Schedule and take MLA-C01 (exam) | **MLA-C01 certification** |
| Databricks | Schedule and take Data Engineer Professional (exam) | **Data Engineer Professional certification** |
| Project | SageMaker endpoint consumed by NexoMart's FastAPI | Full AI-ready platform |

---

## Certification Map

```
AWS
────────────────────────────────────────────────────────────
CLF-C02  (optional)
    ↓
SAA-C03  ──── Architectural foundation (Q1 2027)
    ↓
DEA-C01  ──── Data on AWS: Glue, EMR, Athena, MSK (Q1 2027)
    ↓
AIF-C01  ──── AI Practitioner: Bedrock, SageMaker overview (Q3 2027)
    ↓
MLA-C01  ──── ML Engineer: deep SageMaker, MLOps (Q1 2028)

DATABRICKS
────────────────────────────────────────────────────────────
Fundamentals Accreditation  (free — do it early)
    ↓
Spark Developer Associate  ──── Spark Core, joins, UI (Q1 2027)
    ↓
Data Engineer Associate    ──── Delta, DLT, Unity Catalog (Q3 2027)
    ↓
Data Engineer Professional ──── Prod-grade, MLflow, DAB (Q1 2028)
```

---

## Progress Tracker

Copy and update this section every week:

```
KIMBALL
[ ] Ch 1 — read
[ ] Ch 2 — read
[ ] Ch 3 — read
[ ] Ch 4 — read
[ ] Synthesis written
[ ] NexoMart Bus Matrix drawn
[ ] Book Club T0 script drafted

FUNDAMENTALS OF DE
[ ] Ch 1–2 — read
[ ] Ch 3 — read
[ ] Ch 8 — read
[ ] Ch 9 — read
[ ] Synthesis written
[ ] Book Club T1 script drafted

DDIA
[ ] Ch 3 — read
[ ] Ch 4 — read
[ ] Ch 11 — read
[ ] Synthesis written
[ ] Book Club T2 script drafted

DDD EVANS
[ ] Part I (Ch 1–3) — read
[ ] Part II (Ch 5–7) — read
[ ] Part IV (Ch 14–15) — read
[ ] NexoMart Context Map drawn

KHONONOV
[ ] Ch 1–2 — read
[ ] Ch 3–5 — read
[ ] Ch 6 + 10 — read
[ ] Book Club T3 script drafted

HARD PARTS
[ ] Ch 1–4 — read
[ ] Ch 6 + 9 — read
[ ] Book Club T4 script drafted

GOF
[ ] Intro — read
[ ] Repository + Factory + Observer — read
[ ] Book Club T5 script drafted

FLUENT PYTHON
[ ] Ch 13 — read
[ ] Ch 17 — read
[ ] Ch 21 — read
[ ] Book Club T6 script drafted

AWS — FUNDAMENTALS
[ ] Account created + Free Tier active
[ ] IAM — lab completed (users, roles, policies, STS)
[ ] S3 — lab completed (lifecycle, versioning, encryption)
[ ] VPC — lab completed (subnets, SGs, NAT, VPC endpoints)
[ ] CloudWatch + CloudTrail — lab completed
[ ] KMS — lab completed

AWS — DATA (DEA-C01)
[ ] Glue — lab completed (crawler, catalog, ETL job)
[ ] EMR — lab completed (Spark job on the cluster)
[ ] Athena — lab completed (native Iceberg, workgroup)
[ ] Lake Formation — lab completed (fine-grained access)
[ ] Kinesis / MSK — lab completed (producer + consumer)
[ ] DMS — lab completed (CDC from PostgreSQL)
[ ] MWAA — lab completed (DAG on managed Airflow)
[ ] Cantrill SAA-C03 — course completed
[ ] Maarek DEA-C01 — course completed
[ ] SAA-C03 — passed [x]
[ ] DEA-C01 — passed [x]

AWS — AI (AIF-C01 → MLA-C01)
[ ] Bedrock Workshop — lab completed (RAG over the catalog)
[ ] SageMaker Immersion Day — lab completed
[ ] SageMaker Feature Store — lab completed
[ ] SageMaker Pipelines — lab completed
[ ] NexoMart RAG with Bedrock Knowledge Bases — implemented
[ ] Recommendation model in SageMaker — trained and deployed
[ ] Maarek AIF-C01 — course completed
[ ] AWS Skill Builder ML Engineer path — completed
[ ] AIF-C01 — passed [x]
[ ] MLA-C01 — passed [x]

AWS — PROJECT
[ ] NexoMart on AWS — full Terraform (Phase C)

DATABRICKS
[ ] Community Edition — account created
[ ] Fundamentals Accreditation — obtained [x] (free)
[ ] NexoMart pipeline reproduced in Community Edition
[ ] Catalyst + EXPLAIN EXTENDED — studied
[ ] Broadcast join vs Sort-Merge join — mastered
[ ] AQE (Adaptive Query Execution) — studied
[ ] Spark UI — can read the DAG and spot bottlenecks
[ ] Advanced Structured Streaming — studied
[ ] Delta Live Tables — pipeline implemented
[ ] Unity Catalog — fine-grained access configured
[ ] Databricks Workflows — orchestration implemented
[ ] Liquid Clustering — applied in the project
[ ] Databricks Asset Bundles — pipeline deployed via DAB
[ ] MLflow — experiment logged + model in the registry
[ ] "Spark: The Definitive Guide" — critical chapters read
[ ] "Delta Lake: The Definitive Guide" — critical chapters read
[ ] Spark Developer Associate — passed [x]
[ ] Data Engineer Associate — passed [x]
[ ] Data Engineer Professional — passed [x]
```

---

*Created 2026-07-04 | Update weekly*
*Reference: `READING_SCHEDULE.md`*
