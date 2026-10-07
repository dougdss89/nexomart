# Lessons from GAB's DataOps pipeline — what to bring into IcebergLake

**Context:** while investigating how to automate report delivery via Databricks Genie for the
Ask Bdesk project (client/consulting), I ended up reading the full data-governance
documentation of an organization (GAB) that already runs a mature DataOps pipeline in
production — and applying part of it in a real prototype. This document is not about Ask
Bdesk; it is about what from that pipeline is worth porting into **IcebergLake**, which is
my own project.

---

## 1. What the GAB pipeline does well (technical summary)

Observed architecture: YAML as the source of truth -> PR with blocking CI Gates -> merge ->
automatic codegen (YAML->Terraform) -> automatic PR with the generated `.tf` -> dual
approval -> merge to `main` -> automatic Terraform Apply in production. Their guiding
principle, worth quoting because it sums everything up: **"If it passed the PR, it doesn't
break in `main` or in PRD."**

What sustains that in practice, in order of relevance to my project:

### 1.1 Per-layer guardrails, with increasing rigor Bronze->Silver->Gold
Automatic (blocking) validation that prevents ungoverned `OVERWRITE`, `TRUNCATE`,
`DROP TABLE/DATABASE`, `DELETE` without `WHERE`, recursive `dbutils.fs.rm` — **unless a
versioned allowlist exists, with justification, approver, and expiration date**. Gold has
stricter rules than Silver (e.g., `DELETE` without `WHERE` is a warning in Silver, an error
in Gold; `impact_assessment` is required only in Gold).

**Why it matters:** today my project has no static protection against an accidental
`df.write.mode("overwrite")` in a `gold_layer_scripts/` script. The "protection" is just
manual discipline.

### 1.2 Allowlist with mandatory expiration — never a permanent waiver
Every guardrail exception becomes a versioned YAML entry (`justification`, `approved_by`,
`approved_at`, `expires_at`, `ticket`). An expired item automatically goes back to blocking,
with no manual action. This avoids the classic "temporary exception" pattern that is never
reverted.

### 1.3 DEV->PRD portability discipline
Simple, powerful rule: **no hardcoded environment string in the code** (catalog, cluster id,
`/Repos/DEV/` path). I found and fixed this exact bug in my own prototype during the
investigation — a notebook with `cat_holding_silver_dev` hardcoded, which would break in
production. The right pattern is: the environment always comes from a parameter/variable,
never from a literal string.

### 1.4 Codegen — the YAML is the only thing a human edits
"The developer never writes Terraform by hand." A script converts the declarative YAML
(jobs, pipelines) into `.tf`, runs it in `--dry-run` mode on the PR (validates without
applying), and only generates the real `.tf` after merge. This eliminates the entire bug
class of "hand-written Terraform drifted from the declared YAML".

### 1.5 Dual approval with different roles, not duplicated ones
A PR to production requires **two** approvals with different scopes: the Division (is the
business rule correct? functionally validated?) and the Platform (technical standards,
security/secrets, cost, consistency with other repos). It is not "two people approve the
same thing" — it is "two different perspectives, neither reducible to the other".

### 1.6 Diff-aware CI
PR validations run only over the files changed in the diff — a PR doesn't fail because of a
pre-existing problem in code it didn't even touch. Reduces friction without giving up
blocking what really matters in that PR.

### 1.7 Naming as a mechanism, not aesthetics
The repository naming standard (`<org3>-<domain>-<product>-<purpose>`) is not just visual
standardization — it is a structurally parseable rule (each field is a single token, with no
internal hyphen) that exists so automation can split the fields unambiguously. I violated
it myself on the spot (I used `sucesso-cliente` as the "product", which has an embedded
hyphen) and only noticed on rereading the standard carefully. **Lesson:** a naming
convention is only worth anything if it can be validated/parsed automatically — otherwise it
becomes a suggestion nobody checks.

---

## 2. What can be ported into IcebergLake, concretely

| GAB pattern | Where to apply in IcebergLake | Effort |
|---|---|---|
| Guardrail against destructive writes in Gold | Static check (pre-commit or `pr-checks.yml`) scanning `spark/jobs/gold_layer_scripts/**.py` for `overwrite`, `TRUNCATE`, `DROP TABLE`, `.mode("overwrite")` without explicit confirmation | Medium — reuses the lint infra that already exists (`.pre-commit-config.yaml`, `scripts/lint.sh`) |
| Allowlist with expiration | A simple `allowlist.yml` per layer (`bronze`/`silver`/`gold`), checked by the same script above | Low, once the guardrail exists |
| Portability audit (no hardcoded environment) | Grep/AST check looking for strings like `s3a://bronze-dev/`, `iceberglake_dev`, fixed hosts, instead of vars/`.env` | Low — worth running this now, before the project grows further |
| Declarative YAML + codegen | If the project ever gains Terraform (today it's `docker-compose`/Makefile), it's worth being born with "YAML describes, script generates .tf" rather than hand-written Terraform from the start |
| Diff-aware in `pr-checks.yml` | Check whether the current checks (`ci.yml`, `pr-checks.yml`) already ignore files not touched in the PR — if not, it's an easy friction win |
| Spark anti-pattern detector (`.collect()`, `print()` without logging) | A custom lint rule (Ruff/AST) for the scripts in `spark/jobs/**` — same pattern used in the prototype I built (I found both anti-patterns in my own code before delivering) |
| "Fan-out" parameterization (1 generic job + dynamic list) | Maps directly to Airflow's **dynamic task mapping** — the "don't hardcode the list, read it from a table/config and generate one task per item" pattern is the same rationale as Databricks Jobs' `for_each_task` |
| Per-environment secret isolation with approval | Check whether `cd.yml` already uses GitHub Environments with *required reviewers* for "prod" secrets (production MinIO/Postgres, if that concept exists in the project) |

---

## 3. Process lessons (how I worked, not just what I learned)

These came from observing what worked (and what nearly went wrong) during the investigation
itself, not just from reading the documentation:

1. **Verify against the real system before writing code.** I ran the actual question in
   Genie, queried the tables' real schema, checked which catalogs actually existed — instead
   of assuming from the docs. This caught two real bugs: an `ImportError` for an SDK class
   that only existed in my local version (not the serverless cluster's), and the hardcoded
   catalog that would break in PRD. **Neither would surface from reading documentation
   alone.**
2. **When docs and reality diverge, flag it — don't "fix" it silently.** The catalog
   standard (`cat_<division>_<layer>`) doesn't match the workspace's real catalogs (`_dev`/
   `_prd` in the name). It makes no sense for me to "fix" this alone — it's a divergence that
   needs to go to the process owner (Tooling & Ops), with the fact recorded, not hidden.
3. **Don't fabricate what isn't mine to fabricate.** When I found there was no
   repository/pipeline for the domain, the temptation was to create invented
   `.github/workflows/` "to look complete". I didn't — I documented it as an explicit
   placeholder, because simulating a working pipeline that doesn't exist is worse than
   admitting the gap.
4. **Test in safe mode before any real side effect.** The email job was born with
   `metodo_envio=none` (dry-run, only writes a file) as the default, and only gains the
   ability to actually send when someone changes the parameter explicitly — after human
   authorization exists. **Nothing dangerous by default.**
5. **A convention is only useful if it's verifiable.** I got the repository naming wrong on
   the first attempt because I didn't apply the rule letter by letter — it just "seemed
   reasonable". The fix came from rereading the standard with attention to the concrete
   examples, not the prose description.

---

## 4. For the resume/portfolio

What I described above — per-layer guardrails, allowlist with expiration, portability
discipline, dual approval with distinct roles — is the vocabulary and practice of
**DataOps/Platform Engineering in real production**, not course theory. It's explicitly
worth:

- Documenting a "Governance" section in the IcebergLake README that **implements** (not just
  cites) at least the destructive-write guardrail in Gold + the allowlist with expiration —
  a concrete, demonstrable differentiator in an interview, far stronger than "I know Spark
  and Airflow".
- If possible, writing a short post/note about the "YAML as source of truth + codegen for
  IaC" pattern — a concept that shows up in Databricks Asset Bundles, but whose rationale
  (never hand-edit the generated artifact) is generic and applies to any stack with
  Terraform.
- The habit of "verify against the real system before writing" (item 3.1) is the kind of
  practice that separates senior from junior engineering — worth naming explicitly as
  something I practice, not just doing without recording it.
