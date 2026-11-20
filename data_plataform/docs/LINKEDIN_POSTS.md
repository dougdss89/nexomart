# LinkedIn posts: design patterns series and idea backlog

Working file. Post bodies are in Portuguese (the publishing language); the rest is in English.
Rules from `docs/CONTENT_STRATEGY.md`: long text, no external link in the post, one question at the end.

Sources: `data_plataform` (Spark, seeders, infra), `dev_platform` (Databricks, DABs, Terraform),
`sparkdeveloper` (Spark study project).

Status legend: DRAFT = text written, not published. IDEA = only a hook.

---

## 1. Design patterns series

### Order

| # | Pattern | Status | Code to show |
|---|---|---|---|
| 1 | Strategy | DRAFT | `dev_platform/core/common/pipeline.py` |
| 2 | Factory | DRAFT | `dev_platform/domains/customer/pipeline.py` |
| 3 | Composite | DRAFT | `data_plataform/spark/src/common/transformer.py`, `domains/person/silver/customer.py` |
| 4 | Template Method | IDEA | `Pipeline.run`: fixed order read, transform, write |
| 5 | Decorator | IDEA | `Logging*` wrappers (reader, transformer, writer) |
| 6 | Dependency injection | IDEA | Reader/Writer passed in the constructor |

### What is actually in the code (keep posts accurate)

- **Strategy:** `Pipeline` receives a `BaseReader`, a `list[BaseTransformer]` and a `BaseWriter`.
- **Factory:** it is a *simple factory* (a function), not the GoF Factory Method. Say so in the post.
- **Composite:** `CompositeTransformer` is defined in `dev_platform`, but its factories pass a plain list.
  The real use is in `data_plataform` (Silver `customer`, Bronze jobs, Gold `customer`).
  Nested composites are not used yet.

### Post 1: Strategy (DRAFT)

```
Meu pipeline tem um motor só. O que muda é o que eu encaixo nele.

No meu projeto Databricks, toda tabela passa pelo mesmo método:
ler -> transformar -> gravar.

O que varia são três peças que eu injeto no construtor:
- um Reader (TableReader ou VolumeParquetReader)
- uma lista de Transformers
- um Writer (TableWriter em append ou TableMergeWriter com MERGE idempotente)

Landing -> raw e raw -> trusted rodam o MESMO Pipeline.
Só mudam as três peças.

Isso é o padrão Strategy: cada peça é uma estratégia que
segue um contrato (BaseReader, BaseTransformer, BaseWriter).
Para mudar o comportamento, eu troco o objeto. Não edito if/else.

O custo é real: mais classes e mais arquivos.
Só compensa quando você tem 3 ou mais variações do mesmo passo.
Com 2, um if resolve.

Você já trocou um if/else gigante por classes? Valeu a pena?
```

### Post 2: Factory (DRAFT)

```
Quem monta o pipeline não é o pipeline. É uma função.

No projeto, cada transição de camada tem uma factory:
- build_customer_raw_ingestion (landing -> raw)
- build_customer_trusted (raw -> trusted)

Cada uma monta UM Pipeline: reader, lista de transformers e writer.
O entrypoint do job fica fino: chama a factory e dá .run().

Adicionar um domínio novo = escrever uma factory nova.
O motor não muda.

Detalhe honesto: isso é uma "simple factory" (uma função),
não o Factory Method do livro do GoF. Eu não preciso de hierarquia
de classes para isso, e não inventei uma.

Duas coisas que aprendi montando a lista de transformers:
1. A ORDEM é decisão de projeto. Cast antes de normalizar.
   Dedup antes de dropar as colunas, porque a coluna de desempate
   (inserted_date) ainda precisa existir.
2. Dá para testar a montagem sem ler nenhum dado: o teste confere
   que a factory entregou o reader, o writer e os transformers certos.

Como você decide onde termina a factory e começa o job?
```

### Post 3: Composite (DRAFT)

```
Um grupo de transformações que se comporta como UMA transformação.

No meu pipeline Spark, todo passo implementa o mesmo contrato:
transform(df) -> df.

O CompositeTransformer também implementa esse contrato,
mas por dentro guarda uma lista de outros transformers e roda em sequência.

Resultado: para quem chama, um passo e um grupo de 12 passos são iguais.
- posso aninhar grupos dentro de grupos
- posso envolver o grupo inteiro com um LoggingTransformer
- posso reaproveitar um bloco ("normalizar nomes") em vários domínios

No Silver de customer, são 12 passos encadeados:
KeepLatest, ParseJson, Flatten, NormalizeGender, NormalizeToTimestamp...
cada um embrulhado em logging.

Isso é o padrão Composite: o grupo e a peça têm o mesmo contrato.

Ainda não aninhei grupos de verdade no projeto. Hoje ele serve
para ter um único objeto "transformação" na entrada do Pipeline.
Esse é o próximo refactor.

Onde você já usou "grupo que age como peça"?
```

### Check before publishing

- [ ] `build_customer_trusted` has `NormalizeString` listed twice in a row. Remove the duplicate before showing the code.
- [ ] Confirm the "12 steps" count in Silver `customer` on the day of publishing.
- [ ] No credentials, disk paths or Windows username in screenshots.
- [ ] Per the strategy doc: publish on Medium first, wait about 3 days, then share on LinkedIn.

---

## 2. Idea backlog (hooks only)

### A. Mistakes and lessons (highest reach)
1. Airflow green, table never written: the script swallowed the exception and exited 0 (`dim_customer`).
2. Postgres "lost" 140 GB: it started with the disk unmounted and ran `initdb` on an empty directory. A sentinel file in the entrypoint fixed it.
3. Hours blaming Docker/WSL, but the USB disk was failing writes (Ntfs Event 141).
4. `pandas chunksize` does not stream: OOM in the worker. A named server-side cursor fixed it.
5. A fix applied by hand and lost on container recreate (Postgres tuning, `make stage`).

### B. Architecture decisions
1. Why Delta-only: Iceberg to Delta and back twice, until the decision was written down.
2. Removing Unity Catalog OSS: nothing really depended on it, the catalog is the Hive Metastore.
3. 45-day retention is safe only because ingestion never reads behind the watermark.
4. Reader, Writer and Transformer with Decorator and Composite in Spark.
5. Jars baked into the image instead of Ivy (Spark 4.1 upgrade).

### C. Synthetic data and chaos engineering
1. Dirty data on purpose: 5 to 10% anomalies, documented per table.
2. A chaos anomaly that violates a constraint kills the whole batch.
3. Customer metrics derived from a latent engagement level.
4. `created_at` spread from 2020 to now, with `updated_at = now()` mandatory.
5. Bugs that only appear at real volume (asyncpg 32,767 parameter limit, CNPJ overflow).

### D. Spark and ingestion
1. Incremental ingestion with a watermark.
2. Upgrade to Spark 4.1.1 and Delta 4.1: ANSI off on purpose, DataFlint `spark4`, Ivy permission error.
3. Iceberg vectorised reader SIGSEGV, fixed by a table property.
4. A hardcoded 12g executor on an 8g worker.
5. About 69k small files after a re-land.

### E. Local infra
1. 381 GB freed: partition retention, `fstrim` and nightly `Optimize-VHD` (VHDX 601 to 425 GB).
2. One table with about 800x normal volume (276M rows per month).
3. The recurring WSL popup: a detached process does not survive, only a foreground session does.
4. Bind mounts on a late-attached disk (`make stage`).
5. Identify a disk by ext4 UUID, never by `sdX`.

### F. `sparkdeveloper` (study and career)
1. A 7-module Spark study plan: architecture, SQL, DataFrame, tuning, streaming, Spark Connect, pandas API.
2. Spark Connect in practice.
3. A local Spark 4.1.1 cluster with 2 workers (the host driver needs `host.docker.internal`).
4. Heavy jobs on purpose to train skew and shuffle tuning.
5. Databricks (Autoloader, Lakeflow) versus the local open-source stack.

### G. Project overview
1. From AdventureWorks in 2023 to a lakehouse in 2026.
2. NexoMart and NexoBank: the marketplace and financial expansion (requirements only).
3. An engineering diary (`daily.md`): documenting what went wrong.

### H. Databricks project (`dev_platform`)
1. Almost destroyed prod with Terraform: the plan showed 5 destroys (`var.catalog` default `advworks_dev` against a `advworks_prd` state). Stopped before apply; fixed with `terraform.workspace` and `locals`.
2. Terraform and DABs: one resource, one owner (schemas and grants vs jobs and wheel).
3. Renaming a job in prod without recreating it: `bundle deployment bind/unbind`.
4. Serverless rejects session configs: `autoMerge` became `.withSchemaEvolution()`; with ANSI on, use `try_cast`.
5. The test passed and the transformer did nothing: a wrong-case column name, silently dropped by `ColumnValidator`.
6. Genie Space managed as code.
7. `MODIFY` does not imply `SELECT` in Unity Catalog.
8. A seed job that ships `PAUSED` to control cost.
9. Liquid clustering on the MERGE key.
10. CI/CD with a service principal (repository secrets vs environment secrets).
