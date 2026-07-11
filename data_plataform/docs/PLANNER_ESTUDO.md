# Planner de Estudo — Agenda Concreta

> **Início**: semana de 07/07/2026  
> **Referência**: `CRONOGRAMA_LEITURA.md` (conceitos) — este arquivo é o calendário executável  
> **Atualizar**: ao terminar cada semana, marque o que foi feito e ajuste o próximo bloco se necessário

---

## Como usar este planner

- **Dias úteis**: 15–20 min. Não é sessão de estudo — é manutenção do ritmo. Ler sem pressão.
- **Sábado**: 2–3h. Sessão principal. Aqui acontece a leitura profunda + síntese.
- **Domingo**: 1h opcional. Laboratório, exercício ou revisão de grifos.
- **Entrega de fase**: ao final de cada bloco, você tem um produto concreto (roteiro do Clube, star schema desenhado, lab AWS concluído).

---

## MÊS 1 — JULHO 2026
**Foco principal**: Kimball Caps 1–4 (Pilar 4 — Leituras)  
**Foco secundário**: AWS Skill Builder — primeiros módulos (Pilar 1)  
**Entrega do mês**: star schema do NexoMart desenhado + rascunho do roteiro do Clube T0

---

### SEMANA 1 — 07 a 13 de Julho
**Kimball — Capítulo 1: Dimensional Modeling Primer (~35p)**  
Objetivo: entender por que o modelo dimensional existe e o que são fatos e dimensões.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 07/07 | Dia útil | Kimball Cap 1 — ler até metade (~17p). Foco: o que é modelagem dimensional e por que não basta o OLTP | 20 min |
| Ter 08/07 | Dia útil | Continuar Cap 1 — fatos, dimensões, granularidade. Grifo: a frase mais importante até agora | 20 min |
| Qua 09/07 | Dia útil | Terminar Cap 1. Anotar: quais tabelas do NexoMart seriam fatos e quais seriam dimensões | 15 min |
| Qui 10/07 | Dia útil | Reler grifos do Cap 1. Escrever a síntese do cap em 3 bullets | 15 min |
| Sex 11/07 | — | Descanso — sem leitura obrigatória | — |
| **Sáb 12/07** | **Sessão principal** | **Revisar Cap 1 (30min) → Iniciar Cap 2 (~20p, primeira metade) → Exercício: listar as 5 dimensões óbvias do NexoMart** | **2h** |
| Dom 13/07 | Opcional | AWS Skill Builder: criar conta + completar módulo "AWS Cloud Practitioner Essentials" (vídeo intro) | 1h |

**Entrega da semana**: 5 dimensões do NexoMart identificadas + síntese do Cap 1 escrita

---

### SEMANA 2 — 14 a 20 de Julho
**Kimball — Capítulo 2: Retail Sales (~35p)**  
Objetivo: aplicar os conceitos em um case de varejo — análogo direto ao NexoMart.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 14/07 | Dia útil | Terminar Cap 2 primeira metade (se não terminou no sábado). Foco: como o Kimball define grain no varejo | 20 min |
| Ter 15/07 | Dia útil | Cap 2 segunda metade — dimensões conformadas, hierarquias, slowly changing preview | 20 min |
| Qua 16/07 | Dia útil | Anotar: qual seria o grain da fact_sales do NexoMart? (1 linha por order_item? por pedido?) | 15 min |
| Qui 17/07 | Dia útil | Escrever declaração de grain do NexoMart em 1 frase. Grifo 3 conceitos do Cap 2 | 15 min |
| Sex 18/07 | — | Descanso | — |
| **Sáb 19/07** | **Sessão principal** | **Revisar Cap 2 (30min) → Iniciar Cap 3 — Inventory (~25p, completo) → Exercício: rascunhar fact_sales com dimensões ao redor** | **2h30** |
| Dom 20/07 | Opcional | AWS Skill Builder: módulo IAM — usuários, grupos, roles, policies (prático no console Free Tier) | 1h |

**Entrega da semana**: grain declaration da fact_sales escrita + rascunho da fact_sales com dimensões

---

### SEMANA 3 — 21 a 27 de Julho
**Kimball — Capítulo 4: Procurement + Síntese Geral (~25p + revisão)**  
Objetivo: fechar Kimball, consolidar o star schema e preparar o Clube T0.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 21/07 | Dia útil | Cap 4 — Procurement (~12p, primeira metade). Relacionar com o schema `purchasing` do NexoMart | 20 min |
| Ter 22/07 | Dia útil | Terminar Cap 4. Anotar: o que o NexoMart tem em common com o case de procurement do Kimball | 20 min |
| Qua 23/07 | Dia útil | Revisão geral: reler todos os grifos dos Caps 1–4. Responder as 3 perguntas de síntese | 20 min |
| Qui 24/07 | Dia útil | Escrever a frase-âncora do Kimball (a que vai abrir o Clube T0) | 15 min |
| Sex 25/07 | — | Descanso | — |
| **Sáb 26/07** | **Sessão principal** | **Síntese final Kimball (30min) → Desenhar Bus Matrix NexoMart (5 fatos × 9 dimensões) → Rascunho completo do roteiro Clube T0** | **3h** |
| Dom 27/07 | Opcional | Implementar no projeto: criar esboço das tabelas gold (fact_sales, dim_customer, dim_product) em SQL ou como comentário no código | 1h |

**Entrega da semana**: Bus Matrix desenhada + roteiro Clube T0 rascunhado

---

### Resumo Julho

| Semana | Leitura | Lab/Prática | Entrega |
|--------|---------|-------------|---------|
| 1 (07–13/07) | Kimball Cap 1 | AWS: criar conta + intro | 5 dimensões NexoMart identificadas |
| 2 (14–20/07) | Kimball Cap 2 + início Cap 3 | AWS: IAM no console | Grain declaration + rascunho fact_sales |
| 3 (21–27/07) | Kimball Cap 3 + Cap 4 + síntese | Projeto: esboço tabelas gold | Bus Matrix + roteiro Clube T0 |

---

## MÊS 2 — AGOSTO 2026
**Foco principal**: Fundamentos de Engenharia de Dados — Reis & Housley (Pilar 4)  
**Foco secundário**: AWS Fase A — S3 + VPC fundamentos (Pilar 1)  
**Entrega do mês**: vocabulário do ciclo de vida consolidado + roteiro Clube T1 + labs S3 e VPC

---

### SEMANA 4 — 28 Jul a 03 Ago
**Fundamentos DE — Caps 1 + 2 (~60p)**  
Objetivo: entender o ciclo de vida completo e os "undercurrents" que afetam tudo.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 28/07 | Dia útil | Cap 1 — ciclo de vida do engenheiro de dados. O NexoMart em que fase do ciclo está? | 20 min |
| Ter 29/07 | Dia útil | Continuar Cap 1. Anotar: o que é "geração de dados" no NexoMart | 20 min |
| Qua 30/07 | Dia útil | Cap 2 — Undercurrents (segurança, qualidade, observabilidade). Grifo: os que já estão no projeto | 20 min |
| Qui 31/07 | Dia útil | Terminar Cap 2. Anotar: quais undercurrents o NexoMart ainda não tem implementados | 15 min |
| Sex 01/08 | — | Descanso | — |
| **Sáb 02/08** | **Sessão principal** | **Revisar Caps 1–2 (30min) → Iniciar Cap 3 (~20p) → Exercício: mapear o ciclo de vida completo do NexoMart (landing → gold)** | **2h** |
| Dom 03/08 | Opcional | AWS: lab S3 — criar bucket, configurar versionamento, lifecycle rule, server-side encryption | 1h |

---

### SEMANA 5 — 04 a 10 de Agosto
**Fundamentos DE — Caps 3 + 8 (~90p)**  
Objetivo: arquitetura de dados e transformações — conectar com a stack do NexoMart.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 04/08 | Dia útil | Terminar Cap 3 (se não terminou). Foco: o que define uma boa arquitetura de dados | 20 min |
| Ter 05/08 | Dia útil | Cap 8 — Queries e Transformações. Como dbt se encaixa aqui | 20 min |
| Qua 06/08 | Dia útil | Continuar Cap 8. Relacionar com os Spark jobs Silver → Gold do projeto | 20 min |
| Qui 07/08 | Dia útil | Síntese do Cap 8. Como o Medallion Architecture aparece no livro | 15 min |
| Sex 08/08 | — | Descanso | — |
| **Sáb 09/08** | **Sessão principal** | **Revisar Caps 3 + 8 (30min) → Cap 9 — Ingestão completo (~40p) → Relacionar com FastAPI + Airflow do NexoMart** | **2h30** |
| Dom 10/08 | Opcional | AWS: lab VPC — criar VPC com subnets públicas/privadas, NAT Gateway, Security Groups | 1h |

---

### SEMANA 6 — 11 a 17 de Agosto
**Fundamentos DE — Cap 9 (conclusão) + síntese + Clube T1**

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 11/08 | Dia útil | Terminar Cap 9. Comparar os padrões de ingestão do livro com o que você implementou | 20 min |
| Ter 12/08 | Dia útil | Revisão geral Fundamentos DE: reler todos os grifos | 20 min |
| Qua 13/08 | Dia útil | Responder as 3 perguntas de síntese do livro completo | 15 min |
| Qui 14/08 | Dia útil | Escrever a frase-âncora do Fundamentos DE (abre o Clube T1) | 15 min |
| Sex 15/08 | — | Descanso | — |
| **Sáb 16/08** | **Sessão principal** | **Síntese final Fundamentos DE (30min) → Roteiro completo Clube T1 → Atualizar o ciclo de vida do NexoMart com o vocabulário do livro** | **2h** |
| Dom 17/08 | Opcional | Databricks: criar conta Community Edition + completar "Databricks Fundamentals Accreditation" (gratuito, ~2h) | 1h30 |

### Resumo Agosto

| Semana | Leitura | Lab/Prática | Entrega |
|--------|---------|-------------|---------|
| 4 (28/07–03/08) | Fund. DE Caps 1–2 | AWS: S3 lab | Ciclo de vida NexoMart mapeado |
| 5 (04–10/08) | Fund. DE Caps 3 + 8 | AWS: VPC lab | Ligação Medallion ↔ projeto |
| 6 (11–17/08) | Fund. DE Cap 9 + síntese | Databricks: Fundamentals Accreditation | Roteiro Clube T1 + certificado Fundamentals |

---

## MÊS 3 — SETEMBRO 2026
**Foco principal**: DDIA — Kleppmann Caps 3–4 + 11 (Pilar 4)  
**Foco secundário**: AWS Skill Builder — Glue + EMR + Athena introdução (Pilar 1)  
**Entrega do mês**: roteiro Clube T2 + compreensão de por que Iceberg existe

---

### SEMANA 7 — 18 a 24 de Agosto (transição)
**DDIA — Capítulo 3: Storage and Retrieval (~35p)**  
Objetivo: entender B-Tree vs LSM-Tree e por que Parquet + Iceberg são a escolha certa.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 18/08 | Dia útil | DDIA Cap 3 — primeira parte: B-Tree (índices do PostgreSQL) vs LSM-Tree (Parquet/Iceberg) | 20 min |
| Ter 19/08 | Dia útil | Continuar Cap 3 — column-oriented storage (por que Parquet é mais rápido para analytics) | 20 min |
| Qua 20/08 | Dia útil | Terminar Cap 3. Anotar: por que Iceberg é LSM-Tree em essência | 20 min |
| Qui 21/08 | Dia útil | Síntese Cap 3 + 3 bullets de conexão com o projeto | 15 min |
| Sex 22/08 | — | Descanso | — |
| **Sáb 23/08** | **Sessão principal** | **Revisar Cap 3 (30min) → Cap 4 — Encoding and Evolution completo (~25p) → Relacionar com schema evolution no Iceberg** | **2h** |
| Dom 24/08 | Opcional | AWS: lab Glue — criar Data Catalog, crawler sobre S3, schema evolution | 1h |

---

### SEMANA 8 — 25 a 31 de Agosto
**DDIA — Capítulo 11: Stream Processing (~45p)**  
Objetivo: entender por que Kafka é o centro da arquitetura orientada a eventos.

| Dia | Sessão | O que fazer | Tempo |
|-----|--------|-------------|-------|
| Seg 25/08 | Dia útil | DDIA Cap 11 — primeira parte: o que é processamento de streams vs batch | 20 min |
| Ter 26/08 | Dia útil | Cap 11 — message brokers, log-based (Kafka) vs traditional MQ | 20 min |
| Qua 27/08 | Dia útil | Cap 11 — stream joins, exactly-once semantics | 20 min |
| Qui 28/08 | Dia útil | Terminar Cap 11. Anotar: por que o Outbox Pattern resolve o "dual write problem" | 15 min |
| Sex 29/08 | — | Descanso | — |
| **Sáb 30/08** | **Sessão principal** | **Síntese completa DDIA (30min) → Responder as 3 perguntas de síntese → Rascunho roteiro Clube T2** | **2h** |
| Dom 31/08 | Opcional | AWS: lab Kinesis vs MSK — subir um producer/consumer simples no Free Tier | 1h |

---

### Semanas 9–10 — Setembro (02 a 14/09)
**Buffer + AWS Fase A finalização + início Databricks prático**

| Semana | Foco | O que fazer |
|--------|------|-------------|
| 9 (02–07/09) | AWS Fase A | Completar labs CloudWatch + CloudTrail + KMS. Revisar IAM, S3, VPC na prática |
| 10 (08–14/09) | Databricks | Notebook Databricks Community: reproduzir os jobs Silver → Gold do projeto em Databricks. Comparar com o código local |

### Resumo Setembro

| Período | Leitura | Lab/Prática | Entrega |
|---------|---------|-------------|---------|
| 18–24/08 | DDIA Cap 3 + 4 | AWS: Glue lab | Conexão Parquet/Iceberg ↔ teoria storage |
| 25–31/08 | DDIA Cap 11 | AWS: Kinesis lab | Roteiro Clube T2 |
| 01–14/09 | — buffer — | AWS: fechar Fase A / Databricks: jobs no Community | AWS Fase A concluída |

---

## MÊS 4 — OUTUBRO 2026
**Foco principal**: DDD Evans (Pilar 4) + início AWS Fase B  
**Foco secundário**: Databricks — Spark Core avançado  
**Entrega do mês**: bounded contexts do NexoMart documentados

---

### Semanas 11–13 — Outubro (05 a 25/10)
**DDD Evans — Partes I + II + IV (~220p)**

| Semana | Foco | Capítulos | Entrega |
|--------|------|-----------|---------|
| 11 (05–11/10) | Evans Parte I | Caps 1–3: Ubiquitous Language | Glossário de domínio do NexoMart |
| 12 (12–18/10) | Evans Parte II | Caps 5–6: Entities, Value Objects, Aggregates | Identificar aggregate roots dos 7 schemas |
| 13 (19–25/10) | Evans Parte II + IV | Cap 7: Repositories + Caps 14–15: Bounded Contexts | Context Map do NexoMart desenhado |

**Paralelo AWS semanas 11–13**: AWS Skill Builder — Glue Studio + EMR fundamentos  
**Paralelo Databricks**: Spark avançado — Catalyst optimizer, EXPLAIN, AQE

---

## MÊS 5 — NOVEMBRO 2026
**Foco principal**: Learning DDD — Khononov Caps 1–6 + 10 (Pilar 4)  
**Foco secundário**: AWS Fase B — Athena + Lake Formation + Redshift  
**Entrega do mês**: roteiro Clube T3 + implementação do Outbox Pattern no projeto

---

### Semanas 14–16 — Novembro (02 a 22/11)

| Semana | Foco | Capítulos | Entrega |
|--------|------|-----------|---------|
| 14 (02–08/11) | Khononov Caps 1–2 | Business Domains + Domain Knowledge | Refinar Context Map com vocabulário atualizado |
| 15 (09–15/11) | Khononov Caps 3–5 | Bounded Contexts + Aggregates | Rascunho de implementação do Aggregate em Python |
| 16 (16–22/11) | Khononov Cap 6 + 10 | Cap 6 (eventos) + Cap 10 (Outbox, Saga, CQRS) | Roteiro Clube T3 + esboço Outbox no projeto |

**Paralelo AWS**: Athena v3 lab (Iceberg nativo), Lake Formation fine-grained access  
**Paralelo Databricks**: Delta Live Tables introdução

---

## MÊS 6 — DEZEMBRO 2026
**Foco principal**: Hard Parts + GoF (Pilar 4)  
**Foco secundário**: AWS Fase B conclusão — DMS + MWAA + MSK  
**Entrega do mês**: roteiro Clube T4 + T5

---

### Semanas 17–20 — Dezembro

| Semana | Foco | O que estudar | Entrega |
|--------|------|--------------|---------|
| 17 (30/11–06/12) | Hard Parts Caps 1–2 | As partes difíceis + encontrando limites | Identificar os trade-offs do NexoMart |
| 18 (07–13/12) | Hard Parts Caps 3–4 + 6 + 9 | Decomposição + CQRS + data access | Roteiro Clube T4 |
| 19 (14–20/12) | GoF — Intro + padrões | Intro + Repository + Factory + Observer | Nomear os padrões no código do projeto |
| 20 (21–27/12) | GoF — síntese | Revisão + síntese + roteiro | Roteiro Clube T5 |

**Dezembro paralelo AWS**: completar DEA-C01 study plan (Maarek). Agendar exame para Jan/Fev 2027  
**Dezembro paralelo Databricks**: Databricks Spark Developer study plan (Spark: The Definitive Guide + exercícios)

---

## 2027 — Consolidação e Certificações

### Jan–Mar 2027
| Foco | O que fazer | Meta |
|------|-------------|------|
| Leituras | Fluent Python (Caps 13 + 17 + 21) + Clube T6 | Roteiro Clube T6 |
| AWS | Agendar e fazer DEA-C01 (exame) | **Certificação DEA-C01** |
| Databricks | Agendar e fazer Spark Developer (exame) | **Certificação Spark Developer** |
| Projeto | Async generators nos seeders + refatoração com protocolos | Código mais Pythônico |

### Apr–Jun 2027
| Foco | O que fazer | Meta |
|------|-------------|------|
| AWS | AIF-C01 study plan (Maarek + AWS Skill Builder AI path) | Estudo AIF-C01 |
| Databricks | Data Engineer Associate study plan (Databricks Academy) | Estudo Associate |
| Projeto | Reproduzir pipeline NexoMart completo no Community Edition | Pipeline Databricks funcionando |
| AWS Lab | Bedrock Workshop + RAG sobre catálogo NexoMart | Projeto AIF-C01 concreto |

### Jul–Set 2027
| Foco | O que fazer | Meta |
|------|-------------|------|
| AWS | Agendar e fazer AIF-C01 (exame) | **Certificação AIF-C01** |
| Databricks | Agendar e fazer Data Engineer Associate (exame) | **Certificação Data Engineer Associate** |
| Projeto | SageMaker: treinar modelo de recomendação sobre fact_sales | Pipeline ML rodando |

### Out–Dez 2027
| Foco | O que fazer | Meta |
|------|-------------|------|
| AWS | MLA-C01 study plan (AWS Skill Builder ML Engineer path) | Estudo MLA-C01 |
| Databricks | Data Engineer Professional study plan (labs avançados) | Estudo Professional |
| Projeto AWS | NexoMart na AWS — Fase C (Terraform completo) | Deploy AWS real |
| Projeto Databricks | DLT + Workflows + DAB — pipeline NexoMart em Professional | Pipeline prod-ready |

---

## 2028 — Nível Profissional Completo

### Jan–Mar 2028
| Foco | O que fazer | Meta |
|------|-------------|------|
| AWS | Agendar e fazer MLA-C01 (exame) | **Certificação MLA-C01** |
| Databricks | Agendar e fazer Data Engineer Professional (exame) | **Certificação Data Engineer Professional** |
| Projeto | SageMaker endpoint consumido pela FastAPI do NexoMart | Plataforma AI-ready completa |

---

## Mapa de Certificações

```
AWS
────────────────────────────────────────────────────────────
CLF-C02  (opcional)
    ↓
SAA-C03  ──── Fundação arquitetural (Q1 2027)
    ↓
DEA-C01  ──── Dados em AWS: Glue, EMR, Athena, MSK (Q1 2027)
    ↓
AIF-C01  ──── IA Practitioner: Bedrock, SageMaker overview (Q3 2027)
    ↓
MLA-C01  ──── ML Engineer: SageMaker profundo, MLOps (Q1 2028)

DATABRICKS
────────────────────────────────────────────────────────────
Fundamentals Accreditation  (gratuito — faz logo)
    ↓
Spark Developer Associate  ──── Spark Core, joins, UI (Q1 2027)
    ↓
Data Engineer Associate    ──── Delta, DLT, Unity Catalog (Q3 2027)
    ↓
Data Engineer Professional ──── Prod-grade, MLflow, DAB (Q1 2028)
```

---

## Marcador de Progresso

Copie e atualize esta seção toda semana:

```
KIMBALL
[ ] Cap 1 — lido
[ ] Cap 2 — lido
[ ] Cap 3 — lido
[ ] Cap 4 — lido
[ ] Síntese escrita
[ ] Bus Matrix NexoMart desenhada
[ ] Roteiro Clube T0 rascunhado

FUNDAMENTOS DE
[ ] Caps 1–2 — lidos
[ ] Cap 3 — lido
[ ] Cap 8 — lido
[ ] Cap 9 — lido
[ ] Síntese escrita
[ ] Roteiro Clube T1 rascunhado

DDIA
[ ] Cap 3 — lido
[ ] Cap 4 — lido
[ ] Cap 11 — lido
[ ] Síntese escrita
[ ] Roteiro Clube T2 rascunhado

DDD EVANS
[ ] Parte I (Caps 1–3) — lida
[ ] Parte II (Caps 5–7) — lida
[ ] Parte IV (Caps 14–15) — lida
[ ] Context Map NexoMart desenhado

KHONONOV
[ ] Caps 1–2 — lidos
[ ] Caps 3–5 — lidos
[ ] Cap 6 + 10 — lidos
[ ] Roteiro Clube T3 rascunhado

HARD PARTS
[ ] Caps 1–4 — lidos
[ ] Caps 6 + 9 — lidos
[ ] Roteiro Clube T4 rascunhado

GOF
[ ] Intro — lida
[ ] Repository + Factory + Observer — lidos
[ ] Roteiro Clube T5 rascunhado

FLUENT PYTHON
[ ] Cap 13 — lido
[ ] Cap 17 — lido
[ ] Cap 21 — lido
[ ] Roteiro Clube T6 rascunhado

AWS — FUNDAMENTOS
[ ] Conta criada + Free Tier ativo
[ ] IAM — lab concluído (users, roles, policies, STS)
[ ] S3 — lab concluído (lifecycle, versionamento, encryption)
[ ] VPC — lab concluído (subnets, SGs, NAT, VPC endpoints)
[ ] CloudWatch + CloudTrail — lab concluído
[ ] KMS — lab concluído

AWS — DADOS (DEA-C01)
[ ] Glue — lab concluído (crawler, catalog, ETL job)
[ ] EMR — lab concluído (Spark job no cluster)
[ ] Athena — lab concluído (Iceberg nativo, workgroup)
[ ] Lake Formation — lab concluído (fine-grained access)
[ ] Kinesis / MSK — lab concluído (producer + consumer)
[ ] DMS — lab concluído (CDC do PostgreSQL)
[ ] MWAA — lab concluído (DAG no Airflow gerenciado)
[ ] Cantrill SAA-C03 — curso completo
[ ] Maarek DEA-C01 — curso completo
[ ] SAA-C03 — aprovado ✓
[ ] DEA-C01 — aprovado ✓

AWS — IA (AIF-C01 → MLA-C01)
[ ] Bedrock Workshop — lab concluído (RAG sobre catálogo)
[ ] SageMaker Immersion Day — lab concluído
[ ] SageMaker Feature Store — lab concluído
[ ] SageMaker Pipelines — lab concluído
[ ] RAG NexoMart com Bedrock Knowledge Bases — implementado
[ ] Modelo de recomendação no SageMaker — treinado e deployado
[ ] Maarek AIF-C01 — curso completo
[ ] AWS Skill Builder ML Engineer path — completo
[ ] AIF-C01 — aprovado ✓
[ ] MLA-C01 — aprovado ✓

AWS — PROJETO
[ ] NexoMart na AWS — Terraform completo (Fase C)

DATABRICKS
[ ] Community Edition — conta criada
[ ] Fundamentals Accreditation — obtido ✓ (gratuito)
[ ] Pipeline NexoMart reproduzido no Community Edition
[ ] Catalyst + EXPLAIN EXTENDED — estudado
[ ] Broadcast join vs Sort-Merge join — dominado
[ ] AQE (Adaptive Query Execution) — estudado
[ ] Spark UI — consegue ler DAG e identificar gargalos
[ ] Structured Streaming avançado — estudado
[ ] Delta Live Tables — pipeline implementado
[ ] Unity Catalog — fine-grained access configurado
[ ] Databricks Workflows — orquestração implementada
[ ] Liquid Clustering — aplicado no projeto
[ ] Databricks Asset Bundles — pipeline deployado via DAB
[ ] MLflow — experimento registrado + modelo no registry
[ ] "Spark: The Definitive Guide" — caps críticos lidos
[ ] "Delta Lake: The Definitive Guide" — caps críticos lidos
[ ] Spark Developer Associate — aprovado ✓
[ ] Data Engineer Associate — aprovado ✓
[ ] Data Engineer Professional — aprovado ✓
```

---

*Criado em 2026-07-04 | Atualizar semanalmente*  
*Referência: `CRONOGRAMA_LEITURA.md`*
