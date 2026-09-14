# Lições da esteira DataOps do GAB — o que trazer pro IcebergLake

**Contexto:** enquanto investigava como automatizar envio de relatórios via Databricks
Genie para o projeto Ask Bdesk (cliente/consultoria), acabei lendo a documentação
completa de governança de dados de uma organização (GAB) que já opera uma esteira
DataOps madura em produção — e aplicando parte dela num protótipo real. Este
documento não é sobre o Ask Bdesk; é sobre o que dessa esteira vale a pena portar
pro **IcebergLake**, que é meu projeto.

---

## 1. O que a esteira GAB faz bem (resumo técnico)

Arquitetura observada: YAML como fonte de verdade → PR com CI Gates bloqueantes →
merge → codegen automático (YAML→Terraform) → PR automática com o `.tf` gerado →
aprovação dupla → merge em `main` → Terraform Apply automático em produção.
Princípio-guia deles, que vale citar porque resume tudo: **"Se passou no PR, não
quebra em `main` nem em PRD."**

O que sustenta isso na prática, em ordem de relevância pro meu projeto:

### 1.1 Guardrails por camada, com rigor crescente Bronze→Silver→Gold
Validação automática (bloqueante) que impede `OVERWRITE` não governado, `TRUNCATE`,
`DROP TABLE/DATABASE`, `DELETE` sem `WHERE`, `dbutils.fs.rm` recursivo — **a menos
que exista uma allowlist versionada, com justificativa, aprovador e data de
expiração**. Gold tem regras mais rígidas que Silver (ex.: `DELETE` sem `WHERE` é
aviso em Silver, erro em Gold; `impact_assessment` é obrigatório só em Gold).

**Por que importa:** hoje meu projeto não tem nenhuma proteção estática contra um
`df.write.mode("overwrite")` acidental num script de `gold_layer_scripts/`. A
"proteção" é só disciplina manual.

### 1.2 Allowlist com expiração obrigatória — nunca liberação permanente
Toda exceção a um guardrail vira uma entrada YAML versionada
(`justification`, `approved_by`, `approved_at`, `expires_at`, `ticket`). Item
expirado volta a bloquear automaticamente, sem ação manual. Isso evita o padrão
clássico de "exceção temporária" que nunca é revertida.

### 1.3 Disciplina de portabilidade DEV→PRD
Regra simples e poderosa: **nenhuma string de ambiente hardcoded no código**
(catálogo, cluster id, caminho `/Repos/DEV/`). Encontrei e corrigi esse exato bug
no meu próprio protótipo durante a investigação — um notebook com
`cat_holding_silver_dev` fixo no código, que quebraria em produção. O padrão certo
é: ambiente sempre vem de parâmetro/variável, nunca de string literal.

### 1.4 Codegen — o YAML é a única coisa que um humano edita
"O desenvolvedor nunca escreve Terraform manualmente." Um script converte o YAML
declarativo (jobs, pipelines) em `.tf`, roda em modo `--dry-run` no PR (valida sem
aplicar), e só gera o `.tf` de verdade depois do merge. Isso elimina a categoria
inteira de bug "Terraform escrito à mão divergiu do YAML declarado".

### 1.5 Aprovação dupla com papéis diferentes, não duplicados
PR pra produção exige **duas** aprovações com escopos diferentes: a Divisão
(regra de negócio correta? validado funcionalmente?) e a Plataforma (padrões
técnicos, segurança/secrets, custo, consistência com outros repos). Não é "duas
pessoas aprovam a mesma coisa" — é "duas perspectivas diferentes, cada uma
irredutível à outra".

### 1.6 Diff-aware CI
As validações de PR rodam só sobre os arquivos que mudaram no diff — um PR não
falha por causa de um problema pré-existente em código que ele nem tocou. Reduz
fricção sem abrir mão de bloquear o que realmente importa naquele PR.

### 1.7 Nomenclatura como mecanismo, não estética
A norma de nomenclatura de repositório (`<org3>-<dominio>-<produto>-<proposito>`)
não é só padronização visual — é uma regra estruturalmente parseável (cada campo
é um único token, sem hífen interno) que existe pra automação conseguir separar os
campos sem ambiguidade. Eu mesmo violei isso na hora (usei `sucesso-cliente` como
"produto", que tem hífen embutido) e só percebi ao reler a norma com atenção.
**Lição:** convenção de nome só vale se for possível validar/parsear
automaticamente — senão vira sugestão que ninguém confere.

---

## 2. O que dá pra portar pro IcebergLake, concretamente

| Padrão GAB | Onde aplicar no IcebergLake | Esforço |
|---|---|---|
| Guardrail contra escrita destrutiva em Gold | Check estático (pre-commit ou `pr-checks.yml`) escaneando `spark/jobs/gold_layer_scripts/**.py` por `overwrite`, `TRUNCATE`, `DROP TABLE`, `.mode("overwrite")` sem confirmação explícita | Médio — reaproveita a infra de lint que já existe (`.pre-commit-config.yaml`, `scripts/lint.sh`) |
| Allowlist com expiração | Um `allowlist.yml` simples por camada (`bronze`/`silver`/`gold`), checado pelo mesmo script acima | Baixo, depois do guardrail existir |
| Auditoria de portabilidade (nada de ambiente hardcoded) | Grep/AST check procurando strings tipo `s3a://bronze-dev/`, `iceberglake_dev`, hosts fixos, em vez de vars/`.env` | Baixo — acho que vale rodar isso já, antes de crescer mais o projeto |
| YAML declarativo + codegen | Se algum dia o projeto ganhar Terraform (hoje é `docker-compose`/Makefile), vale já nascer com "YAML descreve, script gera .tf" em vez de Terraform escrito à mão desde o início |
| Diff-aware em `pr-checks.yml` | Conferir se os checks atuais (`ci.yml`, `pr-checks.yml`) já ignoram arquivos não tocados no PR — se não, é ganho de fricção fácil |
| Detector de anti-patterns Spark (`.collect()`, `print()` sem log) | Regra de lint customizada (Ruff/AST) pros scripts em `spark/jobs/**` — mesmo padrão usado no protótipo que fiz (achei os dois anti-patterns no meu próprio código antes de entregar) |
| Parametrização "fan-out" (1 job genérico + lista dinâmica) | Mapeia direto pro **dynamic task mapping** do Airflow — o padrão "não hardcode a lista, leia de uma tabela/config e gere uma task por item" é o mesmo racional do `for_each_task` do Databricks Jobs |
| Isolamento de secrets por ambiente com approval | Conferir se `cd.yml` já usa GitHub Environments com *required reviewers* pra secrets de "prod" (MinIO/Postgres de produção, se existir esse conceito no projeto) |

---

## 3. Lições de processo (como eu trabalhei, não só o que aprendi)

Estas vieram de observar o que funcionou (e o que quase deu errado) durante a
própria investigação, não só da leitura da documentação:

1. **Verificar contra o sistema real antes de escrever código.** Rodei a
   pergunta de verdade no Genie, consultei o schema real das tabelas, chequei
   quais catálogos existiam de fato — em vez de assumir a partir da doc. Isso
   pegou dois bugs reais: um `ImportError` de uma classe do SDK que só existia na
   minha versão local (não na do cluster serverless), e o catálogo hardcoded que
   quebraria em PRD. **Nenhum dos dois apareceria só lendo documentação.**
2. **Quando doc e realidade divergem, sinalizar — não "corrigir" silenciosamente.**
   A norma de catálogo (`cat_<divisao>_<camada>`) não bate com os catálogos reais
   do workspace (`_dev`/`_prd` no nome). Não faz sentido eu "consertar" isso
   sozinho — é uma divergência que precisa ir pro dono do processo (Tooling &
   Ops), com o fato registrado, não escondido.
3. **Não fabricar o que não é seu de fabricar.** Quando descobri que não existia
   repositório/esteira pro domínio, a tentação seria criar `.github/workflows/`
   inventados "pra parecer completo". Não fiz — documentei como placeholder
   explícito, porque simular uma esteira funcional que não existe é pior do que
   admitir a lacuna.
4. **Testar em modo seguro antes de qualquer efeito colateral real.** O job de
   e-mail nasceu com `metodo_envio=none` (dry-run, só grava arquivo) como padrão,
   e só ganha capacidade de enviar de verdade quando alguém troca o parâmetro
   explicitamente — depois que a autorização humana existir. **Nada perigoso por
   default.**
5. **Convenção só serve se for verificável.** Errei a nomenclatura do repositório
   na primeira tentativa porque não apliquei a regra letra por letra — só
   "pareceu razoável". A correção veio de reler a norma com atenção aos exemplos
   concretos, não à descrição em prosa.

---

## 4. Pra levar pro currículo/portfólio

O que descrevi acima — guardrails por camada, allowlist com expiração, disciplina
de portabilidade, aprovação dupla com papéis distintos — é vocabulário e prática
de **DataOps/Platform Engineering em produção real**, não teoria de curso. Vale
explicitamente:

- Documentar no README do IcebergLake uma seção "Governança" que **implemente**
  (não só cite) pelo menos o guardrail de escrita destrutiva em Gold + a
  allowlist com expiração — é um diferencial concreto e demonstrável em
  entrevista, muito mais forte do que "sei Spark e Airflow".
- Se possível, escrever um post/nota curta sobre o padrão "YAML como fonte de
  verdade + codegen pra IaC" — é um conceito que aparece em Databricks Asset
  Bundles, mas o racional (nunca editar o artefato gerado à mão) é genérico e
  vale pra qualquer stack com Terraform.
- O hábito de "verificar contra o sistema real antes de escrever" (item 3.1)
  é o tipo de prática que separa engenharia sênior de júnior — vale nomear
  isso explicitamente como algo que pratico, não só fazer sem registrar.
