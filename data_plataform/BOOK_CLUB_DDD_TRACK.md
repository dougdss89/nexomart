# Theory Track / Clube do Livro — DDD (Khononov + Evans)

> Nota: este arquivo é novo (a versão anterior mencionada em sessões passadas não foi
> encontrada em disco). Contém por ora só o plano de conteúdo do Clube T3 (DDD),
> derivado e alinhado a `docs/CRONOGRAMA_LEITURA.md` (fonte de verdade dos capítulos e
> páginas do plano de estudo pessoal). O restante da Estratégia Consolidada (personas,
> Fast Track, Deep Track, NexoMart) pode ser reconstruído aqui depois, se fizer sentido.

## Por que combinar os dois livros

- **Evans (Domain-Driven Design, 2003 — o Livro Azul)** — fonte original, mais densa e
  filosófica. É a "base" teórica do Clube T3 no cronograma pessoal.
- **Khononov (Learning Domain-Driven Design, 2021)** — mapa moderno, prático, cobre
  padrões que não existiam formalizados em 2003 (Outbox, Saga, CQRS). É o próprio
  "Clube T3" no cronograma.

Decisão: em vez de um único episódio Clube T3 cobrindo os dois livros de uma vez,
**expandir em 4 sub-episódios (T3.1–T3.4)**, pareando Evans + Khononov por tema. Mantém
a numeração `T` já estabelecida no canal (T0 Kimball, T1 Fundamentos DE, T2 DDIA...) em
vez de criar um prefixo novo.

## Clube T3.1 — "Linguagem e Domínio"

- **Evans** — Parte I (caps 1–3): Ubiquitous Language + modelo para trabalhar (~60p)
- **Khononov** — Caps 1–2: Analyzing Business Domains + Discovering Domain Knowledge (~40p)
- **Tema central**: como nomear os domínios do NexoMart certo — `product`, `sales`,
  `warehouse`, `purchasing`, `marketing`, `person`, `humanresources` — antes de escrever
  qualquer código. Ubiquitous language como pré-requisito, não formalidade.

## Clube T3.2 — "Bounded Contexts"

- **Khononov** — Caps 3–4: Managing Domain Complexity + Integrating Bounded Contexts (~40p)
- **Evans** — Parte IV (caps 14–15): Strategic Design — Context Map, Bounded Contexts (~70p)
- **Tema central**: separar os domínios reais do projeto em bounded contexts de verdade —
  onde as fronteiras do NexoMart hoje são só schemas do Postgres, e onde deveriam ser
  fronteiras de serviço.
- **Gancho**: prepara terreno para qualquer ADR futuro sobre decomposição de serviços.

## Clube T3.3 — "Blocos Táticos: Aggregates"

- **Evans** — Parte II (caps 5–7): Entities, Value Objects, Aggregates, Repositories (~90p)
  — a origem desses padrões
- **Khononov** — Cap 5: Implementing Aggregates (~40p) — a versão moderna/prática
- **Tema central**: candidato real a Aggregate no dbapi — `PurchaseOrderHeader` + detail
  como estudo de caso.
- **Conecta com ADR-007** (arquitetura 3-layer Repository/Service/Router) já documentada
  no cronograma pessoal.

## Clube T3.4 — "Padrões de Integração"

- **Khononov** — Cap 10: Design Patterns — Outbox, Saga, CQRS (~60p)
- **Evans**: sem capítulo correspondente — os padrões de integração assíncrona não
  existiam formalizados no livro de 2003. Episódio praticamente 100% Khononov.
- **Tema central**: como isso conecta com Kafka como buffer entre API e storage.
- **Conecta com ADR-005** (Outbox Pattern para exactly-once com Kafka) já documentada
  no cronograma pessoal.

## Próximos passos em aberto

- Confirmar se os 4 sub-episódios saem todos na mesma semana (1 semana antes do Fast
  Track de domínio/decomposição, como já é a regra pros outros Clubes) ou espaçados.
- Roteirizar T3.1 primeiro — é o que depende só do Khononov + Evans Parte I, ambos já
  disponíveis.
- Reconstruir (ou não) o restante da Estratégia Consolidada neste arquivo.
