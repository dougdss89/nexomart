# Theory Track / Clube do Livro — DDD (Khononov + Evans)

> Nota: este arquivo é novo (a versão anterior mencionada em sessões passadas não foi
> encontrada em disco). Contém por ora só o plano de estudo/conteúdo de DDD combinando
> os dois livros. O restante da Estratégia Consolidada (personas, Fast Track, Deep Track,
> ADRs, NexoMart) pode ser reconstruído aqui depois, se fizer sentido.

## Por que combinar os dois livros

- **Khononov (Learning Domain-Driven Design, 2021)** — mapa moderno, didático, vocabulário
  atualizado, exemplos claros. Bom como "trilho principal".
- **Evans (Domain-Driven Design, 2003 — o Livro Azul)** — fonte original, mais denso e
  filosófico. Traz profundidade que o Khononov resume/atualiza mas não substitui,
  principalmente na Parte III (Refactoring Toward Deeper Insight), que não tem
  equivalente direto no livro do Khononov.

## Estrutura proposta: 4 episódios "Clube TD" (Theory Track — DDD)

Nomenclatura `TD` para não colidir com a numeração `T1–T4` já usada pro Clube do Livro
geral (Kimball etc.) — decidir se mantém prefixo próprio ou entra na sequência principal.

### Clube TD1 — "Domínio e Contexto" (Strategic Design)
- **Khononov**: Cap. 1–4 (subdomínios, ubiquitous language, bounded context, context mapping)
- **Evans**: Parte IV — Cap. 11–14 (Strategic Design: bounded context, continuous
  integration, context map, distillation)
- **Tema central**: como dividir o NexoMart em bounded contexts de verdade — usar os
  domínios já existentes no projeto (`product`, `sales`, `warehouse`, `purchasing`,
  `marketing`, `person`, `humanresources`) como estudo de caso ao vivo.
- **Gancho pro Fast Track**: prepara o terreno para qualquer ADR futuro sobre separação
  de serviços/domínios.

### Clube TD2 — "Os Blocos de Construção" (Tactical Design)
- **Khononov**: Cap. 5–6 (transaction script / active record vs. domain model rico,
  aggregates, value objects, domain events)
- **Evans**: Parte II — Cap. 4–6 (Layered Architecture, Entities, Value Objects,
  Services, Modules, Aggregates, Factories, Repositories) — a **origem** desses padrões
- **Tema central**: onde no dbapi já existe (ou falta) um Aggregate de verdade — ex.:
  `PurchaseOrderHeader` + detail como candidato natural.

### Clube TD3 — "Refatorando Rumo ao Insight" (a parte mais subestimada)
- **Evans**: Parte III — Cap. 7–10 (Breakthrough, Supple Design, Intention-Revealing
  Interfaces, Side-Effect-Free Functions, Assertions, Conceptual Contours, Standalone
  Classes, Closure of Operations, Declarative Design, Specification) — episódio
  praticamente standalone, sem correspondente direto no Khononov
- **Khononov**: Cap. 10–11 (heurísticas de design, evolução de decisões) como
  complemento leve
- **Tema central**: o capítulo que quase ninguém lê do Evans, mas é o mais atemporal —
  bom ângulo de conteúdo ("o que os outros resumos de DDD não te contam").

### Clube TD4 — "DDD na Era dos Dados" (Event-Driven Architecture / Data Mesh)
- **Khononov**: Cap. 12–13 (Event-Driven Architecture, Data Mesh) — sem equivalente no
  Evans, conteúdo genuinamente novo
- **Tema central**: ponte direta com o que o Fast Track já está construindo (Kafka,
  CQRS) — Data Mesh conecta DDD com a própria tese do canal (arquitetura de dados
  orientada a domínio).

## Cronograma de leitura (pessoal, antes de virar roteiro)

1. **Semana 1** (só Khononov disponível): ler Parte I inteira (Cap. 1–4) — mapa geral.
2. **Semana 2+** (Evans chega): por capítulo do Khononov, ler o trecho correspondente
   do Evans na sequência (ver tabela de pareamento acima).
3. Fechar com Evans Parte III isolada — não tem pressa, é o material mais denso.
4. Aplicar cada bloco de leitura nos domínios reais do `data_plataform` antes de
   transformar em roteiro de Clube TD.

## Próximos passos em aberto

- Decidir prefixo dos episódios (`TD1–TD4` vs. entrar na sequência `T`).
- Definir se os Clube TD entram antes de algum Fast Track específico (padrão: 1 semana
  antes, como já é a regra pros outros Clubes).
- Reconstruir (ou não) o restante da Estratégia Consolidada neste arquivo.
