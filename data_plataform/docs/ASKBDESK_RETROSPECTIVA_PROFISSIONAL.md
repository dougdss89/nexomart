# Retrospectiva profissional e técnica — projeto Ask Bdesk (GAB)

**Contexto:** este documento olha para trás, de forma imparcial, sobre o conjunto de
trabalho feito no domínio Ask Bdesk/Sucesso do Cliente (pipeline Bronze→Silver→Gold,
KPIs, 3 Genie Agents em produção, 4 dashboards Lakeview) ao longo de
~25/08 a 18/09/2026. Complementa [`LESSONS_LEARNED_GAB_DATAOPS.md`](LESSONS_LEARNED_GAB_DATAOPS.md),
que fala do que portar da esteira DataOps do GAB para o IcebergLake. Este aqui é sobre
mim: o que os padrões reais desse trabalho dizem sobre pontos fortes e lacunas, técnicas
e de processo.

---

## 1. Pontos fortes confirmados por evidência (não autoavaliação)

### 1.1 Disciplina de causa raiz, não só sintoma
Repetidamente, o problema relatado não era a causa real, e isso foi descoberto por
investigação, não assumido:
- Card 01 ("grupo errado") → pipeline `orquestrador_silver_gold_sucesso_cliente` pausado
  há 5 dias, não um bug de resolução de grupo.
- "Grupo duplicado" no Genie → colisão de `id_grupo_atual` diferentes resolvendo para o
  mesmo nome (`kpi_10_sla_backlog_por_grupo.py`), não um problema de prompt.
- Investigação de carga incremental → a origem já era CDC quase real-time; o atraso era
  do padrão de `.createOrReplace()` completo na Silver, não da fonte.
- Quarentena DQ-G-004 (13.529 linhas) → não era inversão de data de negócio, era um
  artefato de precisão/ordem entre dois timestamps de eventos automatizados quase
  simultâneos.

Em nenhum desses casos a explicação óbvia foi aceita de primeira — todos foram
confirmados com query direta contra o dado real antes de propor correção.

### 1.2 Saber quando NÃO decidir sozinho
Vários itens foram deliberadamente bloqueados por decisão de negócio ausente, em vez de
inventar uma regra plausível: exposição de dado individual por analista (Card 02),
redefinição do KPI oficial de Backlog (Card 20), metodologia de SLA (Gold vs. BI,
resíduo de 204 chamados nunca forçado a bater), critério de "sobrecarga" (nunca definido
em lugar nenhum dos dados). Isso é o oposto do anti-padrão comum de "código decide
regra de negócio por omissão".

### 1.3 Teste ao vivo como critério de fechamento, não a instrução escrita
O padrão consistente — escrever a regra, rodar `genie start-conversation` com uma
pergunta real, conferir o SQL gerado e o número — pegou pelo menos duas falhas que uma
leitura da instrução não pegaria: Card 18 (regra de escopo escrita mas sem efeito
prático, 2 tentativas) e a descoberta de que o fix do V11 não tinha se propagado para
4 outras menções de `descricao_status` na mesma sala.

### 1.4 Documentar a lacuna em vez de simular que ela não existe
Fornecedor, Viva+30, Apontamento de Horas, Analistas Destaque — todos marcados
explicitamente como "sem fonte de dado" nos dashboards em vez de estimados ou omitidos
silenciosamente. Mesmo princípio aplicado ao ISO/Alertas (nota FCR da Nexa excluída por
falta de base, não estimada).

---

## 2. Lacunas técnicas e de processo (a parte que exige ser imparcial)

### 2.1 Mudança direta em produção sem trilha de revisão
Todas as ~80+ mudanças de instrução nos Genie Agents (25 cards + 12 VL + 16 D02 + 18
CARD04 + a regra TS001 mais recente) foram aplicadas via `genie update-space` direto na
sala de produção, no mesmo dia da confirmação verbal/chat de uma única pessoa (geralmente
Ewerton). Não existe PR, não existe diff revisado por segunda pessoa, não existe rollback
versionado além da memória de sessão. Isso é exatamente o padrão que
[`LESSONS_LEARNED_GAB_DATAOPS.md`](LESSONS_LEARNED_GAB_DATAOPS.md) descreve como boa
prática madura (aprovação dupla, allowlist, trilha auditável) — mas o próprio trabalho no
Ask Bdesk não seguiu isso, porque não havia essa esteira para esse domínio. **A lição já
documentada não fechou o loop com a prática.**

### 2.2 Duplicação estrutural nunca resolvida, só descoberta
`genie_governanca/publish_instructions.py` e seu README (16/09/2026) documentam que
Facilitador e Supervisão têm ~15 regras de negócio idênticas escritas à mão, em
duplicidade, em cada sala — e que Liderança e Customer Success KPIs ficaram *anos-luz*
atrás em cobertura da mesma regra. Esse kit foi construído, mas **nunca aplicado em
nenhuma sala** (confirmado no próprio README: "Não foi aplicado em nenhuma sala").
Resultado prático: a regra do Card 06 (disclaimer de alteração de métrica) foi escrita
na Liderança, esquecida na Supervisão/Facilitador por dias, e só replicada quando um
item de validação *diferente* (V02) reabriu o mesmo gap. Isso é reativo, não sistêmico —
o mecanismo pra evitar exatamente esse tipo de esquecimento já foi projetado e ficou
engavetado.

### 2.3 Nenhum harness de regressão
Toda validação é "1 pergunta ao vivo, 1 resposta conferida". Não existe um conjunto de
perguntas-benchmark rodado automaticamente depois de cada mudança de instrução (a própria
skill de Genie Agents recomenda ≥30 itens de benchmark antes de considerar uma sala
estável — nunca construído aqui). Consequência observada na prática: regras que
funcionavam voltaram a quebrar sem ninguém perceber até o próximo lote de cards do GAB
apontar de novo (ex.: escopo Sucesso do Cliente teve 3 rodadas de "fechado" antes de
virar coluna estrutural em vez de instrução).

### 2.4 Instrução de texto como primeiro reflexo, não último recurso
A própria skill usada neste projeto (`databricks-genie-agents`) documenta "texto de
instrução é último recurso" — mas o padrão real do trabalho foi: tentar resolver via
texto primeiro, e só subir para mudança estrutural (nova coluna, novo JOIN) depois de
uma ou duas falhas confirmadas (Card 18, "grupo vs equipe", levou 2 tentativas de
instrução antes de virar `equipe_sucesso_cliente` na tabela). O resultado final foi bom,
mas o caminho até lá custou ciclos evitáveis — vale internalizar a régua "se a regra
nomeia uma tabela/coluna/junção específica, ela provavelmente devia nascer estrutural,
não como texto".

### 2.5 Levantamento de regra de negócio feito tarde, não antes de construir
Backlog oficial, "sobrecarga", metodologia de SLA, mapeamento de prioridade, e agora a
regra TS001 ("operação hoje" exclui concluídos/futuros) — em todos os casos a definição
de negócio só apareceu **depois** que alguém já tinha usado o KPI/agente e reportado
divergência. Nenhum desses casos teve uma etapa formal de "confirmar definição com o GAB
antes de implementar" — o ciclo foi sempre construir → usuário reclama → investigar →
formalizar. Builds mais baratos no curto prazo, mas o custo real apareceu em retrabalho
(SLA foi revisitado em pelo menos 4 rodadas de card diferentes sem nunca fechar de vez).

### 2.6 Dois tipos de consumidor, uma checklist mental só
Pelo menos uma vez (rename das tabelas `_sugestao`→oficial em 15/09), o dashboard
Lakeview quebrou silenciosamente porque a checklist de "quem consome essa tabela" só
cobria os Genie Spaces, não os dashboards. Foi pego e corrigido no mesmo dia, mas por
sorte de revisão, não por um processo que garantisse cobrir os dois tipos de consumidor
sempre que uma tabela Gold muda de nome.

---

## 3. Leitura de habilidade técnica (onde a régua está hoje)

**Forte e demonstrável:**
- Debugging de dados em camada Lakehouse (Bronze/Silver/Gold, DLT/Lakeflow, CDF/CDC,
  cascatas de resolução com fallback).
- Engenharia de prompt/instrução para agentes conversacionais sobre dado estruturado
  (Genie), incluindo os limites reais do que instrução de texto resolve vs. o que exige
  mudança de schema.
- Lakeview/AI-BI: medidas calculadas, multi-dataset filters, armadilhas de nome
  (colisão case-insensitive), tudo documentado com causa raiz, não só "funcionou".
- Comunicação técnica para não-técnicos: PDFs de fechamento por card, artifacts de
  modelo ER gerados ao vivo do `information_schema`, sempre citando achado real (número,
  chamado, screenshot) em vez de generalidade.

**Ainda incipiente, com evidência concreta da lacuna:**
- Engenharia de plataforma/governança *aplicada* (não só documentada) — CI Gates,
  allowlist, aprovação dupla, tudo isso existe como conceito bem entendido (seção 1-2 do
  outro documento) mas zero disso foi implementado no próprio domínio Ask Bdesk.
- Testes automatizados/regressão para sistemas dirigidos por prompt — a disciplina de
  "testar ao vivo" é boa, mas não escala e não é repetível; é o gap mais claro entre "sei
  fazer" e "sei sistematizar".
- Gestão de requisito antes de construir — o padrão builder-first é rápido para prototipar
  mas caro em retrabalho quando o domínio (SLA, backlog, sobrecarga) tem múltiplas
  definições plausíveis e nenhuma dona clara antes do fato.

---

## 4. Ações concretas de melhoria (priorizadas, não uma lista de boas intenções)

1. **Aplicar o próprio kit de consolidação** (`genie_governanca/`) em pelo menos uma sala,
   começando pela mais defasada (Customer Success KPIs) — fechar o loop entre "documentei
   a duplicação" e "resolvi a duplicação". É trabalho que já está pronto, só não foi
   publicado.
2. **Construir um benchmark mínimo (10-15 perguntas) por sala**, versionado em arquivo,
   rodado manualmente antes de fechar qualquer card novo — não precisa ser o eval
   automatizado completo da skill, mas precisa parar de depender só de "testei uma
   pergunta agora".
3. **Antes de escrever uma regra de texto que nomeia tabela/coluna/junção específica**,
   perguntar explicitamente "isso deveria ser uma coluna?" antes de publicar — aplicar a
   régua da seção 2.4 no início do ciclo, não depois de duas falhas.
4. **Um changelog versionado por sala** (mesmo que só um arquivo Markdown por Genie
   Space, com data + regra + quem confirmou) — não substitui CI Gates de verdade, mas dá
   trilha auditável sem esperar a esteira formal (que está bloqueada por 8 itens de
   Tooling & Ops, fora do controle da Divisão).
5. **Checklist de "2 consumidores"** sempre que uma tabela Gold for renomeada/removida:
   Genie Spaces (`data_sources.tables[]`) E dashboards Lakeview (`queryLines`) — os dois,
   sempre, não um dos dois de memória.
