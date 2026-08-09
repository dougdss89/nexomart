# Clube do Livro — T1: "O Cliente é a Dimensão Mais Difícil"
## Lendo o Kimball: Caps 8 e 16 | Theory Track | 8–10 min

> **Quando vai ao ar**: 1 semana antes do episódio de Modelagem Dimensional do Ciclo 2
> **Livro**: The Data Warehouse Toolkit, 3ª ed. — Ralph Kimball & Margy Ross
> **Capítulos**: 8 (CRM Analytics) + 16 (Insurance)
> **Pré-requisito de audiência**: ter assistido ao Clube T0 (Caps 1–3) ou conhecer os conceitos de Star Schema, fato e dimensão

---

## Objetivo do Episódio

Mostrar que a dimensão `customer`/`person` é a mais estratégica **e** a mais problemática de qualquer DW — e que o Kimball resolve isso com três decisões de design que vão aparecer diretamente no NexoMart.

---

## Estrutura de Roteiro

```
00:00 - 00:35  Hook — o problema que todo mundo já viveu
00:35 - 01:15  Contexto: onde estamos no livro
01:15 - 03:30  Bloco 1 — SCD-2 na dimensão cliente + a armadilha da contagem
03:30 - 05:30  Bloco 2 — Bridge Table: quando uma linha não é suficiente
05:30 - 07:30  Bloco 3 — Cadeia de datas + mini-dimensão (Cap 16)
07:30 - 09:00  Conexão direta com o NexoMart
09:00 - 10:00  Pergunta para o Discord + CTA
```

---

## Roteiro Completo (o que falar)

### 00:00 — HOOK

> *"Você abre o relatório de clientes ativos. O número aparece: 1,2 milhão. Você vai para o banco de dados e conta com um SELECT COUNT(*): 2,4 milhões. A reunião com o negócio vira um caos — qual dos dois está certo? Os dois. E os dois estão errados ao mesmo tempo. Esse é o problema que o Kimball resolve no capítulo 8."*

**[Mostrar na tela]**: título do episódio + capa do Kimball Toolkit

---

### 00:35 — CONTEXTO: Onde Estamos no Livro

> *"No Clube T0 a gente leu os primeiros capítulos — grain, fato, dimensão, Star Schema, o básico que funda tudo. Hoje a gente salta para os capítulos 8 e 16, que tratam de dois setores diferentes, CRM e seguros, mas ensinam as mesmas três técnicas. Eu escolhi esses dois capítulos juntos porque eles formam o kit de sobrevivência de quem vai modelar qualquer dimensão cliente no mundo real."*

---

### 01:15 — BLOCO 1: SCD-2 na Dimensão Cliente + A Armadilha da Contagem

> *"O Kimball abre o capítulo 8 com uma afirmação direta: a dimensão cliente é crítica e precisa ser conformizada. Conformizada significa que ela tem a mesma estrutura em todos os data marts da empresa — mesmo data type, mesma coluna, mesma granularidade. Se o marketing usa uma versão e as vendas usam outra, você não tem um DW, você tem dois silos digitais.*
>
> *E aí começa o problema real. Quando você usa SCD Type 2 — que mantém o histórico de mudanças do cliente — cada mudança de endereço, cada mudança de nome, cada mudança de status vira uma linha nova na tabela. Um cliente que mudou de endereço três vezes tem quatro linhas. Agora: qual é o total de clientes?*
>
> *O Kimball dá três formas de responder isso sem mentir:*"

**[Mostrar na tela: slide ou quadro branco com os três métodos]**

> *"Um: COUNT DISTINCT na NK — a chave natural, o ID do sistema de origem. Não na surrogate key do DW, na natural key que não muda nunca.*
>
> *Dois: filtrar pela flag `is_current` ou `is_active` — só contar as linhas que representam o estado atual.*
>
> *Três: filtrar pelo intervalo de datas — `WHERE valid_to IS NULL` ou `valid_to > hoje`. Qualquer uma das três funciona. O erro é fazer um COUNT(*) sem pensar.*
>
> *Outro detalhe que o Kimball menciona: se o dado de cliente vier de fonte externa, com taxa de atualização diferente dos seus sistemas internos, crie uma dimensão separada e relacione com a dimensão mãe. Não tente enforçar tudo na mesma tabela. O ganho operacional compensa o modelo ligeiramente mais complexo."*

---

### 03:30 — BLOCO 2: Bridge Table — Quando Uma Linha Não É Suficiente

> *"A segunda técnica é a Bridge Table, e ela aparece em dois momentos diferentes nos capítulos 8 e 16.*
>
> *O cenário: você tem uma dimensão que na teoria tem um valor por cliente — segmento, categoria de crédito, tipo de apólice. Mas na prática, com o tempo, aparecem clientes que pertencem a múltiplos grupos ao mesmo tempo. O modelo relacional clássico não aguenta isso sem repetição de dados.*
>
> *A solução do Kimball é criar uma tabela ponte entre a tabela fato e a dimensão. Essa tabela ponte não é uma dimensão comum — ela é uma resolução de relacionamento N:M. A fato aponta para a bridge, a bridge aponta para a dimensão.*
>
> *No capítulo 16, ele mostra uma variação ainda mais específica: uma dimensão que agrega outras dimensões — tipo 'setor alimentício' que agrupa restaurante, bar, supermercado. A mesma lógica: bridge como ponto de entrada, dimensão base com o detalhe.*
>
> *A regra prática que eu tirei daqui: se você está colocando um array ou uma lista numa coluna do banco relacional, é um sinal de que você precisa de uma bridge table no modelo dimensional."*

**[Mostrar na tela: diagrama simples — Fato → Bridge → Dim]**

---

### 05:30 — BLOCO 3: Cadeia de Datas e Mini-Dimensão (Cap 16)

> *"O capítulo 16 fala de seguradoras, mas tem dois insights que se aplicam a qualquer modelo de vendas.*
>
> *O primeiro: em vez de uma única data por pedido, o Kimball sugere modelar uma cadeia de datas. Data de criação do pedido. Data do envio. Data da entrega. Data do fechamento do processo. Cada uma dessas datas é uma chave para a dimensão de data. Com elas, você consegue responder perguntas como: qual o tempo médio entre o pedido e a entrega? Em quais dias da semana os pedidos atrasam mais? Qual o gap entre envio e fechamento?*
>
> *Segundo insight: a mini-dimensão. Quando um atributo da dimensão cliente muda muito rápido — frequência de compra, classificação de risco, nível de engajamento — se você tratar isso com SCD-2 normal, a tabela vai explodir. Um cliente que muda de nível de engajamento toda semana vira 52 linhas por ano só de histórico de nível.*
>
> *A solução é criar uma dimensão separada, menor, só para esses atributos voláteis — a mini-dimensão — e relacioná-la com a tabela fato diretamente, não com a dimensão cliente principal. Você isola a volatilidade sem contaminar a dimensão central."*

**[Mostrar na tela: diagrama — Fato com 4 FK de data + FK de mini-dim]**

---

### 07:30 — CONEXÃO COM O NEXOMART

> *"Agora o que isso tem a ver com o NexoMart? Tudo.*
>
> *Primeiro: a tabela `person.customer` no nosso OLTP já tem `is_active` e `row_guid` — o AuditMixin. Quando a gente transformar isso em dimensão no DW, a contagem correta de clientes vai usar exatamente o `row_guid` como natural key e o `is_active` como flag de estado atual. Exatamente o que o Kimball prescreveu no capítulo 8.*
>
> *Segundo: a tabela `sales.sales_order` tem `order_date`, mas ela poderia ter — e vai ter no modelo dimensional — quatro datas: a data do pedido, a data de pagamento, a data de envio e a data de entrega. Cada uma como FK para a `dim_date`. A cadeia de datas do capítulo 16 entra no nosso modelo real.*
>
> *Terceiro: no domínio `marketing`, as campanhas vão ter multi-canal — um anúncio pode estar em Google, Facebook e TikTok ao mesmo tempo. Isso é um atributo multivalorado. A bridge table do capítulo 8 é exatamente o que vai resolver esse caso no modelo dimensional do NexoMart.*
>
> *Três técnicas do livro, três casos de uso diretos no projeto que a gente tá construindo juntos."*

---

### 09:00 — PERGUNTA PARA O DISCORD + CTA

> *"Antes de fechar, a pergunta da semana para o servidor do Discord — canal #clube-do-livro:*
>
> *Você já modelou ou viu um caso onde a dimensão cliente estava sem conformidade entre áreas? O marketing com uma versão, as vendas com outra? O que aconteceu? Qual foi o custo disso?*
>
> *No próximo episódio do canal, a gente vai pegar tudo isso e construir ao vivo. Dimensão cliente, SCD-2, bridge table, cadeia de datas — no código, no banco, com dados reais do NexoMart. Se você ainda não assistiu o Clube T0, link na descrição. E se você quiser participar da discussão, o link do Discord também tá lá.*
>
> *Até o próximo."*

---

## Artefatos Visuais Necessários

| Cena | O que mostrar | Como produzir |
|---|---|---|
| Hook | Dois números conflitantes (1,2M vs 2,4M) | Slide simples — branco, texto grande |
| Bloco 1 | Os 3 métodos de COUNT correto | Slide de lista com destaque visual |
| Bloco 1 | Código SQL dos 3 métodos | Terminal ou VSCode com syntax highlight |
| Bloco 2 | Diagrama Fato → Bridge → Dim | dbdiagram.io ou diagrama desenhado à mão |
| Bloco 3 | Fato com 4 FKs de data + FK de mini-dim | Mesmo diagrama, estendido |
| Conexão NexoMart | Schema `person.customer` no código | VSCode ou DBeaver com o modelo real |

---

## Código SQL de Apoio (mostrar na tela)

```sql
-- Os 3 métodos de contagem correta com SCD-2

-- Método 1: COUNT DISTINCT na natural key
SELECT COUNT(DISTINCT customer_nk) AS total_clientes
FROM dim_customer;

-- Método 2: Flag is_current
SELECT COUNT(*) AS total_clientes
FROM dim_customer
WHERE is_current = TRUE;

-- Método 3: Intervalo de datas
SELECT COUNT(*) AS total_clientes
FROM dim_customer
WHERE valid_to IS NULL;
```

```sql
-- Cadeia de datas numa fato de pedidos (Cap 16)
CREATE TABLE fact_sales_order (
    order_sk         BIGINT,
    order_date_sk    INT REFERENCES dim_date(date_sk),   -- data do pedido
    payment_date_sk  INT REFERENCES dim_date(date_sk),   -- data do pagamento
    ship_date_sk     INT REFERENCES dim_date(date_sk),   -- data do envio
    delivery_date_sk INT REFERENCES dim_date(date_sk),   -- data da entrega
    customer_sk      BIGINT,
    product_sk       BIGINT,
    revenue          NUMERIC(19,4),
    quantity         INT
);
```

---

## Checklist de Produção

- [ ] Gravar introdução com câmera + gancho oral
- [ ] Preparar slides dos 3 métodos de COUNT (Canva ou similar)
- [ ] Desenhar diagrama Bridge Table (pode ser à mão + câmera por cima)
- [ ] Desenhar diagrama cadeia de datas (mesmo processo)
- [ ] Abrir VSCode com `person/customer_model.py` para a seção NexoMart
- [ ] Preparar os dois blocos de SQL no terminal
- [ ] Gravar CTA com link do Discord na tela
- [ ] Thumbnail: "O Kimball e a Armadilha da Dimensão Cliente" (número conflitante em destaque)

---

## Links de Distribuição (preencher após publicação)

| Plataforma | Conteúdo derivado | Status |
|---|---|---|
| YouTube | Episódio completo | — |
| LinkedIn Post | "3 erros de contagem de clientes em DW — e como o Kimball corrige cada um" | — |
| LinkedIn Newsletter | Edição: "A dimensão que todo DW subestima" | — |
| Medium | Artigo longo: "SCD-2, Bridge Tables e cadeia de datas — o trio do cap 8 e 16 do Kimball" | — |
| Instagram | Carrossel: diagrama Bridge Table explicado em 5 slides | — |
| Discord #clube-do-livro | Pergunta da semana postada | — |

---

*Documento criado em 2026-05-06 | Clube do Livro T1 — Caps 8 + 16 do Kimball Toolkit*
