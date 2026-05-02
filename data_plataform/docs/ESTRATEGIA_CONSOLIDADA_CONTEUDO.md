# Estratégia Consolidada de Conteúdo — Data Engineering Creator

> YouTube + LinkedIn + Medium + Instagram: como criar, distribuir e crescer documentando seu Data Platform.

---

## As Personas Consultadas

**ALEX** — Especialista em Storytelling  
*Acredita que toda arquitetura técnica esconde uma história humana. Foco em narrativa, conflito, resolução e emoção.*

**MARINA** — Especialista em Comunicação  
*Obcecada com clareza, linguagem acessível e a distância entre o que o expert sabe e o que a audiência entende.*

**RAFAEL** — Especialista em Apresentação  
*Domina ritmo visual, slides, demos ao vivo, câmera e edição. Sabe que o formato mata ou salva o conteúdo.*

---

## O Princípio Central: Um Conteúdo, Cinco Formatos

Você não vai criar conteúdo separado para cada plataforma. Você vai criar **uma vez** e **distribuir em camadas**. O YouTube é o motor. Todo o resto é derivado.

```
YOUTUBE (Pillar — três trilhas)
        │
        ├── Fast Track (série principal — EP01 a EP09)
        ├── Deep Track (fundamentos — EP02.1, 02.2...)
        ├── Theory Track / Clube do Livro ──→ DISCORD (comunidade)
        │
        ├── MEDIUM (Texto longo — SEO orgânico)
        ├── LINKEDIN NEWSLETTER (Ângulo profissional — audiência existente)
        ├── LINKEDIN POSTS (Insights individuais + ADRs arquiteturais)
        └── INSTAGRAM (Visual — diagrama / bastidores / carrossel)
```

Cada plataforma tem uma função específica no ecossistema. Misturar as funções quebra a estratégia.

---

## BLOCO I — YOUTUBE: O Motor

### PARTE 1 — Posicionamento: Para Quem Você Está Falando?

**MARINA:** Antes de gravar o primeiro frame, precisamos resolver uma questão fundamental: quem é a sua audiência? Esse projeto — FastAPI, Airflow, Spark, Iceberg, Trino, MinIO, Nessie — é denso. Se você falar para iniciantes, vai precisar de 20 vídeos só de fundamentos. Se falar para sêniores, pode ir direto ao ponto. A decisão muda tudo.

**ALEX:** Eu diria que a audiência ideal aqui são **engenheiros de dados plenos e desenvolvedores backend curiosos** que já passaram pela frustração de montar uma stack de dados e descobriram que é um campo minado. Eles conhecem Docker, já viram Airflow, mas nunca montaram tudo isso junto de forma coesa. Essa é a dor que você resolve — e dor é o começo de toda boa história.

**RAFAEL:** Do ponto de vista de apresentação, isso define o *tom* de cada vídeo. Para esse público, você não precisa explicar o que é um container Docker, mas precisa mostrar *por que* a sequência de serviços que você escolheu faz sentido e não outra. A câmera pode ficar em segundo plano — o terminal e os dashboards são os protagonistas visuais.

**MARINA:** Minha recomendação: escreva uma frase de posicionamento antes de gravar qualquer coisa. Algo como: *"Este canal é para engenheiros de dados que querem construir uma plataforma de dados moderna, open-source e production-like do zero, sem depender de cloud proprietária."* Isso vira sua bússola editorial.

**ALEX:** E note que essa frase já tem um antagonista implícito — a cloud proprietária, o lock-in, o custo. Antagonista é narrativa. Você não está só ensinando tecnologia; está defendendo uma filosofia.

---

### PARTE 2 — A Estrutura Narrativa: O Arco dos Vídeos

**ALEX:** Todo vídeo técnico de sucesso segue, conscientemente ou não, a estrutura clássica de três atos. No contexto do seu projeto:

- **Ato 1 (Problema):** "Você já tentou montar uma plataforma de dados e ficou perdido entre 10 ferramentas que não conversam entre si?"
- **Ato 2 (Jornada):** Aqui você mostra o caminho — as decisões de arquitetura, os erros, as configurações, o código.
- **Ato 3 (Resolução):** O dado fluindo do FastAPI até o Trino, a query funcionando, o dashboard aparecendo. A satisfação visual do sistema operando.

**MARINA:** Isso é essencial. A maioria dos vídeos técnicos começa direto no Ato 2 — instalação, configuração, código. Sem o Ato 1, a audiência não sabe *por que* deveria se importar. Você perde o espectador nos primeiros 30 segundos.

**RAFAEL:** No YouTube, os primeiros **15 a 30 segundos** determinam se a pessoa fica ou sai. Eu recomendo começar com o *resultado final rodando*: Trino consultando uma tabela Iceberg, um pipeline Airflow completando, os dados aparecendo. Mostre o destino antes de começar a viagem. Depois volta para "mas como chegamos aqui?"

**ALEX:** É o que chamamos de *in medias res* — entrar no meio da ação. A abertura mais poderosa para o primeiro vídeo seria algo como: "Esse terminal aqui está consultando 50 milhões de registros em menos de 3 segundos. Esse pipeline aqui gerou esses dados há 10 minutos. E isso tudo roda na sua máquina, com Docker, sem gastar um centavo em cloud." — pausa — "Bora construir?"

**MARINA:** Perfeito porque estabelece credibilidade, promessa e convite em três frases.

---

### PARTE 3 — Comunicando Complexidade Técnica Sem Perder Ninguém

**MARINA:** Esse é o maior desafio do seu projeto especificamente. Você tem sete serviços que precisam funcionar em conjunto. Se tentar explicar todos de uma vez, ninguém entende nada. O princípio que uso é o da **camada de cebola**: comece com o que o espectador já conhece e adicione uma camada por vídeo.

**ALEX:** Analogias são seus melhores aliados aqui. Algumas que funcionariam:
- **MinIO** = "É o S3 da Amazon, mas roda na sua máquina"
- **Nessie** = "É o Git para os seus dados — você pode criar branches de dados, fazer commit, rollback"
- **Iceberg** = "É a camada que torna sua tabela de dados inteligente: ela sabe o que mudou, quando mudou e quem mudou"
- **Trino** = "É o Google dos seus dados — ele não armazena nada, só sabe onde tudo está e devolve o resultado rápido"
- **Airflow** = "É a agenda da sua plataforma — decide o que roda, quando roda e na que ordem"

**RAFAEL:** Cada uma dessas analogias pode abrir um vídeo inteiro. E visualmente, diagramas animados simples — mesmo feitos no Excalidraw ou no próprio Python como você já fez no `diagram.py` — valem mais que mil palavras. Mostre o dado *se movendo* entre os serviços com setas.

**MARINA:** Outro recurso poderoso: **repita os conceitos em contextos diferentes**. Na segunda vez que mencionar Iceberg, não explique de novo — apenas diga "lembra que o Iceberg é o Git dos dados? Aqui ele vai criar um novo snapshot automaticamente." Reforça sem ser repetitivo.

**ALEX:** E sempre que introduzir uma nova ferramenta, aplique o framework **Problema → Ferramenta → Prova**. "O problema é que queries em arquivos Parquet não suportam deletes eficientes. O Iceberg resolve isso. Olha aqui o DELETE rodando e o dado desaparecendo da query seguinte."

---

### PARTE 4 — Formato, Ritmo Visual e Apresentação

**RAFAEL:** Vamos falar de produção. Para um projeto como esse, recomendo fortemente o formato **"Talking Head + Screen"**: você aparece em um canto pequeno da tela (câmera de notebook serve para começar) enquanto 80% do frame é seu terminal, seu browser, seu código. Isso humaniza sem tirar o foco do que importa.

**ALEX:** Humanizar é crucial. As pessoas não seguem canais — seguem pessoas. Mesmo que seja um projeto técnico denso, momentos de "eu travei 3 horas nisso aqui e depois descobri que era uma variável de ambiente errada" constroem conexão genuína. Vulnerabilidade técnica é relatável.

**MARINA:** Para comunicação clara no formato vídeo, algumas regras práticas:
1. **Uma ideia por cena** — se você está explicando o Airflow DAG, não mude para o código do FastAPI na mesma cena sem uma transição clara
2. **Sinalize as transições verbalmente** — "agora que o serviço está configurado, vamos ver como o Spark se conecta a ele"
3. **Recapitule no início** — 15 segundos de "no vídeo anterior fizemos X, hoje vamos fazer Y porque Z"
4. **Preview no início, recap no fim** — espelhe o que você prometeu com o que entregou

**RAFAEL:** Sobre duração: vídeos técnicos profundos performam bem entre **15 e 25 minutos** no YouTube. Abaixo disso você não consegue cobrir o que precisa com profundidade. Acima de 30 minutos, a retenção cai significativamente a menos que você seja já um nome estabelecido. Para o primeiro vídeo, mire em **20 minutos**.

**ALEX:** E divida o vídeo em **capítulos** (timestamps no description e no próprio vídeo). Capítulos aumentam a retenção porque a pessoa sente que pode "navegar" no conteúdo sem ficar perdida.

**RAFAEL:** Estrutura de produção que recomendo para cada vídeo:

```
00:00 - 00:30  Hook (resultado final rodando / promessa)
00:30 - 02:00  Contexto e o que vamos construir hoje
02:00 - 05:00  Diagrama / arquitetura do episódio
05:00 - 18:00  Hands-on (código, terminal, configuração)
18:00 - 20:00  Teste ao vivo + resultado
20:00 - 21:00  Recap + próximo vídeo (CTA)
```

**RAFAEL:** Sobre equipamento — sem desculpas para não começar:
- **Áudio** é mais importante que vídeo. Um microfone USB simples (R$150-300) faz diferença enorme
- **Iluminação** — uma janela na frente já resolve 80% do problema
- **Resolução** — grave em 1080p. 4K é dispensável no começo
- **Edição** — DaVinci Resolve (gratuito) serve bem. Corte os silêncios e os "éééé". O ritmo do áudio define o ritmo do vídeo

---

### PARTE 5 — Planejamento da Série: A Playlist como Produto

**ALEX:** Aqui está o insight mais importante: **a playlist é o produto, não o vídeo**. Cada vídeo individual é um capítulo. O espectador que assiste ao vídeo 1 deve sentir que *precisa* do vídeo 2. Isso é storytelling serial — o mesmo mecanismo que faz pessoas maratanarem séries.

**MARINA:** E isso tem implicações práticas de comunicação. Cada vídeo deve terminar com um cliffhanger técnico: "mas tem um problema — quando o volume de dados cresce, essa abordagem trava. No próximo vídeo vamos resolver isso com particionamento." Você plantou uma dúvida, ela não vai embora.

**RAFAEL:** Minha sugestão de estrutura para a série completa do projeto:

#### Série Sugerida: "Data Platform do Zero"

| Episódio | Título | Serviço Principal |
|----------|--------|------------------|
| EP01 | O Marketplace — O Caso de Negócio, o Banco de Dados e o Problema Analítico | business_db + Visão Geral + Docker |
| EP02 | Ingestão com FastAPI Async — PostgreSQL e 3-Layer Architecture | dbapi + PostgreSQL + SQLAlchemy |
| EP03 | Armazenamento Distribuído — MinIO, Delta Lake e Unity Catalog OSS | MinIO + Delta Lake + Unity Catalog |
| EP04 | Processamento em Escala com Spark — da Ingestão ao Bronze | Spark + Autoloader + Airflow |
| EP05 | Kafka entra na Stack — Event-Driven e Outbox Pattern | Kafka + Schema Registry + Outbox |
| EP06 | CQRS na Prática — Read Path via Trino sobre Delta Lake | Trino + CQRS |
| EP07 | Qualidade de Dados — Chaos Engineering, dbt e Great Expectations | dbt + Great Expectations + OpenLineage |
| EP08 | Observabilidade — OpenTelemetry, Prometheus e Data Lineage | OpenTelemetry + Prometheus + Marquez |
| EP09 | Pipeline Completo — do Evento Kafka ao Dashboard Trino | Integração total |

**ALEX:** Repara que essa estrutura tem um arco perfeito: o EP01 é a promessa, os EPs 02-09 são a jornada (cada serviço é um "boss") e o EP10 é a resolução — tudo funcionando junto. É a estrutura do herói aplicada à engenharia de dados.

**MARINA:** E do ponto de vista de SEO e distribuição, títulos com "do zero" e "na prática" performam muito bem em conteúdo técnico em português. Não subestime thumbnails — elas determinam o CTR (taxa de clique). Use uma fórmula consistente: logo da tecnologia principal + uma frase de impacto curta.

---

### PARTE 7 — As Duas Trilhas: Fast Track e Deep Track

**MARINA:** Quero voltar a algo que disse antes — a camada de cebola. O problema real é que a audiência que você quer atingir não é homogênea. Tem o engenheiro pleno que já conhece FastAPI e quer saber como integrá-lo ao Iceberg. Mas tem o dev backend que está migrando para dados e não sabe o que é um seeder, não entende async de verdade, nunca configurou um pool de conexões. Se você fizer um vídeo só, vai ser raso demais para um e denso demais para o outro. Você vai perder os dois.

**ALEX:** Esse é um problema clássico de audiência dividida. E a solução narrativa para isso é criar duas jornadas paralelas com o mesmo destino. É o que séries de TV fazem com subplots — narrativas que correm em paralelo, se cruzam nos pontos certos e chegam juntas no clímax. Aqui o "clímax" é o EP10 — o pipeline completo rodando. Os dois públicos chegam lá por caminhos diferentes.

**MARINA:** Exatamente. E eu chamaria assim: **Fast Track** e **Deep Track**. O Fast Track é a série principal que já planejamos — EP01 a EP10. Assume que a pessoa tem uma base técnica razoável e quer construir. O Deep Track são episódios complementares, numerados como sub-episódios — EP02.1, EP02.2, EP02.3 — que existem para quem precisa dos fundamentos antes de entrar no episódio principal. São episódios menores, focados, cirúrgicos.

**RAFAEL:** Do ponto de vista de produção, isso é elegante porque muda completamente o formato. O Fast Track tem 15-25 minutos, terminal aberto, código na tela, ritmo alto. O Deep Track pode ser 8-12 minutos, mais didático, com mais diagrama e menos código, câmera um pouco mais presente porque você está *explicando*, não *construindo*. São produtos diferentes em aparência, mas derivados da mesma preparação.

**ALEX:** E tem um ganho narrativo enorme: você transforma a barreira de entrada em conteúdo. Em vez de suavizar o EP02 para não assustar os iniciantes — o que sempre dilui o conteúdo para os avançados — você diz: "se você não conhece FastAPI ainda, eu gravei o EP02.1 especialmente para você. Assiste aquele primeiro e volta aqui." É um convite, não um muro. E a pessoa que assiste o EP02.1 já chega no EP02 preparada e agradecida.

**MARINA:** A comunicação entre as trilhas precisa ser explícita em cada vídeo. No início do Fast Track, mencione o Deep Track disponível. No final do Deep Track, mostre onde o espectador entra na Fast Track. Isso cria uma malha — as pessoas circulam entre as playlists e o tempo total assistido no canal explode.

**RAFAEL:** Duas playlists distintas no canal, com nomes claros:
- **"Data Platform do Zero — Fast Track"** (EP01 a EP10)
- **"Fundamentos de Data Engineering"** (todos os sub-episódios organizados por tema)

A segunda playlist tem valor standalone. Alguém que nunca vai construir a plataforma completa pode assistir só os fundamentos e já aprender muito. Isso amplia o topo do funil sem comprometer a profundidade da série principal.

**ALEX:** E o SEO dessa segunda playlist é ouro puro. Enquanto o Fast Track compete com outros tutoriais de stack completa — que são poucos mas densos — o Deep Track compete em keywords de fundamentos: "o que é FastAPI", "como funciona async python", "o que é um ORM", "seeder de banco de dados". Essas keywords têm volume altíssimo e a concorrência em português é fraca.

**MARINA:** Resumindo a lógica: o Fast Track é o produto principal. O Deep Track é o caminho de entrada — e também um produto autônomo para quem ainda não está pronto para o Fast Track. Os dois se alimentam. E o espectador nunca fica preso: sempre tem um próximo passo claro independente do nível em que ele chegou.

---

#### Estrutura Completa das Três Trilhas

| Episódio | Trilha | Título | Duração |
|----------|--------|--------|---------|
| EP01 | Fast Track | O Marketplace — O Caso de Negócio, o Banco de Dados e o Problema Analítico | 22 min |
| Clube T0 | Theory Track | Lendo Kimball Caps 1–3: O que é modelagem dimensional e por que o OLTP não basta | 10 min |
| EP01.1 | Deep Track | O que é uma Plataforma de Dados? (e por que montar uma) | 10 min |
| EP01.2 | Deep Track | Docker para Engenheiros de Dados — o mínimo que você precisa saber | 10 min |
| EP01.3 | Deep Track | OLTP vs OLAP: por que não conseguimos analisar direto do banco transacional | 10 min |
| EP01.4 | Deep Track | Fato, Dimensão e Grain: os 3 conceitos do Kimball no contexto do Marketplace | 12 min |
| EP01.5 | Deep Track | Star Schema na prática: desenhando o modelo dimensional do NexoMart | 12 min |
| Clube T1 | Theory Track | Lendo: Fundamentos de Engenharia de Dados — ciclo de vida e Medallion | 10 min |
| EP02 | Fast Track | Ingestão com FastAPI Async — PostgreSQL e 3-Layer Architecture | 20 min |
| EP02.1 | Deep Track | O que é FastAPI e por que async muda tudo em APIs de dados | 10 min |
| EP02.2 | Deep Track | PostgreSQL para Engenheiros de Dados — além do CRUD | 10 min |
| EP02.3 | Deep Track | O que é um Seeder e por que todo pipeline de dados precisa de um | 8 min |
| EP02.4 | Deep Track | SQLAlchemy 2.0 — ORM, Async e a arquitetura de repositórios | 12 min |
| EP02.5 | Deep Track | Explodindo JSONB: como transformar order_items em linhas analíticas no Spark | 10 min |
| Clube T2 | Theory Track | DDIA — Storage Engines: por que Delta Lake e Parquet são dramaticamente mais rápidos | 10 min |
| EP03 | Fast Track | Armazenamento Distribuído — MinIO, Delta Lake e Unity Catalog OSS | 20 min |
| EP03.1 | Deep Track | Delta Lake vs Iceberg — trade-offs reais e por que escolhemos Delta | 12 min |
| EP03.2 | Deep Track | Unity Catalog OSS — governança de dados Open Source alinhada ao Databricks | 10 min |
| EP03.3 | Deep Track | SCD Type 2 na prática: clientes e produtos que mudam com o tempo | 12 min |
| EP04 | Fast Track | Processamento em Escala com Spark — da Ingestão ao Bronze | 22 min |
| EP04.1 | Deep Track | Driver e Executor: a divisão de trabalho no Spark | 10 min |
| EP04.2 | Deep Track | Autoloader: ingestão incremental, checkpoint e schema evolution | 12 min |
| Clube T3 | Theory Track | DDIA Cap 11 — por que o log do Kafka é o event store imutável | 10 min |
| EP05 | Fast Track | Kafka entra na Stack — Event-Driven e Outbox Pattern | 22 min |
| EP05.1 | Deep Track | Structured Streaming e Kafka no Spark — offset management e checkpoints | 12 min |
| Clube T4 | Theory Track | As Partes Difíceis — decomposição e CQRS em sistemas distribuídos | 10 min |
| EP06 | Fast Track | CQRS na Prática — Read Path via Trino sobre Delta Lake | 20 min |
| EP06.1 | Deep Track | O que é SQL Federado? Como o Trino consulta sem mover dados | 10 min |
| EP07 | Fast Track | Qualidade de Dados — Chaos Engineering, dbt e Great Expectations | 20 min |
| EP07.1 | Deep Track | dbt na prática — transformações Silver → Gold sobre Delta Lake | 12 min |
| EP07.2 | Deep Track | O que é Chaos Engineering em dados? Qualidade, fraudes e anomalias | 12 min |
| EP08 | Fast Track | Observabilidade — OpenTelemetry, Prometheus e Data Lineage | 20 min |
| EP08.1 | Deep Track | OpenTelemetry em pipelines de dados — traces, métricas e logs | 10 min |
| EP09 | Fast Track | Pipeline Completo — do Evento Kafka ao Dashboard Trino | 25 min |

---

#### Exemplo Detalhado: EP02 em Fast Track + Deep Track

**EP02 — Fast Track (20 min): "Ingestão de Dados com FastAPI Async"**

> Assume: você já sabe o que é uma API REST e já usou Docker antes.  
> Entrega: endpoint `/sales/orders` recebendo dados, validando com Pydantic, persistindo via repositório assíncrono no PostgreSQL.

```
00:00 - 00:30  Hook: "Esse endpoint aqui vai receber 50 mil requisições por hora sem travar."
00:30 - 02:00  O papel do dbapi na stack: onde ele se encaixa entre o mundo externo e o Iceberg
02:00 - 04:00  Diagrama da arquitetura 3 camadas: Router → Service → Repository
04:00 - 08:00  Criando o modelo SQLAlchemy com AuditMixin
08:00 - 12:00  Schema Pydantic + validações de negócio
12:00 - 17:00  Repository com bulk insert assíncrono (sem for-loop)
17:00 - 19:00  Testando com 10.000 registros — tempo de resposta ao vivo
19:00 - 20:00  Cliffhanger: "Esses dados estão no Postgres. No EP03 vamos modelar as relações."
```

---

**EP02.1 — Deep Track (10 min): "O que é FastAPI e por que async muda tudo em APIs de dados"**

> Para quem: devs que conhecem Flask/Django mas nunca usaram FastAPI ou nunca entenderam async de verdade.  
> Entrega: clareza conceitual — você sai sabendo *por que* async importa em ingestão de dados de alta volumetria.

```
00:00 - 01:00  Problema: "Por que APIs síncronas travam quando o volume de dados cresce?"
01:00 - 04:00  Analogia: garçom síncrono vs garçom assíncrono (o garçom não espera o prato ficar pronto)
04:00 - 07:00  FastAPI vs Flask: o que muda na prática (benchmark simples ao vivo)
07:00 - 09:00  Como async/await funciona em Python — o mínimo que você precisa saber
09:00 - 10:00  "Agora que você entende isso, o EP02 vai fazer muito mais sentido — link na descrição."
```

---

**EP02.2 — Deep Track (10 min): "PostgreSQL para Engenheiros de Dados — além do CRUD"**

> Para quem: pessoas que usaram Postgres como banco de uma aplicação web mas nunca pensaram nele como destino de pipelines.  
> Entrega: entender schemas, particionamento básico, o papel do asyncpg e por que não usamos SQLite aqui.

```
00:00 - 01:00  "Postgres não é só banco de aplicação — aqui ele é nossa camada transacional"
01:00 - 04:00  Schemas como namespaces: sales, warehouse, person, product (demonstração visual)
04:00 - 07:00  asyncpg vs psycopg2: por que o driver importa em alta concorrência
07:00 - 09:30  Bulk insert vs insert individual — demonstração de performance ao vivo
09:30 - 10:00  "Próximo: o que é SQLAlchemy 2.0 e por que abstraímos o SQL — EP02.4"
```

---

**EP02.3 — Deep Track (8 min): "O que é um Seeder e por que todo pipeline precisa de um"**

> Para quem: quem vem do mundo de aplicações web e nunca trabalhou com geração de dados sintéticos.  
> Entrega: entender o papel do seeder, chaos engineering básico e como isso simula produção real.

```
00:00 - 01:00  "Como você testa um pipeline sem dados reais? Com dados falsos que se comportam como reais."
01:00 - 03:30  O que um seeder faz: gera, valida e insere dados com estrutura de produção
03:30 - 06:00  Chaos engineering básico: por que injetamos erros de propósito (CPFs inválidos, duplicatas)
06:00 - 07:30  Argparse + integração com Airflow BashOperator (preview do EP07)
07:30 - 08:00  "No EP02 você vai ver o seeder rodando contra a API que construímos."
```

---

**EP02.4 — Deep Track (12 min): "SQLAlchemy 2.0 — ORM, Async e a arquitetura de repositórios"**

> Para quem: quem conhece SQL mas nunca usou ORM, ou usou SQLAlchemy 1.x e ficou confuso com a versão 2.0.  
> Entrega: entender Model → Repository como separação de responsabilidades, e por que zero for-loops em inserts.

```
00:00 - 01:00  "ORM não é magia — é uma camada de tradução entre Python e SQL"
01:00 - 04:00  SQLAlchemy 1.x vs 2.0: o que mudou e por que importa (async nativo)
04:00 - 07:00  Model vs Repository: por que separar acesso a dados de regra de negócio
07:00 - 10:00  bulk insert com insert().values(matrix) — por que isso escala e o for-loop não
10:00 - 12:00  "Agora você está pronto para o EP02 — o full build da camada de ingestão."
```

---

### PARTE 8 — Theory Track: O Clube do Livro como Terceira Trilha

**ALEX:** Tem uma coisa que falta na maioria dos canais técnicos em português — e que diferencia os canais que viram referência dos que ficam estagnados em 5 mil inscritos. A *justificativa* das decisões. Qualquer um pode mostrar como instalar o Kafka. Poucos conseguem explicar *por que* Kafka aqui e não RabbitMQ, Pulsar ou uma fila simples. A resposta não está no código — está nos livros. E o Clube do Livro resolve isso de um jeito que eu nunca vi ser feito em português.

**MARINA:** O que torna essa proposta diferente é o timing. O Clube não acontece *depois* do episódio — como complementar opcional que ninguém assiste. Ele acontece *antes*. A leitura antecede a implementação por design. Isso transforma o canal de um repositório de tutoriais em um registro de como um engenheiro sênior constrói algo do zero — lendo antes de codar, documentando as decisões, entendendo os trade-offs. Essa frase resume o posicionamento: **"a gente não implementa sem entender por que"**. Eu nunca vi outro canal em português usar isso como diferencial. Esse é o espaço vazio que você ocupa.

**RAFAEL:** Do ponto de vista de produção, o Theory Track tem um formato completamente diferente das outras duas trilhas. Não é tutorial — é discussão de leitura. Eu chamaria de **"Lendo Junto"**: 8-10 minutos onde você pega capítulos específicos do livro da vez e extrai os 3-5 conceitos que vão aparecer no próximo Fast Track. Não é resumo do livro inteiro — é a ponte entre a teoria e a implementação. Câmera mais presente, anotações na tela, ritmo conversacional. Thumbnail roxo — para a audiência saber imediatamente ao ver a miniatura: isso é Clube do Livro, não é Fast Track nem Deep Track.

**ALEX:** E tem um terceiro elemento além do vídeo: o Discord. Clube do Livro sem comunidade é só mais um vídeo. Com o Discord, cada sessão de leitura vira um debate — a audiência traz dúvidas, discordâncias, exemplos de como aplicou aquilo na empresa delas. Isso é conteúdo gerado pela comunidade que você não consegue produzir sozinho. Uma live mensal onde você aplica o conceito do livro em código ao vivo fecha o ciclo perfeito: **leitura → debate no Discord → implementação ao vivo → Fast Track**.

**MARINA:** A cadência que faz sentido: uma sessão do Clube por episódio Fast Track, saindo 1 semana antes. Com o Fast Track saindo a cada 2 semanas, o Clube sai na semana alternada. A pessoa que acompanha o canal tem conteúdo toda semana — e sempre com sequência lógica. No longo prazo, essa é também a semente de monetização: parceria com editoras para distribuir exemplares aos membros do Discord, assinatura premium para as lives mensais, bootcamp que percorre a trilha completa com os livros como base teórica.

#### Livros da Temporada 1 — Mapeados por Decisão Arquitetural

| Temporada | Livro | Capítulos Relevantes | Fundamenta |
|-----------|-------|---------------------|------------|
| T0 | **The Data Warehouse Toolkit** (Kimball & Ross) | Caps 1–4: Modelagem dimensional, Star Schema, Grain, SCDs | EP01, EP02, EP07 |
| T1 | **Fundamentos de Engenharia de Dados** (Reis & Housley) | Ciclo de vida, Medallion, Data Quality como disciplina | EP02, EP03, EP07 |
| T2 | **Designing Data-Intensive Applications** (Kleppmann) | Caps 3–4: Storage Engines + Cap 11: Stream Processing | EP03, EP04, EP05 |
| T3 | **Domain-Driven Design** (Khononov) | Agregados, Consistência de Domínio, Outbox Pattern | EP05 |
| T4 | **Software Architecture: The Hard Parts** (Ford & Richards) | Decomposição, CQRS, Trade-offs em sistemas distribuídos | EP06 |
| T5 | **Gang of Four — Design Patterns** (GoF) | Repository, Factory | EP02 (retroativo) |
| T6 | **Fluent Python** (Ramalho) | Generators, Protocolos, Async nativo | EP02, EP04 |

**ALEX:** Repara no posicionamento que emerge disso tudo: você não está apenas ensinando ferramentas. Você está mostrando *como um engenheiro sênior pensa* — lê antes de implementar, documenta as decisões em ADRs, entende os trade-offs, admite o que ainda não sabe. Esse é o canal que um engenheiro sênior vai recomendar para o júnior quando ele perguntar: *"como você aprende arquitetura de verdade?"*

#### Formato dos Vídeos "Lendo Junto" (8-10 min)

```
00:00 - 01:00  Hook: qual decisão do projeto esse livro fundamenta
01:00 - 03:00  Contexto: por que lemos esse livro agora (a implementação vem na semana seguinte)
03:00 - 07:00  Os 3–5 conceitos-chave que vão aparecer no Fast Track correspondente
07:00 - 09:00  Como esses conceitos se manifestam no código do projeto (preview)
09:00 - 10:00  "Na semana que vem, no EP[N], você vai ver isso acontecendo na plataforma real."
               CTA para Discord: "A discussão continua lá — link na descrição."
```

---

### PARTE 6 — Engajamento, Credibilidade e Crescimento

**MARINA:** Um canal técnico cresce por **credibilidade demonstrada**, não por algoritmo. A melhor estratégia de crescimento é simples: o conteúdo precisa funcionar. Se o espectador seguir seus passos e chegar no mesmo resultado, ele vira fã e compartilha. Se der erro, ele abandona e não volta. Por isso, **teste tudo antes de gravar**. O vídeo é a performance ensaiada, não o experimento ao vivo.

**RAFAEL:** Exceto quando o erro é intencional e pedagógico. Um vídeo chamado "Eu errei a arquitetura e tive que refazer — aprenda com meus erros" pode ser extremamente poderoso. Mostra processo real, não a versão sanitizada.

**ALEX:** Isso é o que chamo de **vulnerabilidade estratégica**. Não é expor fraqueza — é mostrar que você já esteve onde o espectador está agora. "Eu passei 2 dias tentando entender por que o Spark não conectava no MinIO. O problema era esse campo aqui no spark-defaults.conf." Isso é ouro narrativo.

**MARINA:** Para engajamento nos comentários: faça perguntas abertas ao final de cada vídeo. Não "deixe seu like", mas "qual serviço você está tendo mais dificuldade de entender? Me conta nos comentários." Respostas geram mais respostas, e o YouTube interpreta como sinal de qualidade.

**ALEX:** E uma última coisa sobre narrativa de canal: **documente a jornada em tempo real**. Se você ainda está construindo partes do projeto, filme o processo. "Hoje fui implementar o particionamento por data no Iceberg e descobri que..." — isso é mais autêntico e conecta mais do que vídeos de tutoriais polidos onde tudo sempre funciona na primeira tentativa.

**MARINA:** Resumindo a filosofia do canal em uma frase: **"Mostre o trabalho real, explique o raciocínio por trás das decisões, e deixe o dado fluindo no final para provar que funciona."**

---

## BLOCO II — ECOSSISTEMA: O Papel de Cada Plataforma

### YouTube — O Motor (Conteúdo Pilar)
- **Função**: Profundidade técnica + prova de competência
- **Formato**: 15-25 min por vídeo, série estruturada
- **Audiência**: Engenheiros de dados, devs backend, estudantes
- **Métrica que importa**: Watch Time + Subscribers
- **Frequência ideal**: 1 vídeo a cada 2 semanas (consistência > quantidade)

### LinkedIn Newsletter — O Conversor (Audiência Quente)
- **Função**: Transformar conexões passivas em leitores ativos + nutrir os 500 subs existentes
- **Formato**: 800-1.200 palavras, ângulo de carreira/decisão técnica (não tutorial)
- **Audiência**: Os mesmos que te seguem no LinkedIn — recrutadores, gestores, pares técnicos
- **Métrica que importa**: Open Rate + Novos assinantes por edição
- **Frequência ideal**: 1 edição por quinzena (sincronizado com o YouTube)

### LinkedIn Posts — O Amplificador (Alcance Orgânico)
- **Função**: Atrair novos seguidores via feed + direcionar para newsletter e YouTube
- **Formato**: 3-5 posts por semana, cada um extraído do vídeo da semana
- **Audiência**: Feed aberto do LinkedIn — alcance viral possível
- **Métrica que importa**: Impressões + Novos seguidores
- **Frequência ideal**: 3-5x por semana

### Medium — O Arquivo (SEO de Longo Prazo)
- **Função**: Tráfego orgânico via Google + credencial escrita permanente
- **Formato**: Artigo técnico aprofundado, 1.500-3.000 palavras, com código
- **Audiência**: Quem pesquisa "apache iceberg tutorial", "airflow spark integration" no Google
- **Métrica que importa**: Views orgânicas + Follows
- **Frequência ideal**: 1 artigo por vídeo publicado

### Instagram — O Humanizador (Audiência Fria → Morna)
- **Função**: Top of funnel — apresentar você para pessoas que não te conhecem ainda
- **Formato**: Carrosséis técnicos (diagramas, "5 coisas sobre X") + Stories de bastidores
- **Audiência**: Devs e estudantes de tecnologia que usam Instagram
- **Métrica que importa**: Reach + Saves (saves = conteúdo útil)
- **Frequência ideal**: 3x por semana (2 carrosséis + 1 reels curto ou stories)

### Discord — A Comunidade (Theory Track Hub)
- **Função**: Debate das leituras do Clube + comunidade de prática dos membros mais engajados
- **Formato**: Canais por livro/temporada, canal de dúvidas por episódio, voz mensal para a live do Clube
- **Audiência**: Os 10-20% mais engajados do canal — quem quer ir além do vídeo
- **Métrica que importa**: Membros ativos + tópicos criados por sessão do Clube
- **Frequência**: Sempre ativo — cada Clube T[N] gera um tópico de debate com duração de 1 semana
- **Monetização futura**: Acesso premium às lives mensais, sessões privadas de ADR, bootcamp ao vivo

---

## BLOCO III — O FLUXO: Como Tudo Se Conecta

### Semana tipo com 1 vídeo publicado:

```
DIA 1 (Segunda) — Gravação/edição do vídeo YouTube
DIA 2 (Terça)  — Publicar YouTube + LinkedIn Post #1 (insight principal do vídeo)
DIA 3 (Quarta) — Instagram Carrossel (diagrama de arquitetura do vídeo)
DIA 4 (Quinta) — LinkedIn Post #2 (erro/aprendizado do vídeo) + Medium artigo publicado
DIA 5 (Sexta)  — LinkedIn Newsletter (versão editorial do tema) + LinkedIn Post #3 (CTA para newsletter)
DIA 7 (Domingo)— Stories de bastidores do próximo vídeo (Instagram)
```

### Como cada post é extraído do mesmo vídeo:

**Exemplo: EP06 — Apache Spark**

| Plataforma | Ângulo | Título/Legenda |
|-----------|--------|----------------|
| YouTube | Tutorial técnico completo | "Apache Spark no Docker: do zero ao job rodando em 20 min" |
| LinkedIn Post #1 | Insight de decisão | "Por que escolhi Spark em vez de DuckDB para esse projeto (e quando eu erraria essa escolha)" |
| LinkedIn Post #2 | Vulnerabilidade técnica | "Passei 3 horas travado nessa config do Spark. O problema era uma linha no spark-defaults.conf" |
| LinkedIn Post #3 | CTA editorial | "Escrevi sobre isso na newsletter essa semana. Link nos comentários." |
| Medium | Artigo técnico com código | "Spark + MinIO + Iceberg: configuração completa com exemplos reais" |
| Newsletter | Ângulo de carreira | "O que saber sobre Spark antes da próxima entrevista de dados (e o que ninguém te conta)" |
| Instagram | Visual | Carrossel: "5 conceitos do Spark que você precisa entender antes de usar em prod" |

---

## BLOCO IV — PLATAFORMAS: Guia Específico por Canal

### LinkedIn Newsletter — Reativando os 500 Subs

Essa é sua maior vantagem imediata. 500 assinantes ativos valem mais que 10.000 seguidores frios.

**Estratégia de reativação em 3 etapas:**

**Etapa 1 — Edição de Retorno (Semana 1)**
Não finja que nunca parou. Seja direto:

> *"Essa newsletter ficou parada por [X] meses. Vou explicar por quê parei, o que estava construindo nesse tempo, e o que muda agora."*

Mostre o projeto — a plataforma de dados — como o motivo. Você estava construindo o conteúdo, não apenas publicando sobre ele. Isso ressignifica o silêncio.

**Etapa 2 — Série de Onboarding (Semanas 2-4)**
Publique 3 edições introdutórias antes de entrar na cadência normal:
- Edição 2: "A stack que estou documentando e por que escolhi cada ferramenta"
- Edição 3: "O maior erro que cometi montando essa arquitetura"
- Edição 4: "O que você vai aprender acompanhando essa série"

**Etapa 3 — Cadência Quinzenal Sustentável**
Uma edição a cada duas semanas, sempre sincronizada com o vídeo do YouTube.

**Formato da Newsletter LinkedIn (template):**

```
LINHA DE ASSUNTO: [Número] + [benefício específico] + [emoção]
Exemplos:
- "3 decisões de arquitetura que eu refaria (e o que aprendi)"
- "Por que o Airflow quase me fez desistir — e como resolvi"
- "A query que deveria demorar 3h rodou em 4 segundos. Veja como."

ESTRUTURA INTERNA:
• Abertura (2-3 linhas): gancho + o que vem nessa edição
• Contexto (1 parágrafo): por que esse tema importa agora
• Conteúdo principal (3-4 parágrafos): o ensinamento real
• Lição transferível (1 parágrafo): o que isso muda na prática
• CTA (2 linhas): link para o vídeo + convite para responder
```

---

### LinkedIn Posts — A Fábrica de Alcance

O feed do LinkedIn tem um mecanismo diferente de qualquer outra rede: **texto longo sem link performa melhor** que posts com link externo. O algoritmo pune quem "faz as pessoas saírem do LinkedIn".

**Tática principal: publicar sem link → responder com link nos comentários**

```
Post principal: Insight completo, sem link
Primeiro comentário (seu): "Gravei um vídeo detalhando isso aqui: [link]"
```

**5 tipos de posts que funcionam:**

**Tipo 1 — O Contraintuitivo**
*"Todo mundo fala para usar [tecnologia X]. Eu discordo. Aqui está o porquê."*

**Tipo 2 — O Número com História**
*"50 milhões de registros inseridos em 12 minutos. Era impossível há 6 meses com a arquitetura que eu tinha."*

**Tipo 3 — A Lição Cara**
*"O erro que me custou 2 dias de debug. Não cometa esse."*

**Tipo 4 — O Carrossel de Conceito**
*"5 ferramentas open-source que substituem serviços AWS pagos [carrossel]"*

**Tipo 5 — O Bastidor**
*"Gravando o EP07 da série sobre Airflow. Aqui está o diagrama que usei para organizar o raciocínio antes de codar."*

---

### Medium — SEO de Longo Prazo

O Medium é o único canal onde o conteúdo tem valor crescente no tempo — um artigo publicado hoje pode gerar tráfego por anos via Google.

**Dica crítica:** Publique **primeiro no Medium**, espere 3 dias para indexação do Google, depois compartilhe no LinkedIn. Se você compartilhar antes de indexar, o LinkedIn vai "roubar" a audiência e o Google vai tratar como conteúdo duplicado.

**Estratégia de keywords por episódio:**

| Artigo | Keyword Principal |
|--------|------------------|
| EP01 overview | "plataforma de dados open source do zero" |
| EP02 FastAPI | "fastapi postgresql async sqlalchemy 2.0" |
| EP03 Delta Lake | "delta lake tutorial docker unity catalog" |
| EP07 Airflow | "apache airflow docker compose tutorial" |
| EP08 Trino | "trino delta lake query tutorial" |
| EP09 Seeders | "chaos engineering data quality python" |

**Estrutura de artigo no Medium:**

```markdown
# Título com keyword + benefício

## TL;DR (3 bullets — para quem lê scanning)

## O Problema
(Por que esse tema existe?)

## A Solução
(Código + explicação alternada — nunca bloco de código sem contexto)

## Armadilhas Comuns
(O que dá errado — seção mais lida)

## Conclusão
(O que você levaria desse artigo em 3 linhas)

## Próximos passos
(Link para o YouTube + próximo artigo)
```

---

### Instagram — Conteúdo Técnico Que Funciona Visualmente

O segredo é: **não tente ser o que o Instagram não é**. Você não vai ensinar Spark em 30 segundos. Use o Instagram para **despertar curiosidade**, não para ensinar.

**Carrossel de Arquitetura** (melhor formato)
- Slide 1: Diagrama visual da stack completa (você já tem o `diagram.py`)
- Slides 2-6: Cada serviço isolado com 1 frase explicando sua função
- Slide final: "Tutorial completo no YouTube — link na bio"
- Saves > Likes nesse formato — e saves é o que o algoritmo valoriza

**Reels de Terminal** (segundo melhor)
- 30-60 segundos de um pipeline rodando com música lo-fi
- Legenda: "50 milhões de registros. 12 minutos. Zero cloud." + CTA
- Não precisa de narração — o terminal falando sozinho já é conteúdo

**Stories de Bastidores**
- Setup do computador, segundo monitor com terminal, configuração do microfone
- Erros em tempo real ("travado nessa config há 2 horas")
- Enquetes: "Você usa Airflow ou Prefect?" — engajamento fácil

**O que NÃO fazer:**
- Tutoriais em formato de foto estática (ninguém lê)
- Posts com muito texto na imagem
- Frequência inconsistente (2x/semana sempre > 7x/semana por 2 semanas e sumir)

---

## BLOCO V — O FLYWHEEL: Como Tudo Se Auto-Alimenta

```
YOUTUBE FAST TRACK (EP01–EP09)
 → gera prova técnica e conteúdo pilar com ADRs como abertura
 → alimenta MEDIUM (SEO) e NEWSLETTER (audiência quente)
 → referencia DEEP TRACK para espectadores sem base
 → cada episódio tem um Clube correspondente que foi ao ar 1 semana antes

YOUTUBE THEORY TRACK / CLUBE DO LIVRO (Clube T1, T2, T3, T4)
 → acontece 1 semana antes do Fast Track correspondente
 → fundamenta as decisões arquiteturais com livros técnicos de referência
 → gera debate no DISCORD — comunidade de prática do canal
 → a audiência mais engajada migra para o núcleo do canal (Discord)
 → cria diferencial único: "o canal que lê antes de implementar"

YOUTUBE DEEP TRACK (EP02.1, 02.2...)
 → captura buscas de fundamentos (alto volume, baixa concorrência em PT-BR)
 → converte iniciantes em seguidores do FAST TRACK
 → gera artigos no MEDIUM com keywords de topo de funil

DISCORD (Clube do Livro Hub)
 → debate das leituras após cada Theory Track
 → live mensal de aplicação com a audiência participando
 → embrião da comunidade paga (premium, bootcamp)
 → fonte de dúvidas reais para futuros episódios Deep Track

LINKEDIN POSTS
 → alcance orgânico no feed
 → ADRs publicados como posts técnicos (alto engajamento — opinativo e convidativo)
 → novos seguidores descobrem a NEWSLETTER
 → newsletter converte em assinantes do YOUTUBE (todas as três trilhas)

INSTAGRAM
 → audiência fria descobre você
 → vai para o LinkedIn ou YouTube (Deep Track é mais acessível como porta de entrada)
 → entra no ecossistema

MEDIUM
 → Google traz tráfego orgânico constante (Fast Track = keywords avançadas, Deep Track = keywords de volume)
 → leitor encontra link para YOUTUBE
 → se inscreve no canal
```

**Em 6 meses**: O Fast Track cresce via LinkedIn (rede técnica) + Medium (SEO avançado). O Deep Track cresce via Google (keywords de fundamento) + Instagram (top of funnel). O Theory Track diferencia o canal — atrai quem quer mais que tutoriais e constrói o núcleo mais fiel da audiência. O Discord transforma espectadores em comunidade. As três playlists se alimentam mutuamente: o iniciante entra pelo Deep Track, vira fã pelo Fast Track, vira membro pelo Theory Track.

---

## BLOCO VI — EXECUÇÃO: Plano dos Primeiros 30 Dias

### Semana 1 — Fundação
- [ ] Escrever a frase de posicionamento do canal
- [ ] Reativar newsletter com a "Edição de Retorno"
- [ ] Publicar 3 LinkedIn posts de aquecimento (sem link ainda)
- [ ] Gravar EP01 do YouTube (overview da stack)

### Semana 2 — Lançamento
- [ ] Publicar EP01 no YouTube
- [ ] Publicar artigo EP01 no Medium (3 dias antes de divulgar no LinkedIn)
- [ ] LinkedIn Post com insight principal do EP01 (sem link no post)
- [ ] Instagram: carrossel com diagrama da stack

### Semana 3 — Cadência
- [ ] Newsletter Edição 2 (com link para EP01)
- [ ] 2 LinkedIn posts adicionais sobre o tema do EP01
- [ ] Começar gravação do EP02
- [ ] Stories de bastidores no Instagram

### Semana 4 — Validação
- [ ] Analisar métricas: qual post do LinkedIn teve mais alcance? Qual tipo?
- [ ] Qual foi a taxa de abertura da newsletter?
- [ ] Quantas views o EP01 teve?
- [ ] Ajustar o que não funcionou antes de entrar no ritmo do mês 2

---

---

## BLOCO VIII — O Caso de Negócio: O Marketplace NexoMart

### O Contexto Narrativo

**ALEX:** A decisão mais importante para o canal é: não vamos ensinar tecnologias no vácuo. Vamos contar a história de uma empresa. A empresa se chama **NexoMart** — um marketplace brasileiro que está nascendo. Eles têm o banco de dados operacional desenhado. Eles precisam de uma plataforma de dados para analisar o negócio. É exatamente o problema que qualquer engenheiro de dados vai enfrentar na vida real. A diferença é que vamos construir tudo do zero, ao vivo, e documentar cada decisão.

**MARINA:** Do ponto de vista de comunicação, isso muda como os episódios funcionam. O espectador não está assistindo a um tutorial sobre FastAPI — está acompanhando a construção do backend analítico de uma empresa real. Cada ferramenta tem uma justificativa de negócio. O Kafka entra porque o marketplace precisa processar eventos de pedido em tempo real sem travar a API. O Iceberg entra porque precisamos versionar o catálogo de produtos para auditoria. A audiência entende o *porquê* antes do *como*.

**RAFAEL:** E visualmente, isso é ouro. Em vez de abrir um terminal em branco no EP01, você abre o diagrama do banco de dados. Você mostra os 7 schemas. Você explica quem é o Seller, quem é o Customer, o que está em cada tabela. O `business_db.png` já é o slide de abertura do episódio.

---

### O Banco de Dados Operacional (OLTP) do NexoMart

Arquivo de referência: `business_db.sql` + `business_db.png`

O schema define o banco transacional em **7 domínios isolados** — cada schema é um bounded context (conexão direta com DDD, Clube T3).

#### Os 7 Schemas e o que Representam

| Schema | Domínio | Tabelas-chave | Decisão Arquitetural de Destaque |
|--------|---------|--------------|----------------------------------|
| `product` | Catálogo | product, productcategory, productmodel, **productembedding** | JSONB para atributos variáveis por categoria. **pgvector(1536)** para busca semântica — o banco já é AI-ready. |
| `person` | Identidade | customer, **seller**, customeremail, countryregion | Seller como entidade de 1ª classe confirma o modelo de marketplace. Dados bancários no JSONB (desafio de PII e LGPD). |
| `sales` | Transações | salesorder, **ordereventlog**, returnorder, saleschannel | `order_items` em JSONB multi-seller. `ordereventlog` como event sourcing lite (cada mudança de status logada). |
| `marketing` | Aquisição | channel, campaign, adgroup, advertise, **dailyperformance** | Hierarquia de 4 níveis (Channel → Campaign → AdGroup → Ad). `dailyperformance` já tem cara de tabela fato. |
| `humanresources` | Pessoas | employee, department, jobrole | **SCD Type 2 nativo** — `effective_start_date`, `effective_end_date`, `is_current`. Auto-referência em `department` para hierarquia de org. |
| `purchasing` | Suprimentos | vendor, purchaseorderheader, purchaseorderdetail | Header + Detail com `employee_id` no header — o Comprador vem do HR. `audit_trail` em JSONB com histórico de aprovações. |
| `warehouse` | Logística | warehouselocation, productinventory, transactionhistory | `transactionhistory` com qty positivo (entrada) / negativo (saída). `capabilities` JSONB com dados de IoT e cold chain. |

#### 5 Decisões de Design que Viram Episódios

**1. `order_items` como JSONB (ADR implícito)**
Por que não criar `sales.salesorderdetail`? Em um marketplace multi-seller, cada item tem `delivery_info`, `taxes` e `seller_id` próprios. A maioria das queries de cabeçalho nunca precisa da granularidade de item. O custo: para análise, precisamos de `jsonb_array_elements` no SQL ou `explode()` no Spark. → *Deep Track EP02.5*

**2. `productembedding` com pgvector**
O banco prevê busca semântica desde o design. `description_vector(1536)` + `image_vector(512)` gerados via ETL. → *Episódio futuro: ML + DE + pgvector*

**3. SCD Type 2 no `employee`**
O banco transacional já implementa Slowly Changing Dimensions com `effective_start_date / effective_end_date / is_current`. Quando o engenheiro modelar o DW, vai encontrar um aliado inesperado. → *Deep Track EP03.3 + Kimball Clube T0*

**4. `ordereventlog` como event sourcing lite**
Cada transição de estado de um pedido logada: `PAYMENT_APPROVED`, `ITEM_PICKED`, `DELIVERY_ATTEMPT_FAILED`. → *Conecta com DDIA Cap 11 (Clube T3) e Kafka EP05*

**5. Schemas como bounded contexts**
`product.product` não tem tabela de endereço — atributos variáveis ficam no JSONB. Endereço do customer em `demographics`. Endereço do seller em `demographics`. Geo do warehouse em `geo_location`. → *Conecta com DDD (Clube T3)*

---

### O Problema Analítico — Por Que o OLTP Não Basta

**MARINA:** O gancho do EP01 não é "vamos aprender FastAPI". É: *"esse banco guarda tudo que o NexoMart faz. Mas quando o CEO pergunta qual categoria tem a maior margem, qual seller tem menor chargeback, ou qual campanha gerou mais receita — esse banco não responde com performance. Por quê? E o que fazemos?"*

**ALEX:** Mostre o schema funcionando, insira dados com a API, tente fazer a query analítica — e mostre o explain plan explodindo. Ou melhor: mostre que a query *correta nem consegue ser formulada* porque os `order_items` estão no JSONB. *"A resposta para isso é o que vamos construir nos próximos 9 episódios."* Esse é o cliffhanger do EP01.

| Pergunta de Negócio | Problema no OLTP | Solução na Data Platform |
|--------------------|-----------------|------------------------|
| Qual produto vendeu mais esta semana por categoria? | `order_items` em JSONB precisa de unnest | `fact_sales_order_item` (grain: linha de pedido) |
| Qual seller tem a maior margem líquida? | Cruzar 4 tabelas + parse JSONB | `dim_seller` + `fact_sales` com métricas pré-calculadas |
| Como a campanha Google Ads converteu vs Meta? | `dailyperformance` sem join com jornada do cliente | `fact_marketing_performance` + atribuição |
| Quantos dias em média um pedido fica em cada status? | `ordereventlog` precisa de window functions pesadas | `fact_order_lifecycle` com SLA pré-calculado |
| O estoque do warehouse A dura quantos dias? | `transactionhistory` sem agregação histórica | `fact_inventory_snapshot` (Daily Snapshot) |
| Qual comprador aprovou mais POs com fornecedores problemáticos? | Cruzar `purchasing` + `humanresources` + `vendor` | `fact_purchase_order_line` + `dim_vendor` com `credit_rating` |

---

### O Modelo Dimensional — Bus Matrix do NexoMart

A transformação OLTP → Dimensional é o coração da série. O Kimball (Clube T0) fundamenta as decisões antes de construirmos.

**RAFAEL:** A Bus Matrix é o artefato que abre o EP02. Você coloca ela na tela — uma tabela simples — e diz: *"isso é o mapa de navegação da nossa plataforma. Cada linha é um fato. Cada coluna é uma dimensão."* A audiência entende a arquitetura completa antes de ver uma linha de código.

| Fato | date | customer | product | seller | channel | warehouse | employee | vendor | special_offer |
|------|:----:|:--------:|:-------:|:------:|:-------:|:---------:|:--------:|:------:|:-------------:|
| fact_sales_order_item | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| fact_inventory_snapshot | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| fact_marketing_performance | ✓ | — | — | — | ✓ | — | — | — | — |
| fact_purchase_order_line | ✓ | — | ✓ | — | — | — | ✓ | ✓ | — |
| fact_order_lifecycle | ✓ | ✓ | — | ✓ | ✓ | — | — | — | — |

#### Grain de Cada Fato

| Fato | Grain (1 linha = 1 ...) | Fonte OLTP | SCD nas Dims |
|------|------------------------|------------|--------------|
| `fact_sales_order_item` | Item dentro de um pedido (unnest do JSONB) | `sales.salesorder.order_items` | dim_customer SCD2, dim_product SCD2 |
| `fact_inventory_snapshot` | Produto × Warehouse × Dia | `warehouse.productinventory` | dim_product SCD2 |
| `fact_marketing_performance` | Anúncio × Dia | `marketing.dailyperformance` | — |
| `fact_purchase_order_line` | Linha de ordem de compra | `purchasing.purchaseorderdetail` | dim_employee SCD2 (já no OLTP!) |
| `fact_order_lifecycle` | Pedido × Evento de status | `sales.ordereventlog` | dim_customer SCD2 |

---

### Episódios Extras Gerados pelo Caso NexoMart

| Episódio | Trilha | Título |
|----------|--------|--------|
| Clube T0 | Theory Track | Kimball Caps 1–3: O que é modelagem dimensional e por que o OLTP não basta |
| EP01.3 | Deep Track | OLTP vs OLAP: por que não conseguimos analisar direto do banco transacional |
| EP01.4 | Deep Track | Fato, Dimensão e Grain: os 3 conceitos do Kimball no contexto do NexoMart |
| EP01.5 | Deep Track | Star Schema na prática: desenhando o DW do NexoMart — Bus Matrix e grain decisions |
| EP02.5 | Deep Track | Explodindo JSONB: como transformar `order_items` em linhas analíticas no Spark |
| EP03.3 | Deep Track | SCD Type 2 na prática: clientes e produtos que mudam com o tempo |
| EP07.3 | Deep Track | pgvector no DW: busca semântica de produtos integrada ao pipeline |

---

## BLOCO VII — ADRs como Conteúdo: Cada Decisão é um Episódio

**ALEX:** Uma das ideias mais poderosas que emerge da combinação das três trilhas é o uso de ADRs — Architecture Decision Records — como conteúdo. Um ADR é um documento que registra *por que* uma decisão arquitetural foi tomada, quais alternativas foram consideradas e quais trade-offs foram aceitos. É o que empresas de dados de ponta fazem internamente. O diferencial é fazer isso publicamente, ao vivo, com a audiência acompanhando.

**RAFAEL:** Na prática, cada episódio Fast Track abre com o ADR em tela — literalmente um Markdown ou Notion formatado aparecendo por 30-60 segundos: *"Aqui está a decisão que tomamos. Aqui estão as alternativas que rejeitamos. Aqui está por que."* Isso transforma o vídeo de tutorial em raciocínio arquitetural documentado. Não é um tutorial sobre Kafka — é um registro de *por que escolhemos Kafka* e o que o DDIA Cap 11 diz sobre isso.

**MARINA:** E isso tem um efeito imediato no posicionamento: você para de competir com tutoriais de instalação e começa a competir com engenharia de software aplicada. "Como instalar Kafka" tem milhares de vídeos. "Por que Kafka e não RabbitMQ nesta arquitetura específica, fundamentado em Kleppmann" — tem praticamente zero em português. É o espaço vazio.

#### ADRs da Temporada 1

| ADR | Decisão | Livro que Fundamenta | Status |
|-----|---------|---------------------|--------|
| ADR-001 | SQLAlchemy async com bulk insert — sem for-loops | Fluent Python — generators e iteradores eficientes | Aceito |
| ADR-002 | Delta Lake + Unity Catalog OSS para governança e versionamento | DDIA Caps 3–4 + Fundamentos DE | Aceito |
| ADR-003 | Chaos Engineering como estratégia de qualidade de dados | Fundamentos DE — confiabilidade como disciplina | Aceito |
| ADR-004 | Kafka como buffer entre API e armazenamento (event-driven) | DDIA Cap 11 — Stream Processing e o log imutável | Proposto |
| ADR-005 | Outbox Pattern para exactly-once semantics com Kafka | DDD (Khononov) — consistência de domínio e transações | Proposto |
| ADR-006 | CQRS com Trino para separar read/write paths | As Partes Difíceis — decomposição e trade-offs arquiteturais | Proposto |
| ADR-007 | dbt como camada de transformação Silver → Gold | Fundamentos DE — transformação e qualidade como prática | Proposto |
| ADR-008 | OpenLineage + Marquez para rastreabilidade de linhagem | Fundamentos DE — observabilidade de dados | Proposto |

**ALEX:** Os ADRs com status "Proposto" são conteúdo em si. A audiência acompanha as decisões sendo tomadas em tempo real — não edições polidas de algo que já funcionou. *"Estamos debatendo se usamos Kafka ou não. Aqui está o argumento pró. Aqui está o contra. No Clube T3 a gente lê o DDIA Cap 11 juntos. Depois do Clube, tomamos a decisão e implementamos no EP05."* Isso é engajamento genuíno, não artificialmente estimulado.

**MARINA:** Do ponto de vista de LinkedIn, cada ADR é um post. "Tomamos uma decisão arquitetural hoje. O argumento que virou o debate: [trecho do DDIA]. Detalhes no vídeo." Esse formato performa extremamente bem — é opinativo, técnico e convida à discussão sem precisar de clickbait. E os ADRs "Proposto" viram dois posts: o debate antes, e a decisão depois.

---

## Métricas de Sucesso (90 dias)

| Plataforma | Métrica | Meta Realista |
|-----------|---------|--------------|
| YouTube | Subscribers | +100 orgânicos |
| YouTube | Watch Time | >40% retenção média |
| Newsletter | Open Rate | >30% |
| Newsletter | Novos subs | +50 por mês |
| LinkedIn | Impressões/post | >2.000 médio |
| Medium | Views | >500 por artigo |
| Instagram | Saves por carrossel | >50 |

---

## CHECKLIST ANTES DE GRAVAR CADA VÍDEO

**Fast Track (série principal):**
```
[ ] A promessa do vídeo está clara nos primeiros 30 segundos?
[ ] O problema que este vídeo resolve está explícito?
[ ] Existe uma analogia ou metáfora para cada conceito novo?
[ ] O diagrama/arquitetura do episódio está preparado?
[ ] O ambiente está funcionando localmente (testado)?
[ ] Os capítulos/timestamps estão planejados?
[ ] O cliffhanger para o próximo episódio está definido?
[ ] O title e thumbnail comunicam claramente o valor?
[ ] Os sub-episódios Deep Track estão linkados na descrição?
```

**Deep Track (fundamentos):**
```
[ ] O conceito central está definido em uma frase antes de gravar?
[ ] Existe pelo menos uma analogia do mundo real (não técnica)?
[ ] O vídeo termina com uma ponte clara para o Fast Track correspondente?
[ ] A duração está dentro de 8-12 minutos (foco cirúrgico)?
[ ] O título contém a keyword de busca principal ("o que é X", "como funciona Y")?
```

---

## TEMPLATES DE ROTEIRO

### Fast Track — Roteiro Base (15-25 min)

```markdown
# EP[N] — [Título]

## HOOK (00:00 - 00:30)
[Mostre o resultado final rodando. Uma frase de impacto.]
[Mencionar: "Se você não conhece [conceito X], assista o EP[N.1] antes — link na descrição."]

## CONTEXTO (00:30 - 02:00)
- Recap do episódio anterior (se houver)
- O que construímos hoje e por quê

## DIAGRAMA (02:00 - 05:00)
- Onde esse serviço se encaixa na arquitetura total
- O fluxo de dados deste episódio

## HANDS-ON (05:00 - 18:00)
- Passo 1: [descrição]
- Passo 2: [descrição]
- Passo N: [descrição]
- [Documentar pontos de atenção e armadilhas comuns]

## TESTE E RESULTADO (18:00 - 20:00)
- Rodar o pipeline completo do episódio
- Mostrar o dado no destino final

## RECAP E CTA (20:00 - 21:00)
- O que construímos hoje
- O problema que fica em aberto (cliffhanger)
- Links para Deep Track relacionados
- "No próximo episódio..." + link para playlist Fast Track
```

### Deep Track — Roteiro Base (8-12 min)

```markdown
# EP[N.X] — [O que é / Como funciona / Por que usar — Conceito]

## HOOK (00:00 - 01:00)
[Problema concreto: "Você já tentou X e não entendeu por que travou?"]
[Sem mostrar código ainda — mostrar a confusão, não a solução]

## ANALOGIA (01:00 - 03:00)
- Explicar o conceito com analogia do mundo real (não técnica)
- Uma imagem mental que vai grudar

## CONCEITO (03:00 - 07:00)
- Explicação técnica agora, ancorada na analogia
- Diagrama simples (Excalidraw ou quadro branco)
- Uma demonstração mínima se aplicável (< 20 linhas de código)

## QUANDO USAR / QUANDO NÃO USAR (07:00 - 09:30)
- Casos de uso reais
- O que este conceito NÃO resolve (evitar expectativa errada)

## PONTE PARA O FAST TRACK (09:30 - 10:00)
- "Agora que você entende [conceito], você está pronto para o EP[N]."
- "Lá você vai ver isso funcionando em um pipeline real de dados."
- Link direto para o Fast Track correspondente
```

---

## Ferramentas Recomendadas (todas gratuitas ou freemium)

| Função | Ferramenta |
|--------|-----------|
| Design de carrosséis | Canva (templates prontos para devs) |
| Diagrama de arquitetura | Excalidraw ou o próprio `diagram.py` |
| Edição de vídeo | DaVinci Resolve (gratuito) |
| Calendário de conteúdo | Notion ou planilha simples |
| Agendamento LinkedIn | LinkedIn nativo (funciona bem) |
| Agendamento Instagram | Meta Business Suite (gratuito) |
| Análise de keywords Medium | Google Search Console + Ubersuggest |

---

*Gerado em: 2026-04-23 | Atualizado em: 2026-04-25*  
*Base: Data Platform com FastAPI, Kafka, Airflow, Spark, Delta Lake, Unity Catalog OSS, MinIO, Trino, dbt, OpenTelemetry*  
*Braço adicionado: Theory Track (Clube do Livro) + Discord + ADRs como conteúdo*
