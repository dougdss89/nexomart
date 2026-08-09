# Consolidated Content Strategy — Data Engineering Creator

> YouTube + LinkedIn + Medium + Instagram: how to create, distribute, and grow while documenting your Data Platform.

---

## The Personas Consulted

**ALEX** — Storytelling Specialist
*Believes every technical architecture hides a human story. Focused on narrative, conflict, resolution, and emotion.*

**MARINA** — Communication Specialist
*Obsessed with clarity, accessible language, and the gap between what the expert knows and what the audience understands.*

**RAFAEL** — Presentation Specialist
*Masters visual pacing, slides, live demos, camera, and editing. Knows that format can make or break the content.*

---

## The Core Principle: One Piece of Content, Five Formats

You're not going to create separate content for each platform. You're going to create **once** and **distribute in layers**. YouTube is the engine. Everything else is derived from it.

```
YOUTUBE (Pillar — three tracks)
        │
        ├── Fast Track (main series — EP01 to EP09)
        ├── Deep Track (fundamentals — EP02.1, 02.2...)
        ├── Theory Track / Book Club ──→ DISCORD (community)
        │
        ├── MEDIUM (Long-form text — organic SEO)
        ├── LINKEDIN NEWSLETTER (Professional angle — existing audience)
        ├── LINKEDIN POSTS (Individual insights + architectural ADRs)
        └── INSTAGRAM (Visual — diagrams / behind-the-scenes / carousel)
```

Each platform has a specific function in the ecosystem. Mixing up those functions breaks the strategy.

---

## BLOCK I — YOUTUBE: The Engine

### PART 1 — Positioning: Who Are You Talking To?

**MARINA:** Before recording the first frame, we need to answer a fundamental question: who is your audience? This project — FastAPI, Airflow, Spark, Iceberg, Trino, MinIO, Nessie — is dense. If you talk to beginners, you'll need 20 videos just on fundamentals. If you talk to seniors, you can get straight to the point. That decision changes everything.

**ALEX:** I'd say the ideal audience here is **mid-level data engineers and curious backend developers** who have already been through the frustration of putting together a data stack and discovered it's a minefield. They know Docker, they've seen Airflow before, but they've never put all of this together cohesively. That's the pain you solve — and pain is the beginning of every good story.

**RAFAEL:** From a presentation standpoint, this defines the *tone* of every video. For this audience, you don't need to explain what a Docker container is, but you do need to show *why* the sequence of services you chose makes sense and not some other order. The camera can stay in the background — the terminal and the dashboards are the visual protagonists.

**MARINA:** My recommendation: write a positioning statement before recording anything. Something like: *"This channel is for data engineers who want to build a modern, open-source, production-like data platform from scratch, without depending on proprietary cloud."* That becomes your editorial compass.

**ALEX:** And notice that this statement already has an implicit antagonist — proprietary cloud, lock-in, cost. An antagonist is narrative. You're not just teaching technology; you're defending a philosophy.

---

### PART 2 — Narrative Structure: The Video Arc

**ALEX:** Every successful technical video follows, consciously or not, the classic three-act structure. In the context of your project:

- **Act 1 (Problem):** "Have you ever tried to put together a data platform and gotten lost among 10 tools that don't talk to each other?"
- **Act 2 (Journey):** Here you show the path — the architecture decisions, the mistakes, the configuration, the code.
- **Act 3 (Resolution):** Data flowing from FastAPI to Trino, the query working, the dashboard appearing. The visual satisfaction of the system operating.

**MARINA:** This is essential. Most technical videos start straight into Act 2 — installation, configuration, code. Without Act 1, the audience doesn't know *why* they should care. You lose the viewer in the first 30 seconds.

**RAFAEL:** On YouTube, the first **15 to 30 seconds** determine whether the person stays or leaves. I recommend starting with the *final result running*: Trino querying an Iceberg table, an Airflow pipeline completing, the data appearing. Show the destination before starting the journey. Then go back to "but how did we get here?"

**ALEX:** That's what we call *in medias res* — starting in the middle of the action. The most powerful opening for the first video would be something like: "This terminal here is querying 50 million records in under 3 seconds. This pipeline generated this data 10 minutes ago. And all of this runs on your machine, with Docker, without spending a cent on cloud." — pause — "Let's build it?"

**MARINA:** Perfect because it establishes credibility, promise, and invitation in three sentences.

---

### PART 3 — Communicating Technical Complexity Without Losing Anyone

**MARINA:** This is the biggest challenge specific to your project. You have seven services that need to work together. If you try to explain all of them at once, nobody understands anything. The principle I use is the **onion layer**: start with what the viewer already knows and add one layer per video.

**ALEX:** Analogies are your best allies here. Some that would work:
- **MinIO** = "It's Amazon's S3, but it runs on your machine"
- **Nessie** = "It's Git for your data — you can create data branches, commit, rollback"
- **Iceberg** = "It's the layer that makes your data table smart: it knows what changed, when it changed, and who changed it"
- **Trino** = "It's the Google of your data — it doesn't store anything, it just knows where everything is and returns the result fast"
- **Airflow** = "It's your platform's schedule — it decides what runs, when it runs, and in what order"

**RAFAEL:** Each of these analogies can open an entire video. And visually, simple animated diagrams — even ones made in Excalidraw or in Python itself like you already did in `diagram.py` — are worth more than a thousand words. Show the data *moving* between services with arrows.

**MARINA:** Another powerful device: **repeat concepts in different contexts**. The second time you mention Iceberg, don't explain it again — just say "remember Iceberg is the Git of data? Here it's going to create a new snapshot automatically." It reinforces without being repetitive.

**ALEX:** And whenever you introduce a new tool, apply the **Problem → Tool → Proof** framework. "The problem is that queries on Parquet files don't support efficient deletes. Iceberg solves this. Look, here's the DELETE running and the data disappearing from the next query."

---

### PART 4 — Format, Visual Pacing, and Presentation

**RAFAEL:** Let's talk production. For a project like this, I strongly recommend the **"Talking Head + Screen"** format: you appear in a small corner of the screen (a laptop camera is fine to start) while 80% of the frame is your terminal, your browser, your code. This humanizes without taking focus away from what matters.

**ALEX:** Humanizing is crucial. People don't follow channels — they follow people. Even in a dense technical project, moments like "I was stuck on this for 3 hours and then found out it was a wrong environment variable" build genuine connection. Technical vulnerability is relatable.

**MARINA:** For clear communication in video format, some practical rules:
1. **One idea per scene** — if you're explaining the Airflow DAG, don't switch to the FastAPI code in the same scene without a clear transition
2. **Signal transitions verbally** — "now that the service is configured, let's see how Spark connects to it"
3. **Recap at the start** — 15 seconds of "in the previous video we did X, today we're going to do Y because Z"
4. **Preview at the start, recap at the end** — mirror what you promised with what you delivered

**RAFAEL:** On duration: deep technical videos perform well between **15 and 25 minutes** on YouTube. Below that you can't cover what you need with depth. Above 30 minutes, retention drops significantly unless you're already an established name. For the first video, aim for **20 minutes**.

**ALEX:** And break the video into **chapters** (timestamps in the description and in the video itself). Chapters increase retention because the viewer feels they can "navigate" the content without getting lost.

**RAFAEL:** Production structure I recommend for each video:

```
00:00 - 00:30  Hook (final result running / promise)
00:30 - 02:00  Context and what we're building today
02:00 - 05:00  Diagram / architecture of the episode
05:00 - 18:00  Hands-on (code, terminal, configuration)
18:00 - 20:00  Live test + result
20:00 - 21:00  Recap + next video (CTA)
```

**RAFAEL:** On equipment — no excuses not to start:
- **Audio** matters more than video. A simple USB microphone (R$150-300) makes a huge difference
- **Lighting** — a window in front of you already solves 80% of the problem
- **Resolution** — record in 1080p. 4K is unnecessary at the start
- **Editing** — DaVinci Resolve (free) works well. Cut the silences and the "uhhh"s. The pacing of the audio defines the pacing of the video

---

### PART 5 — Series Planning: The Playlist as the Product

**ALEX:** Here's the most important insight: **the playlist is the product, not the video**. Each individual video is a chapter. The viewer who watches video 1 should feel they *need* video 2. That's serial storytelling — the same mechanism that makes people binge-watch series.

**MARINA:** And this has practical communication implications. Every video should end with a technical cliffhanger: "but there's a problem — when the data volume grows, this approach chokes. In the next video we'll solve this with partitioning." You've planted a question that won't go away.

**RAFAEL:** My suggested structure for the project's full series:

#### Suggested Series: "Data Platform From Scratch"

| Episode | Title | Main Service |
|----------|--------|------------------|
| EP01 | The Marketplace — The Business Case, the Database, and the Analytical Problem | business_db + Overview + Docker |
| EP02 | Async Ingestion with FastAPI — PostgreSQL and 3-Layer Architecture | dbapi + PostgreSQL + SQLAlchemy |
| EP03 | Distributed Storage — MinIO, Delta Lake, and Unity Catalog OSS | MinIO + Delta Lake + Unity Catalog |
| EP04 | Processing at Scale with Spark — from Ingestion to Bronze | Spark + Autoloader + Airflow |
| EP05 | Kafka Enters the Stack — Event-Driven and the Outbox Pattern | Kafka + Schema Registry + Outbox |
| EP06 | CQRS in Practice — Read Path via Trino over Delta Lake | Trino + CQRS |
| EP07 | Data Quality — Chaos Engineering, dbt, and Great Expectations | dbt + Great Expectations + OpenLineage |
| EP08 | Observability — OpenTelemetry, Prometheus, and Data Lineage | OpenTelemetry + Prometheus + Marquez |
| EP09 | The Full Pipeline — from Kafka Event to Trino Dashboard | Full integration |

**ALEX:** Notice that this structure has a perfect arc: EP01 is the promise, EPs 02-09 are the journey (each service is a "boss"), and EP10 is the resolution — everything working together. It's the hero's structure applied to data engineering.

**MARINA:** And from an SEO and distribution standpoint, titles with "from scratch" and "in practice" perform very well in Portuguese-language technical content. Don't underestimate thumbnails — they determine CTR (click-through rate). Use a consistent formula: main technology logo + a short impact phrase.

---

### PART 7 — The Two Tracks: Fast Track and Deep Track

**MARINA:** I want to go back to something I said earlier — the onion layer. The real problem is that the audience you want to reach isn't homogeneous. There's the mid-level engineer who already knows FastAPI and wants to know how to integrate it with Iceberg. But there's also the backend dev who's migrating into data and doesn't know what a seeder is, doesn't really understand async, has never configured a connection pool. If you make just one video, it'll be too shallow for one and too dense for the other. You'll lose both.

**ALEX:** This is a classic split-audience problem. And the narrative solution is to create two parallel journeys with the same destination. It's what TV series do with subplots — narratives that run in parallel, cross at the right points, and arrive together at the climax. Here the "climax" is EP10 — the full pipeline running. Both audiences get there by different paths.

**MARINA:** Exactly. And I'd call them: **Fast Track** and **Deep Track**. The Fast Track is the main series we already planned — EP01 to EP10. It assumes the person has a reasonable technical base and wants to build. The Deep Track consists of complementary episodes, numbered as sub-episodes — EP02.1, EP02.2, EP02.3 — that exist for those who need the fundamentals before jumping into the main episode. They're shorter, focused, surgical episodes.

**RAFAEL:** From a production standpoint, this is elegant because it completely changes the format. The Fast Track is 15-25 minutes, terminal open, code on screen, fast pace. The Deep Track can be 8-12 minutes, more didactic, with more diagrams and less code, camera a bit more present because you're *explaining*, not *building*. They're different products in appearance, but derived from the same preparation.

**ALEX:** And there's a huge narrative gain here: you turn the barrier to entry into content. Instead of softening EP02 so you don't scare off beginners — which always dilutes the content for advanced viewers — you say: "if you don't know FastAPI yet, I recorded EP02.1 especially for you. Watch that one first and come back here." It's an invitation, not a wall. And the person who watches EP02.1 arrives at EP02 prepared and grateful.

**MARINA:** Communication between the tracks needs to be explicit in every video. At the start of the Fast Track, mention the available Deep Track. At the end of the Deep Track, show where the viewer enters the Fast Track. This creates a web — people circulate between the playlists and the total watch time on the channel explodes.

**RAFAEL:** Two distinct playlists on the channel, with clear names:
- **"Data Platform From Scratch — Fast Track"** (EP01 to EP10)
- **"Data Engineering Fundamentals"** (all sub-episodes organized by topic)

The second playlist has standalone value. Someone who's never going to build the full platform can watch just the fundamentals and still learn a lot. This widens the top of the funnel without compromising the depth of the main series.

**ALEX:** And the SEO on that second playlist is pure gold. While the Fast Track competes with other full-stack tutorials — which are few but dense — the Deep Track competes on fundamentals keywords: "what is FastAPI", "how does python async work", "what is an ORM", "database seeder". These keywords have very high volume and weak competition in Portuguese.

**MARINA:** To sum up the logic: the Fast Track is the main product. The Deep Track is the entry path — and also a standalone product for those not yet ready for the Fast Track. The two feed each other. And the viewer is never stuck: there's always a clear next step regardless of the level they arrived at.

---

#### Full Structure of the Three Tracks

| Episode | Track | Title | Duration |
|----------|--------|--------|---------|
| EP01 | Fast Track | The Marketplace — The Business Case, the Database, and the Analytical Problem | 22 min |
| Club T0 | Theory Track | Reading Kimball Ch. 1–3: What is dimensional modeling and why OLTP isn't enough | 10 min |
| EP01.1 | Deep Track | What is a Data Platform? (and why build one) | 10 min |
| EP01.2 | Deep Track | Docker for Data Engineers — the minimum you need to know | 10 min |
| EP01.3 | Deep Track | OLTP vs OLAP: why we can't analyze directly from the transactional database | 10 min |
| EP01.4 | Deep Track | Fact, Dimension, and Grain: Kimball's 3 concepts in the Marketplace context | 12 min |
| EP01.5 | Deep Track | Star Schema in practice: designing NexoMart's dimensional model | 12 min |
| Club T1 | Theory Track | Reading: Fundamentals of Data Engineering — lifecycle and Medallion | 10 min |
| EP02 | Fast Track | Async Ingestion with FastAPI — PostgreSQL and 3-Layer Architecture | 20 min |
| EP02.1 | Deep Track | What is FastAPI and why async changes everything in data APIs | 10 min |
| EP02.2 | Deep Track | PostgreSQL for Data Engineers — beyond CRUD | 10 min |
| EP02.3 | Deep Track | What is a Seeder and why every data pipeline needs one | 8 min |
| EP02.4 | Deep Track | SQLAlchemy 2.0 — ORM, Async, and the repository architecture | 12 min |
| EP02.5 | Deep Track | Exploding JSONB: turning order_items into analytical rows in Spark | 10 min |
| Club T2 | Theory Track | DDIA — Storage Engines: why Delta Lake and Parquet are dramatically faster | 10 min |
| EP03 | Fast Track | Distributed Storage — MinIO, Delta Lake, and Unity Catalog OSS | 20 min |
| EP03.1 | Deep Track | Delta Lake vs Iceberg — real trade-offs and why we chose Delta | 12 min |
| EP03.2 | Deep Track | Unity Catalog OSS — open-source data governance aligned with Databricks | 10 min |
| EP03.3 | Deep Track | SCD Type 2 in practice: customers and products that change over time | 12 min |
| EP04 | Fast Track | Processing at Scale with Spark — from Ingestion to Bronze | 22 min |
| EP04.1 | Deep Track | Driver and Executor: the division of labor in Spark | 10 min |
| EP04.2 | Deep Track | Autoloader: incremental ingestion, checkpointing, and schema evolution | 12 min |
| Club T3 | Theory Track | DDIA Ch. 11 — why the Kafka log is the immutable event store | 10 min |
| EP05 | Fast Track | Kafka Enters the Stack — Event-Driven and the Outbox Pattern | 22 min |
| EP05.1 | Deep Track | Structured Streaming and Kafka in Spark — offset management and checkpoints | 12 min |
| Club T4 | Theory Track | The Hard Parts — decomposition and CQRS in distributed systems | 10 min |
| EP06 | Fast Track | CQRS in Practice — Read Path via Trino over Delta Lake | 20 min |
| EP06.1 | Deep Track | What is Federated SQL? How Trino queries without moving data | 10 min |
| EP07 | Fast Track | Data Quality — Chaos Engineering, dbt, and Great Expectations | 20 min |
| EP07.1 | Deep Track | dbt in practice — Silver → Gold transformations over Delta Lake | 12 min |
| EP07.2 | Deep Track | What is Chaos Engineering in data? Quality, fraud, and anomalies | 12 min |
| EP08 | Fast Track | Observability — OpenTelemetry, Prometheus, and Data Lineage | 20 min |
| EP08.1 | Deep Track | OpenTelemetry in data pipelines — traces, metrics, and logs | 10 min |
| EP09 | Fast Track | The Full Pipeline — from Kafka Event to Trino Dashboard | 25 min |

---

#### Detailed Example: EP02 in Fast Track + Deep Track

**EP02 — Fast Track (20 min): "Data Ingestion with FastAPI Async"**

> Assumes: you already know what a REST API is and have used Docker before.
> Delivers: `/sales/orders` endpoint receiving data, validating with Pydantic, persisting via an async repository into PostgreSQL.

```
00:00 - 00:30  Hook: "This endpoint here is going to receive 50 thousand requests per hour without choking."
00:30 - 02:00  The role of dbapi in the stack: where it fits between the outside world and Iceberg
02:00 - 04:00  Diagram of the 3-layer architecture: Router → Service → Repository
04:00 - 08:00  Creating the SQLAlchemy model with AuditMixin
08:00 - 12:00  Pydantic schema + business validations
12:00 - 17:00  Repository with async bulk insert (no for-loop)
17:00 - 19:00  Testing with 10,000 records — live response time
19:00 - 20:00  Cliffhanger: "This data is in Postgres now. In EP03 we'll model the relationships."
```

---

**EP02.1 — Deep Track (10 min): "What is FastAPI and why async changes everything in data APIs"**

> For: devs who know Flask/Django but have never used FastAPI or never truly understood async.
> Delivers: conceptual clarity — you walk away knowing *why* async matters in high-volume data ingestion.

```
00:00 - 01:00  Problem: "Why do synchronous APIs choke when data volume grows?"
01:00 - 04:00  Analogy: synchronous waiter vs asynchronous waiter (the waiter doesn't wait for the dish to be ready)
04:00 - 07:00  FastAPI vs Flask: what changes in practice (simple live benchmark)
07:00 - 09:00  How async/await works in Python — the minimum you need to know
09:00 - 10:00  "Now that you understand this, EP02 is going to make a lot more sense — link in the description."
```

---

**EP02.2 — Deep Track (10 min): "PostgreSQL for Data Engineers — beyond CRUD"**

> For: people who used Postgres as an application database but never thought of it as a pipeline destination.
> Delivers: understanding schemas, basic partitioning, the role of asyncpg, and why we don't use SQLite here.

```
00:00 - 01:00  "Postgres isn't just an application database — here it's our transactional layer"
01:00 - 04:00  Schemas as namespaces: sales, warehouse, person, product (visual demonstration)
04:00 - 07:00  asyncpg vs psycopg2: why the driver matters under high concurrency
07:00 - 09:30  Bulk insert vs individual insert — live performance demonstration
09:30 - 10:00  "Next: what is SQLAlchemy 2.0 and why we abstract the SQL — EP02.4"
```

---

**EP02.3 — Deep Track (8 min): "What is a Seeder and why every pipeline needs one"**

> For: people coming from the web application world who have never worked with synthetic data generation.
> Delivers: understanding the role of the seeder, basic chaos engineering, and how it simulates real production.

```
00:00 - 01:00  "How do you test a pipeline without real data? With fake data that behaves like real data."
01:00 - 03:30  What a seeder does: generates, validates, and inserts data with production-like structure
03:30 - 06:00  Basic chaos engineering: why we inject errors on purpose (invalid CPFs, duplicates)
06:00 - 07:30  Argparse + integration with Airflow BashOperator (preview of EP07)
07:30 - 08:00  "In EP02 you'll see the seeder running against the API we built."
```

---

**EP02.4 — Deep Track (12 min): "SQLAlchemy 2.0 — ORM, Async, and the Repository Architecture"**

> For: people who know SQL but have never used an ORM, or used SQLAlchemy 1.x and got confused by version 2.0.
> Delivers: understanding Model → Repository as separation of concerns, and why zero for-loops in inserts.

```
00:00 - 01:00  "An ORM isn't magic — it's a translation layer between Python and SQL"
01:00 - 04:00  SQLAlchemy 1.x vs 2.0: what changed and why it matters (native async)
04:00 - 07:00  Model vs Repository: why separate data access from business logic
07:00 - 10:00  Bulk insert with insert().values(matrix) — why this scales and the for-loop doesn't
10:00 - 12:00  "Now you're ready for EP02 — the full build of the ingestion layer."
```

---

### PART 8 — Theory Track: The Book Club as the Third Track

**ALEX:** There's something missing from most Portuguese-language technical channels — the thing that separates the channels that become reference points from the ones stuck at 5 thousand subscribers. The *justification* behind decisions. Anyone can show how to install Kafka. Few can explain *why* Kafka here and not RabbitMQ, Pulsar, or a simple queue. The answer isn't in the code — it's in the books. And the Book Club solves this in a way I've never seen done in Portuguese.

**MARINA:** What makes this proposal different is the timing. The Club doesn't happen *after* the episode — as an optional add-on nobody watches. It happens *before*. The reading precedes the implementation, by design. This turns the channel from a repository of tutorials into a record of how a senior engineer builds something from scratch — reading before coding, documenting the decisions, understanding the trade-offs. This sentence sums up the positioning: **"we don't implement without understanding why."** I've never seen another Portuguese-language channel use this as a differentiator. That's the empty space you occupy.

**RAFAEL:** From a production standpoint, the Theory Track has a completely different format from the other two tracks. It's not a tutorial — it's a reading discussion. I'd call it **"Reading Together"**: 8-10 minutes where you take specific chapters from the current book and extract the 3-5 concepts that will show up in the next Fast Track. It's not a summary of the whole book — it's the bridge between theory and implementation. Camera more present, on-screen notes, conversational pace. Purple thumbnail — so the audience knows immediately when they see the thumbnail: this is Book Club, not Fast Track or Deep Track.

**ALEX:** And there's a third element beyond the video: Discord. Book Club without a community is just another video. With Discord, each reading session becomes a debate — the audience brings questions, disagreements, examples of how they applied that at their own company. That's community-generated content you can't produce alone. A monthly live stream where you apply the book's concept in code, live, closes the loop perfectly: **reading → Discord debate → live implementation → Fast Track**.

**MARINA:** The cadence that makes sense: one Club session per Fast Track episode, releasing 1 week before. With the Fast Track coming out every 2 weeks, the Club comes out on the alternating week. Someone following the channel has content every week — and always in logical sequence. In the long run, this is also the seed of monetization: partnerships with publishers to distribute copies to Discord members, a premium subscription for the monthly live streams, a bootcamp that walks the full track with the books as theoretical foundation.

#### Season 1 Books — Mapped by Architectural Decision

| Season | Book | Relevant Chapters | Underpins |
|-----------|-------|---------------------|------------|
| T0 | **The Data Warehouse Toolkit** (Kimball & Ross) | Ch. 1–4: Dimensional modeling, Star Schema, Grain, SCDs | EP01, EP02, EP07 |
| T1 | **Fundamentals of Data Engineering** (Reis & Housley) | Lifecycle, Medallion, Data Quality as a discipline | EP02, EP03, EP07 |
| T2 | **Designing Data-Intensive Applications** (Kleppmann) | Ch. 3–4: Storage Engines + Ch. 11: Stream Processing | EP03, EP04, EP05 |
| T3 | **Domain-Driven Design** (Khononov) | Aggregates, Domain Consistency, Outbox Pattern | EP05 |
| T4 | **Software Architecture: The Hard Parts** (Ford & Richards) | Decomposition, CQRS, trade-offs in distributed systems | EP06 |
| T5 | **Gang of Four — Design Patterns** (GoF) | Repository, Factory | EP02 (retroactive) |
| T6 | **Fluent Python** (Ramalho) | Generators, protocols, native async | EP02, EP04 |

**ALEX:** Notice the positioning that emerges from all this: you're not just teaching tools. You're showing *how a senior engineer thinks* — reads before implementing, documents decisions in ADRs, understands the trade-offs, admits what they still don't know. This is the channel a senior engineer recommends to a junior when they ask: *"how do you actually learn architecture?"*

#### "Reading Together" Video Format (8-10 min)

```
00:00 - 01:00  Hook: which project decision this book underpins
01:00 - 03:00  Context: why we're reading this book now (the implementation comes the following week)
03:00 - 07:00  The 3–5 key concepts that will show up in the corresponding Fast Track
07:00 - 09:00  How these concepts manifest in the project's code (preview)
09:00 - 10:00  "Next week, in EP[N], you'll see this happening on the real platform."
               CTA for Discord: "The discussion continues there — link in the description."
```

---

### PART 6 — Engagement, Credibility, and Growth

**MARINA:** A technical channel grows through **demonstrated credibility**, not algorithm luck. The best growth strategy is simple: the content has to work. If the viewer follows your steps and reaches the same result, they become a fan and share it. If it fails, they leave and don't come back. That's why you should **test everything before recording**. The video is the rehearsed performance, not the live experiment.

**RAFAEL:** Except when the mistake is intentional and pedagogical. A video called "I got the architecture wrong and had to redo it — learn from my mistakes" can be extremely powerful. It shows the real process, not the sanitized version.

**ALEX:** That's what I call **strategic vulnerability**. It's not about exposing weakness — it's about showing that you've already been where the viewer is right now. "I spent 2 days trying to understand why Spark wasn't connecting to MinIO. The problem was this field here in spark-defaults.conf." That's narrative gold.

**MARINA:** For engagement in the comments: ask open-ended questions at the end of every video. Not "leave a like," but "which service are you having the most trouble understanding? Tell me in the comments." Replies generate more replies, and YouTube reads that as a quality signal.

**ALEX:** And one last thing about channel narrative: **document the journey in real time**. If you're still building parts of the project, film the process. "Today I went to implement date partitioning in Iceberg and discovered that..." — this is more authentic and connects more than polished tutorial videos where everything always works on the first try.

**MARINA:** Summing up the channel's philosophy in one sentence: **"Show the real work, explain the reasoning behind the decisions, and let the data flow at the end to prove it works."**

---

## BLOCK II — ECOSYSTEM: The Role of Each Platform

### YouTube — The Engine (Pillar Content)
- **Function**: Technical depth + proof of competence
- **Format**: 15-25 min per video, structured series
- **Audience**: Data engineers, backend devs, students
- **Metric that matters**: Watch Time + Subscribers
- **Ideal frequency**: 1 video every 2 weeks (consistency > quantity)

### LinkedIn Newsletter — The Converter (Warm Audience)
- **Function**: Turn passive connections into active readers + nurture the existing 500 subs
- **Format**: 800-1,200 words, career/technical-decision angle (not a tutorial)
- **Audience**: The same people who follow you on LinkedIn — recruiters, managers, technical peers
- **Metric that matters**: Open Rate + New subscribers per edition
- **Ideal frequency**: 1 edition every two weeks (synced with YouTube)

### LinkedIn Posts — The Amplifier (Organic Reach)
- **Function**: Attract new followers via the feed + drive to newsletter and YouTube
- **Format**: 3-5 posts per week, each extracted from that week's video
- **Audience**: LinkedIn's open feed — potential viral reach
- **Metric that matters**: Impressions + New followers
- **Ideal frequency**: 3-5x per week

### Medium — The Archive (Long-Term SEO)
- **Function**: Organic traffic via Google + permanent written credential
- **Format**: In-depth technical article, 1,500-3,000 words, with code
- **Audience**: People searching "apache iceberg tutorial", "airflow spark integration" on Google
- **Metric that matters**: Organic views + Follows
- **Ideal frequency**: 1 article per published video

### Instagram — The Humanizer (Cold → Warm Audience)
- **Function**: Top of funnel — introduce yourself to people who don't know you yet
- **Format**: Technical carousels (diagrams, "5 things about X") + behind-the-scenes Stories
- **Audience**: Devs and tech students who use Instagram
- **Metric that matters**: Reach + Saves (saves = useful content)
- **Ideal frequency**: 3x per week (2 carousels + 1 short reel or stories)

### Discord — The Community (Theory Track Hub)
- **Function**: Debate around Club readings + practice community for the most engaged members
- **Format**: Channels by book/season, Q&A channel per episode, monthly voice call for the Club live stream
- **Audience**: The most engaged 10-20% of the channel — those who want more than the video
- **Metric that matters**: Active members + threads created per Club session
- **Frequency**: Always active — each Club T[N] generates a debate thread lasting 1 week
- **Future monetization**: Premium access to monthly live streams, private ADR sessions, live bootcamp

---

## BLOCK III — THE FLOW: How Everything Connects

### Typical week with 1 video published:

```
DAY 1 (Monday)    — Record/edit the YouTube video
DAY 2 (Tuesday)   — Publish YouTube + LinkedIn Post #1 (main insight from the video)
DAY 3 (Wednesday) — Instagram Carousel (architecture diagram from the video)
DAY 4 (Thursday)  — LinkedIn Post #2 (mistake/lesson from the video) + Medium article published
DAY 5 (Friday)    — LinkedIn Newsletter (editorial version of the topic) + LinkedIn Post #3 (CTA to newsletter)
DAY 7 (Sunday)    — Behind-the-scenes Stories for the next video (Instagram)
```

### How each post is extracted from the same video:

**Example: EP06 — Apache Spark**

| Platform | Angle | Title/Caption |
|-----------|--------|----------------|
| YouTube | Full technical tutorial | "Apache Spark in Docker: from zero to a running job in 20 min" |
| LinkedIn Post #1 | Decision insight | "Why I chose Spark over DuckDB for this project (and when I'd get that choice wrong)" |
| LinkedIn Post #2 | Technical vulnerability | "I was stuck on this Spark config for 3 hours. The problem was one line in spark-defaults.conf" |
| LinkedIn Post #3 | Editorial CTA | "I wrote about this in the newsletter this week. Link in the comments." |
| Medium | Technical article with code | "Spark + MinIO + Iceberg: full configuration with real examples" |
| Newsletter | Career angle | "What to know about Spark before your next data interview (and what nobody tells you)" |
| Instagram | Visual | Carousel: "5 Spark concepts you need to understand before using it in prod" |

---

## BLOCK IV — PLATFORMS: Channel-Specific Guide

### LinkedIn Newsletter — Reactivating the 500 Subs

This is your biggest immediate advantage. 500 active subscribers are worth more than 10,000 cold followers.

**3-step reactivation strategy:**

**Step 1 — Comeback Edition (Week 1)**
Don't pretend you never stopped. Be direct:

> *"This newsletter has been on hold for [X] months. I'm going to explain why I stopped, what I was building during that time, and what changes now."*

Show the project — the data platform — as the reason. You were building the content, not just publishing about it. This reframes the silence.

**Step 2 — Onboarding Series (Weeks 2-4)**
Publish 3 introductory editions before entering the normal cadence:
- Edition 2: "The stack I'm documenting and why I chose each tool"
- Edition 3: "The biggest mistake I made setting up this architecture"
- Edition 4: "What you'll learn by following this series"

**Step 3 — Sustainable Biweekly Cadence**
One edition every two weeks, always synced with the YouTube video.

**LinkedIn Newsletter Format (template):**

```
SUBJECT LINE: [Number] + [specific benefit] + [emotion]
Examples:
- "3 architecture decisions I'd redo (and what I learned)"
- "Why Airflow almost made me quit — and how I solved it"
- "The query that should've taken 3h ran in 4 seconds. Here's how."

INTERNAL STRUCTURE:
• Opening (2-3 lines): hook + what's in this edition
• Context (1 paragraph): why this topic matters now
• Main content (3-4 paragraphs): the real lesson
• Transferable takeaway (1 paragraph): what this changes in practice
• CTA (2 lines): link to the video + invitation to reply
```

---

### LinkedIn Posts — The Reach Factory

The LinkedIn feed has a different mechanism than any other network: **long-form text without a link performs better** than posts with an external link. The algorithm penalizes anyone who "makes people leave LinkedIn."

**Main tactic: publish without a link → reply with the link in the comments**

```
Main post: Full insight, no link
First comment (yours): "I recorded a video going into detail on this: [link]"
```

**5 post types that work:**

**Type 1 — The Counterintuitive Take**
*"Everyone says to use [technology X]. I disagree. Here's why."*

**Type 2 — The Number With a Story**
*"50 million records inserted in 12 minutes. That would've been impossible 6 months ago with the architecture I had."*

**Type 3 — The Expensive Lesson**
*"The mistake that cost me 2 days of debugging. Don't make this one."*

**Type 4 — The Concept Carousel**
*"5 open-source tools that replace paid AWS services [carousel]"*

**Type 5 — The Behind-the-Scenes**
*"Recording EP07 of the Airflow series. Here's the diagram I used to organize my thinking before coding."*

---

### Medium — Long-Term SEO

Medium is the only channel where content has increasing value over time — an article published today can generate traffic for years via Google.

**Critical tip:** Publish on **Medium first**, wait 3 days for Google to index it, then share it on LinkedIn. If you share before it's indexed, LinkedIn will "steal" the audience and Google will treat it as duplicate content.

**Keyword strategy by episode:**

| Article | Main Keyword |
|--------|------------------|
| EP01 overview | "open source data platform from scratch" |
| EP02 FastAPI | "fastapi postgresql async sqlalchemy 2.0" |
| EP03 Delta Lake | "delta lake tutorial docker unity catalog" |
| EP07 Airflow | "apache airflow docker compose tutorial" |
| EP08 Trino | "trino delta lake query tutorial" |
| EP09 Seeders | "chaos engineering data quality python" |

**Medium article structure:**

```markdown
# Title with keyword + benefit

## TL;DR (3 bullets — for scanners)

## The Problem
(Why does this topic exist?)

## The Solution
(Code + explanation alternating — never a code block without context)

## Common Pitfalls
(What goes wrong — the most-read section)

## Conclusion
(What you'd take away from this article in 3 lines)

## Next steps
(Link to YouTube + next article)
```

---

### Instagram — Technical Content That Works Visually

The secret is: **don't try to be what Instagram isn't**. You're not going to teach Spark in 30 seconds. Use Instagram to **spark curiosity**, not to teach.

**Architecture Carousel** (best format)
- Slide 1: Visual diagram of the full stack (you already have `diagram.py`)
- Slides 2-6: Each service isolated with 1 sentence explaining its function
- Final slide: "Full tutorial on YouTube — link in bio"
- Saves > Likes in this format — and saves is what the algorithm values

**Terminal Reels** (second best)
- 30-60 seconds of a pipeline running with lo-fi music
- Caption: "50 million records. 12 minutes. Zero cloud." + CTA
- No narration needed — the terminal speaking for itself is already content

**Behind-the-Scenes Stories**
- Computer setup, second monitor with terminal, microphone configuration
- Real-time mistakes ("stuck on this config for 2 hours")
- Polls: "Do you use Airflow or Prefect?" — easy engagement

**What NOT to do:**
- Tutorials as static image posts (nobody reads them)
- Posts with too much text on the image
- Inconsistent frequency (2x/week always beats 7x/week for 2 weeks and then disappearing)

---

## BLOCK V — THE FLYWHEEL: How Everything Self-Feeds

```
YOUTUBE FAST TRACK (EP01–EP09)
 → generates technical proof and pillar content with ADRs as the opener
 → feeds MEDIUM (SEO) and NEWSLETTER (warm audience)
 → references DEEP TRACK for viewers without the base knowledge
 → each episode has a corresponding Club session that aired 1 week before

YOUTUBE THEORY TRACK / BOOK CLUB (Club T1, T2, T3, T4)
 → happens 1 week before the corresponding Fast Track
 → underpins architectural decisions with reference technical books
 → generates debate on DISCORD — the channel's practice community
 → the most engaged audience migrates to the channel's core (Discord)
 → creates a unique differentiator: "the channel that reads before implementing"

YOUTUBE DEEP TRACK (EP02.1, 02.2...)
 → captures fundamentals searches (high volume, low competition in PT-BR)
 → converts beginners into FAST TRACK followers
 → generates MEDIUM articles with top-of-funnel keywords

DISCORD (Book Club Hub)
 → debate on readings after each Theory Track
 → monthly application live stream with audience participation
 → seed of the paid community (premium, bootcamp)
 → source of real questions for future Deep Track episodes

LINKEDIN POSTS
 → organic reach in the feed
 → ADRs published as technical posts (high engagement — opinionated and inviting)
 → new followers discover the NEWSLETTER
 → newsletter converts into YOUTUBE subscribers (all three tracks)

INSTAGRAM
 → cold audience discovers you
 → moves to LinkedIn or YouTube (Deep Track is a more accessible entry point)
 → enters the ecosystem

MEDIUM
 → Google brings constant organic traffic (Fast Track = advanced keywords, Deep Track = volume keywords)
 → reader finds the link to YOUTUBE
 → subscribes to the channel
```

**In 6 months**: The Fast Track grows via LinkedIn (technical network) + Medium (advanced SEO). The Deep Track grows via Google (fundamentals keywords) + Instagram (top of funnel). The Theory Track sets the channel apart — it attracts people who want more than tutorials and builds the audience's most loyal core. Discord turns viewers into a community. The three playlists feed each other: the beginner enters through the Deep Track, becomes a fan through the Fast Track, becomes a member through the Theory Track.

---

## BLOCK VI — EXECUTION: First 30 Days Plan

### Week 1 — Foundation
- [ ] Write the channel's positioning statement
- [ ] Reactivate the newsletter with the "Comeback Edition"
- [ ] Publish 3 warm-up LinkedIn posts (no link yet)
- [ ] Record EP01 of the YouTube series (stack overview)

### Week 2 — Launch
- [ ] Publish EP01 on YouTube
- [ ] Publish the EP01 article on Medium (3 days before promoting it on LinkedIn)
- [ ] LinkedIn Post with the main insight from EP01 (no link in the post)
- [ ] Instagram: carousel with the stack diagram

### Week 3 — Cadence
- [ ] Newsletter Edition 2 (with link to EP01)
- [ ] 2 additional LinkedIn posts about the EP01 topic
- [ ] Start recording EP02
- [ ] Behind-the-scenes Stories on Instagram

### Week 4 — Validation
- [ ] Analyze metrics: which LinkedIn post had the most reach? Which type?
- [ ] What was the newsletter's open rate?
- [ ] How many views did EP01 get?
- [ ] Adjust what didn't work before settling into month 2's rhythm

---

---

## BLOCK VIII — The Business Case: The NexoMart Marketplace

### The Narrative Context

**ALEX:** The most important decision for the channel is: we're not going to teach technologies in a vacuum. We're going to tell the story of a company. The company is called **NexoMart** — a Brazilian marketplace that's just getting started. They have their operational database designed. They need a data platform to analyze the business. It's exactly the problem any data engineer will face in real life. The difference is we're going to build everything from scratch, live, and document every decision.

**MARINA:** From a communication standpoint, this changes how the episodes work. The viewer isn't watching a tutorial about FastAPI — they're following the construction of a real company's analytical backend. Every tool has a business justification. Kafka comes in because the marketplace needs to process order events in real time without choking the API. Iceberg comes in because we need to version the product catalog for audit purposes. The audience understands the *why* before the *how*.

**RAFAEL:** And visually, that's gold. Instead of opening a blank terminal in EP01, you open the database diagram. You show the 7 schemas. You explain who the Seller is, who the Customer is, what's in each table. `business_db.png` is already the episode's opening slide.

---

### NexoMart's Operational Database (OLTP)

Reference files: `business_db.sql` + `business_db.png`

The schema defines the transactional database across **7 isolated domains** — each schema is a bounded context (a direct connection to DDD, Club T3).

#### The 7 Schemas and What They Represent

| Schema | Domain | Key Tables | Notable Architectural Decision |
|--------|---------|--------------|----------------------------------|
| `product` | Catalog | product, productcategory, productmodel, **productembedding** | JSONB for attributes that vary by category. **pgvector(1536)** for semantic search — the database is already AI-ready. |
| `person` | Identity | customer, **seller**, customeremail, countryregion | Seller as a first-class entity confirms the marketplace model. Banking data in JSONB (a PII and LGPD challenge). |
| `sales` | Transactions | salesorder, **ordereventlog**, returnorder, saleschannel | `order_items` in multi-seller JSONB. `ordereventlog` as lightweight event sourcing (every status change logged). |
| `marketing` | Acquisition | channel, campaign, adgroup, advertise, **dailyperformance** | 4-level hierarchy (Channel → Campaign → AdGroup → Ad). `dailyperformance` already looks like a fact table. |
| `humanresources` | People | employee, department, jobrole | **Native SCD Type 2** — `effective_start_date`, `effective_end_date`, `is_current`. Self-reference in `department` for org hierarchy. |
| `purchasing` | Supply Chain | vendor, purchaseorderheader, purchaseorderdetail | Header + Detail with `employee_id` on the header — the Buyer comes from HR. `audit_trail` in JSONB with approval history. |
| `warehouse` | Logistics | warehouselocation, productinventory, transactionhistory | `transactionhistory` with positive qty (inbound) / negative qty (outbound). `capabilities` JSONB with IoT and cold-chain data. |

#### 5 Design Decisions That Become Episodes

**1. `order_items` as JSONB (implicit ADR)**
Why not create a `sales.salesorderdetail`? In a multi-seller marketplace, each item has its own `delivery_info`, `taxes`, and `seller_id`. Most header-level queries never need item-level granularity. The cost: for analysis, we need `jsonb_array_elements` in SQL or `explode()` in Spark. → *Deep Track EP02.5*

**2. `productembedding` with pgvector**
The database plans for semantic search from the design stage. `description_vector(1536)` + `image_vector(512)` generated via ETL. → *Future episode: ML + DE + pgvector*

**3. SCD Type 2 on `employee`**
The transactional database already implements Slowly Changing Dimensions with `effective_start_date / effective_end_date / is_current`. When the engineer models the DW, they'll find an unexpected ally. → *Deep Track EP03.3 + Kimball Club T0*

**4. `ordereventlog` as lightweight event sourcing**
Every state transition of an order is logged: `PAYMENT_APPROVED`, `ITEM_PICKED`, `DELIVERY_ATTEMPT_FAILED`. → *Connects to DDIA Ch. 11 (Club T3) and Kafka EP05*

**5. Schemas as bounded contexts**
`product.product` has no address table — variable attributes live in JSONB. Customer address lives in `demographics`. Seller address lives in `demographics`. Warehouse geo lives in `geo_location`. → *Connects to DDD (Club T3)*

---

### The Analytical Problem — Why OLTP Isn't Enough

**MARINA:** The hook for EP01 isn't "let's learn FastAPI." It's: *"this database stores everything NexoMart does. But when the CEO asks which category has the highest margin, which seller has the lowest chargeback rate, or which campaign generated the most revenue — this database doesn't respond with performance. Why? And what do we do about it?"*

**ALEX:** Show the schema working, insert data through the API, try to run the analytical query — and show the explain plan blowing up. Or better: show that the query *can't even be formulated* correctly because `order_items` is stuck in JSONB. *"The answer to this is what we're going to build over the next 9 episodes."* That's the EP01 cliffhanger.

| Business Question | OLTP Problem | Data Platform Solution |
|--------------------|-----------------|------------------------|
| Which product sold the most this week by category? | `order_items` in JSONB needs unnesting | `fact_sales_order_item` (grain: order line) |
| Which seller has the highest net margin? | Join 4 tables + parse JSONB | `dim_seller` + `fact_sales` with pre-calculated metrics |
| How did the Google Ads campaign convert vs. Meta? | `dailyperformance` with no join to customer journey | `fact_marketing_performance` + attribution |
| On average, how many days does an order stay in each status? | `ordereventlog` needs heavy window functions | `fact_order_lifecycle` with pre-calculated SLA |
| How many days will warehouse A's stock last? | `transactionhistory` with no historical aggregation | `fact_inventory_snapshot` (Daily Snapshot) |
| Which buyer approved the most POs with problematic vendors? | Join `purchasing` + `humanresources` + `vendor` | `fact_purchase_order_line` + `dim_vendor` with `credit_rating` |

---

### The Dimensional Model — NexoMart's Bus Matrix

The OLTP → Dimensional transformation is the heart of the series. Kimball (Club T0) underpins the decisions before we build.

**RAFAEL:** The Bus Matrix is the artifact that opens EP02. You put it on screen — a simple table — and say: *"this is the navigation map for our platform. Every row is a fact. Every column is a dimension."* The audience understands the full architecture before seeing a single line of code.

| Fact | date | customer | product | seller | channel | warehouse | employee | vendor | special_offer |
|------|:----:|:--------:|:-------:|:------:|:-------:|:---------:|:--------:|:------:|:-------------:|
| fact_sales_order_item | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| fact_inventory_snapshot | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| fact_marketing_performance | ✓ | — | — | — | ✓ | — | — | — | — |
| fact_purchase_order_line | ✓ | — | ✓ | — | — | — | ✓ | ✓ | — |
| fact_order_lifecycle | ✓ | ✓ | — | ✓ | ✓ | — | — | — | — |

#### Grain of Each Fact

| Fact | Grain (1 row = 1 ...) | OLTP Source | SCD in Dims |
|------|------------------------|------------|--------------|
| `fact_sales_order_item` | Item within an order (unnested from JSONB) | `sales.salesorder.order_items` | dim_customer SCD2, dim_product SCD2 |
| `fact_inventory_snapshot` | Product × Warehouse × Day | `warehouse.productinventory` | dim_product SCD2 |
| `fact_marketing_performance` | Ad × Day | `marketing.dailyperformance` | — |
| `fact_purchase_order_line` | Purchase order line | `purchasing.purchaseorderdetail` | dim_employee SCD2 (already in the OLTP!) |
| `fact_order_lifecycle` | Order × Status event | `sales.ordereventlog` | dim_customer SCD2 |

---

### Extra Episodes Generated by the NexoMart Case

| Episode | Track | Title |
|----------|--------|--------|
| Club T0 | Theory Track | Kimball Ch. 1–3: What is dimensional modeling and why OLTP isn't enough |
| EP01.3 | Deep Track | OLTP vs OLAP: why we can't analyze directly from the transactional database |
| EP01.4 | Deep Track | Fact, Dimension, and Grain: Kimball's 3 concepts in the NexoMart context |
| EP01.5 | Deep Track | Star Schema in practice: designing NexoMart's DW — Bus Matrix and grain decisions |
| EP02.5 | Deep Track | Exploding JSONB: turning `order_items` into analytical rows in Spark |
| EP03.3 | Deep Track | SCD Type 2 in practice: customers and products that change over time |
| EP07.3 | Deep Track | pgvector in the DW: semantic product search integrated into the pipeline |

---

## BLOCK VII — ADRs as Content: Every Decision Is an Episode

**ALEX:** One of the most powerful ideas to emerge from combining the three tracks is using ADRs — Architecture Decision Records — as content. An ADR is a document that records *why* an architectural decision was made, what alternatives were considered, and what trade-offs were accepted. This is what top-tier data companies do internally. The differentiator is doing it publicly, live, with the audience following along.

**RAFAEL:** In practice, every Fast Track episode opens with the ADR on screen — literally a formatted Markdown or Notion doc appearing for 30-60 seconds: *"Here's the decision we made. Here are the alternatives we rejected. Here's why."* This turns the video from a tutorial into documented architectural reasoning. It's not a tutorial about Kafka — it's a record of *why we chose Kafka* and what DDIA Ch. 11 says about it.

**MARINA:** And this has an immediate effect on positioning: you stop competing with installation tutorials and start competing with applied software engineering. "How to install Kafka" has thousands of videos. "Why Kafka and not RabbitMQ in this specific architecture, grounded in Kleppmann" — has practically zero in Portuguese. That's the empty space.

#### Season 1 ADRs

| ADR | Decision | Book It's Grounded In | Status |
|-----|---------|---------------------|--------|
| ADR-001 | Async SQLAlchemy with bulk insert — no for-loops | Fluent Python — efficient generators and iterators | Accepted |
| ADR-002 | Delta Lake + Unity Catalog OSS for governance and versioning | DDIA Ch. 3–4 + Fundamentals of DE | Accepted |
| ADR-003 | Chaos Engineering as a data quality strategy | Fundamentals of DE — reliability as a discipline | Accepted |
| ADR-004 | Kafka as a buffer between the API and storage (event-driven) | DDIA Ch. 11 — Stream Processing and the immutable log | Proposed |
| ADR-005 | Outbox Pattern for exactly-once semantics with Kafka | DDD (Khononov) — domain consistency and transactions | Proposed |
| ADR-006 | CQRS with Trino to separate read/write paths | The Hard Parts — decomposition and architectural trade-offs | Proposed |
| ADR-007 | dbt as the Silver → Gold transformation layer | Fundamentals of DE — transformation and quality as practice | Proposed |
| ADR-008 | OpenLineage + Marquez for lineage traceability | Fundamentals of DE — data observability | Proposed |

**ALEX:** The ADRs with "Proposed" status are content in themselves. The audience follows the decisions being made in real time — not polished edits of something that already worked. *"We're debating whether to use Kafka or not. Here's the argument for. Here's the argument against. In Club T3 we read DDIA Ch. 11 together. After the Club, we make the decision and implement it in EP05."* That's genuine engagement, not artificially manufactured.

**MARINA:** From a LinkedIn standpoint, every ADR is a post. "We made an architectural decision today. The argument that sparked the debate: [excerpt from DDIA]. Details in the video." This format performs extremely well — it's opinionated, technical, and invites discussion without needing clickbait. And the "Proposed" ADRs become two posts: the debate before, and the decision after.

---

## Success Metrics (90 days)

| Platform | Metric | Realistic Target |
|-----------|---------|--------------|
| YouTube | Subscribers | +100 organic |
| YouTube | Watch Time | >40% average retention |
| Newsletter | Open Rate | >30% |
| Newsletter | New subs | +50 per month |
| LinkedIn | Impressions/post | >2,000 average |
| Medium | Views | >500 per article |
| Instagram | Saves per carousel | >50 |

---

## CHECKLIST BEFORE RECORDING EACH VIDEO

**Fast Track (main series):**
```
[ ] Is the video's promise clear in the first 30 seconds?
[ ] Is the problem this video solves made explicit?
[ ] Is there an analogy or metaphor for every new concept?
[ ] Is the episode's diagram/architecture ready?
[ ] Is the environment working locally (tested)?
[ ] Are the chapters/timestamps planned?
[ ] Is the cliffhanger for the next episode defined?
[ ] Do the title and thumbnail clearly communicate the value?
[ ] Are the Deep Track sub-episodes linked in the description?
```

**Deep Track (fundamentals):**
```
[ ] Is the central concept defined in one sentence before recording?
[ ] Is there at least one real-world (non-technical) analogy?
[ ] Does the video end with a clear bridge to the corresponding Fast Track?
[ ] Is the duration within 8-12 minutes (surgical focus)?
[ ] Does the title contain the main search keyword ("what is X", "how does Y work")?
```

---

## SCRIPT TEMPLATES

### Fast Track — Base Script (15-25 min)

```markdown
# EP[N] — [Title]

## HOOK (00:00 - 00:30)
[Show the final result running. One impactful sentence.]
[Mention: "If you don't know [concept X], watch EP[N.1] first — link in the description."]

## CONTEXT (00:30 - 02:00)
- Recap of the previous episode (if any)
- What we're building today and why

## DIAGRAM (02:00 - 05:00)
- Where this service fits in the overall architecture
- This episode's data flow

## HANDS-ON (05:00 - 18:00)
- Step 1: [description]
- Step 2: [description]
- Step N: [description]
- [Document points of attention and common pitfalls]

## TEST AND RESULT (18:00 - 20:00)
- Run the episode's full pipeline
- Show the data at the final destination

## RECAP AND CTA (20:00 - 21:00)
- What we built today
- The problem that remains open (cliffhanger)
- Links to related Deep Track episodes
- "Next episode..." + link to the Fast Track playlist
```

### Deep Track — Base Script (8-12 min)

```markdown
# EP[N.X] — [What is / How does it work / Why use — Concept]

## HOOK (00:00 - 01:00)
[Concrete problem: "Have you ever tried X and not understood why it choked?"]
[Don't show code yet — show the confusion, not the solution]

## ANALOGY (01:00 - 03:00)
- Explain the concept with a real-world (non-technical) analogy
- A mental image that will stick

## CONCEPT (03:00 - 07:00)
- Technical explanation now, anchored in the analogy
- Simple diagram (Excalidraw or whiteboard)
- A minimal demo if applicable (< 20 lines of code)

## WHEN TO USE / WHEN NOT TO USE (07:00 - 09:30)
- Real use cases
- What this concept does NOT solve (avoid setting the wrong expectation)

## BRIDGE TO THE FAST TRACK (09:30 - 10:00)
- "Now that you understand [concept], you're ready for EP[N]."
- "There you'll see this working in a real data pipeline."
- Direct link to the corresponding Fast Track
```

---

## Recommended Tools (all free or freemium)

| Function | Tool |
|--------|-----------|
| Carousel design | Canva (ready-made templates for devs) |
| Architecture diagram | Excalidraw or `diagram.py` itself |
| Video editing | DaVinci Resolve (free) |
| Content calendar | Notion or a simple spreadsheet |
| LinkedIn scheduling | Native LinkedIn (works well) |
| Instagram scheduling | Meta Business Suite (free) |
| Medium keyword analysis | Google Search Console + Ubersuggest |

---

*Generated on: 2026-04-23 | Updated on: 2026-04-25*
*Base: Data Platform with FastAPI, Kafka, Airflow, Spark, Delta Lake, Unity Catalog OSS, MinIO, Trino, dbt, OpenTelemetry*
*Branch added: Theory Track (Book Club) + Discord + ADRs as content*
