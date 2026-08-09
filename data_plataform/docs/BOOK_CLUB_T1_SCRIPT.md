# Book Club — T1: "The Customer Is the Hardest Dimension"
## Reading Kimball: Chapters 8 and 16 | Theory Track | 8–10 min

> **When it airs**: 1 week before the Dimensional Modeling episode of Cycle 2
> **Book**: The Data Warehouse Toolkit, 3rd ed. — Ralph Kimball & Margy Ross
> **Chapters**: 8 (CRM Analytics) + 16 (Insurance)
> **Audience prerequisite**: having watched Book Club T0 (Chapters 1–3) or knowing the concepts of Star Schema, fact, and dimension

---

## Episode Goal

Show that the `customer`/`person` dimension is the most strategic **and** the most problematic in any DW — and that Kimball solves this with three design decisions that will show up directly in NexoMart.

---

## Script Structure

```
00:00 - 00:35  Hook — the problem everyone has already lived through
00:35 - 01:15  Context: where we are in the book
01:15 - 03:30  Block 1 — SCD-2 on the customer dimension + the counting trap
03:30 - 05:30  Block 2 — Bridge Table: when one row isn't enough
05:30 - 07:30  Block 3 — Date chain + mini-dimension (Chapter 16)
07:30 - 09:00  Direct connection to NexoMart
09:00 - 10:00  Question for Discord + CTA
```

---

## Full Script (what to say)

### 00:00 — HOOK

> *"You open the active customers report. The number shows up: 1.2 million. You go to the database and count with a SELECT COUNT(*): 2.4 million. The meeting with the business turns into chaos — which one is correct? Both. And both are wrong at the same time. That's the problem Kimball solves in chapter 8."*

**[Show on screen]**: episode title + Kimball Toolkit cover

---

### 00:35 — CONTEXT: Where We Are in the Book

> *"In Book Club T0 we read the first chapters — grain, fact, dimension, Star Schema, the basics that everything else is built on. Today we jump to chapters 8 and 16, which cover two different industries, CRM and insurance, but teach the same three techniques. I picked these two chapters together because they form the survival kit for anyone modeling a customer dimension in the real world."*

---

### 01:15 — BLOCK 1: SCD-2 on the Customer Dimension + The Counting Trap

> *"Kimball opens chapter 8 with a direct statement: the customer dimension is critical and needs to be conformed. Conformed means it has the same structure across every data mart in the company — same data type, same column, same granularity. If marketing uses one version and sales uses another, you don't have a DW, you have two digital silos.*
>
> *And that's where the real problem starts. When you use SCD Type 2 — which keeps the history of customer changes — every address change, every name change, every status change becomes a new row in the table. A customer who moved three times has four rows. Now: what's the total customer count?*
>
> *Kimball gives three ways to answer that without lying:*"

**[Show on screen: slide or whiteboard with the three methods]**

> *"One: COUNT DISTINCT on the NK — the natural key, the ID from the source system. Not the DW surrogate key, the natural key that never changes.*
>
> *Two: filter by the `is_current` or `is_active` flag — only count the rows that represent the current state.*
>
> *Three: filter by date range — `WHERE valid_to IS NULL` or `valid_to > today`. Any of the three works. The mistake is running a COUNT(*) without thinking.*
>
> *Another detail Kimball mentions: if customer data comes from an external source, with a different refresh rate than your internal systems, create a separate dimension and relate it to the parent dimension. Don't try to force everything into the same table. The operational gain is worth the slightly more complex model."*

---

### 03:30 — BLOCK 2: Bridge Table — When One Row Isn't Enough

> *"The second technique is the Bridge Table, and it shows up in two different moments across chapters 8 and 16.*
>
> *The scenario: you have a dimension that in theory has one value per customer — segment, credit category, policy type. But in practice, over time, customers show up who belong to multiple groups at the same time. The classic relational model can't handle that without duplicating data.*
>
> *Kimball's solution is to create a bridge table between the fact table and the dimension. That bridge table isn't an ordinary dimension — it's a resolution of an N:M relationship. The fact points to the bridge, the bridge points to the dimension.*
>
> *In chapter 16, he shows an even more specific variation: a dimension that aggregates other dimensions — something like 'food sector' that groups restaurant, bar, supermarket. Same logic: bridge as the entry point, base dimension with the detail.*
>
> *The practical rule I took away from this: if you're putting an array or a list into a column of the relational database, that's a sign you need a bridge table in the dimensional model."*

**[Show on screen: simple diagram — Fact → Bridge → Dim]**

---

### 05:30 — BLOCK 3: Date Chain and Mini-Dimension (Chapter 16)

> *"Chapter 16 talks about insurers, but it has two insights that apply to any sales model.*
>
> *First: instead of a single date per order, Kimball suggests modeling a chain of dates. Order creation date. Shipping date. Delivery date. Process close date. Each of those dates is a key into the date dimension. With them, you can answer questions like: what's the average time between the order and the delivery? On which days of the week do orders run late most often? What's the gap between shipping and closing?*
>
> *Second insight: the mini-dimension. When an attribute of the customer dimension changes very fast — purchase frequency, risk classification, engagement level — if you handle that with regular SCD-2, the table will explode. A customer whose engagement level changes every week turns into 52 rows a year just from level history.*
>
> *The solution is to create a separate, smaller dimension just for those volatile attributes — the mini-dimension — and relate it directly to the fact table, not to the main customer dimension. You isolate the volatility without contaminating the central dimension."*

**[Show on screen: diagram — Fact with 4 date FKs + mini-dim FK]**

---

### 07:30 — CONNECTION TO NEXOMART

> *"Now what does this have to do with NexoMart? Everything.*
>
> *First: the `person.customer` table in our OLTP already has `is_active` and `row_guid` — the AuditMixin. When we turn that into a dimension in the DW, the correct customer count is going to use exactly `row_guid` as the natural key and `is_active` as the current-state flag. Exactly what Kimball prescribed in chapter 8.*
>
> *Second: the `sales.sales_order` table has `order_date`, but it could have — and will have, in the dimensional model — four dates: order date, payment date, shipping date, and delivery date. Each one as an FK to `dim_date`. The chapter 16 date chain lands directly in our real model.*
>
> *Third: in the `marketing` domain, campaigns are going to be multi-channel — one ad can be on Google, Facebook, and TikTok at the same time. That's a multivalued attribute. The chapter 8 bridge table is exactly what's going to solve that case in NexoMart's dimensional model.*
>
> *Three techniques from the book, three direct use cases in the project we're building together."*

---

### 09:00 — QUESTION FOR DISCORD + CTA

> *"Before wrapping up, this week's question for the Discord server — channel #book-club:*
>
> *Have you ever modeled or seen a case where the customer dimension was out of conformance between departments? Marketing with one version, sales with another? What happened? What did it cost?*
>
> *In the next channel episode, we're going to take all of this and build it live. Customer dimension, SCD-2, bridge table, date chain — in code, in the database, with real NexoMart data. If you haven't watched Book Club T0 yet, link in the description. And if you want to join the discussion, the Discord link is there too.*
>
> *See you next time."*

---

## Required Visual Assets

| Scene | What to show | How to produce it |
|---|---|---|
| Hook | Two conflicting numbers (1.2M vs 2.4M) | Simple slide — white background, large text |
| Block 1 | The 3 correct COUNT methods | List slide with visual highlight |
| Block 1 | SQL code for the 3 methods | Terminal or VSCode with syntax highlighting |
| Block 2 | Fact → Bridge → Dim diagram | dbdiagram.io or hand-drawn diagram |
| Block 3 | Fact with 4 date FKs + mini-dim FK | Same diagram, extended |
| NexoMart connection | `person.customer` schema in code | VSCode or DBeaver with the real model |

---

## Supporting SQL Code (show on screen)

```sql
-- The 3 correct counting methods with SCD-2

-- Method 1: COUNT DISTINCT on the natural key
SELECT COUNT(DISTINCT customer_nk) AS total_customers
FROM dim_customer;

-- Method 2: is_current flag
SELECT COUNT(*) AS total_customers
FROM dim_customer
WHERE is_current = TRUE;

-- Method 3: date range
SELECT COUNT(*) AS total_customers
FROM dim_customer
WHERE valid_to IS NULL;
```

```sql
-- Date chain in an orders fact table (Chapter 16)
CREATE TABLE fact_sales_order (
    order_sk         BIGINT,
    order_date_sk    INT REFERENCES dim_date(date_sk),   -- order date
    payment_date_sk  INT REFERENCES dim_date(date_sk),   -- payment date
    ship_date_sk     INT REFERENCES dim_date(date_sk),   -- shipping date
    delivery_date_sk INT REFERENCES dim_date(date_sk),   -- delivery date
    customer_sk      BIGINT,
    product_sk       BIGINT,
    revenue          NUMERIC(19,4),
    quantity         INT
);
```

---

## Production Checklist

- [ ] Record intro with camera + verbal hook
- [ ] Prepare slides for the 3 COUNT methods (Canva or similar)
- [ ] Draw the Bridge Table diagram (can be hand-drawn + overhead camera)
- [ ] Draw the date chain diagram (same process)
- [ ] Open VSCode with `person/customer_model.py` for the NexoMart section
- [ ] Prepare both SQL blocks in the terminal
- [ ] Record CTA with Discord link on screen
- [ ] Thumbnail: "Kimball and the Customer Dimension Trap" (conflicting number front and center)

---

## Distribution Links (fill in after publishing)

| Platform | Derived content | Status |
|---|---|---|
| YouTube | Full episode | — |
| LinkedIn Post | "3 customer-counting mistakes in a DW — and how Kimball fixes each one" | — |
| LinkedIn Newsletter | Edition: "The dimension every DW underestimates" | — |
| Medium | Long-form article: "SCD-2, Bridge Tables, and date chains — the trio from Kimball chapters 8 and 16" | — |
| Instagram | Carousel: Bridge Table diagram explained in 5 slides | — |
| Discord #book-club | Question of the week posted | — |

---

*Document created 2026-05-06 | Book Club T1 — Chapters 8 + 16 of the Kimball Toolkit*
