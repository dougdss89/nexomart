"""
Data Platform — Architecture Diagrams (Post-Debate Roadmap)
Generates two PNGs:
  1. data_platform_pipeline.png — 7 pipeline stages (LR, backbone only)
  2. data_platform_support.png  — Airflow · Observability · Catalog · dbt

Run: uv run python3 diagram.py
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.generic.blank import Blank
from diagrams.onprem.analytics import Dbt, PowerBI, Spark, Superset, Trino
from diagrams.onprem.analytics import Flink as GreatExpectations
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.monitoring import Datadog, Grafana, Prometheus, Zabbix
from diagrams.onprem.queue import Kafka
from diagrams.onprem.tracing import Jaeger
from diagrams.onprem.vcs import Git
from diagrams.onprem.workflow import Airflow
from diagrams.programming.framework import FastAPI

# ── Shared palette ─────────────────────────────────────────────────────────────
NODE = {"fontcolor": "#e6edf3", "fontsize": "11", "fontname": "Helvetica"}


def cls(bg, fc, pc, margin="22"):
    return {
        "fontsize": "13",
        "fontname": "Helvetica Bold",
        "style": "rounded,filled",
        "margin": margin,
        "bgcolor": bg,
        "fontcolor": fc,
        "pencolor": pc,
    }


C_DEF = cls("#161b22", "#79c0ff", "#30363d")
C_NEW = cls("#0a1a0a", "#3fb950", "#3fb950")
C_GOLD = cls("#1c1a0e", "#e3b341", "#e3b341")
C_OBS = cls("#130e1a", "#a371f7", "#a371f7")
C_PRES = cls("#0e1c2e", "#58a6ff", "#388bfd")
C_ORCH = cls("#1a0e0e", "#f78166", "#f78166")
C_PROC = cls("#0e1818", "#3fb950", "#30363d")

E = lambda lbl="": Edge(color="#58a6ff", fontcolor="#8b949e", fontsize="9", label=lbl)
EK = lambda lbl="": Edge(
    color="#3fb950", style="bold", fontcolor="#3fb950", fontsize="9", label=lbl
)
EA = lambda lbl="": Edge(
    color="#f78166", style="dashed", fontcolor="#f78166", fontsize="9", label=lbl
)
EC = lambda lbl="": Edge(
    color="#d29922", style="dashed", fontcolor="#d29922", fontsize="9", label=lbl
)
EQ = lambda lbl="": Edge(
    color="#a371f7", style="dashed", fontcolor="#a371f7", fontsize="9", label=lbl
)
EP = lambda lbl="": Edge(color="#e3b341", fontcolor="#e3b341", fontsize="9", label=lbl)
EO = lambda lbl="": Edge(
    color="#79c0ff", style="dotted", fontcolor="#79c0ff", fontsize="9", label=lbl
)


# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 1 — Main Data Pipeline
# Only the 7 pipeline stages. No isolated clusters (they live in diagram 2).
# ~14 edges total — all forward-facing LR → clean layout guaranteed.
# ══════════════════════════════════════════════════════════════════════════════
GRAPH_1 = {
    "fontsize": "26",
    "fontname": "Helvetica Bold",
    "bgcolor": "#0d1117",
    "fontcolor": "#e6edf3",
    "pad": "2.5",
    "splines": "ortho",
    "nodesep": "0.9",
    "ranksep": "2.4",
    "labeljust": "c",
    "labelloc": "t",
    "dpi": "180",
}

with Diagram(
    "\nData Platform  —  Main Data Pipeline\n★ = Roadmap (new)          No ★ = Existing",
    filename="data_platform_pipeline",
    outformat="png",
    direction="LR",
    graph_attr=GRAPH_1,
    node_attr=NODE,
    show=False,
):
    # ── Stage 1: Ingestion ─────────────────────────────────────────────────────
    # dbapi runs in CLI mode (BashOperator) — there is no persistent online service.
    # The 3-layer architecture (Repository·Service·Router) exists in the code and
    # demonstrates the pattern; the load happens via "uv run python seeder.py" in Airflow.
    with Cluster("① Data Loaders  ·  dbapi  (CLI mode via BashOperator)", graph_attr=C_DEF):
        api = FastAPI(
            "FastAPI  ·  3-Layer REST API\n"
            "Invoked via BashOperator (CLI)\n"
            "Does not run as an online service"
        )
        seeders = Blank(
            "Seeders  ·  Chaos Engineering\n1–10% anomalies injected\nDuplicates · Fraud · Outliers"
        )

    # ── Stage 2: Event Bus ★ ───────────────────────────────────────────────────
    with Cluster("② Event Bus  ★", graph_attr=C_NEW):
        kafka = Kafka("Apache Kafka  ·  Message Broker")
        schema_rg = Git("Karapace ★  ·  Schema Registry\nAvro · FULL_TRANSITIVE compatibility")
        outbox = PostgreSQL("Outbox Table ★\nExactly-Once via Debezium CDC")
        outbox >> EO("relay") >> kafka
        kafka >> EO("validate") >> schema_rg

    # ── Stage 3: Transactional Store ──────────────────────────────────────────
    with Cluster("③ Transactional Store  ·  OLTP", graph_attr=C_DEF):
        pg = PostgreSQL(
            "PostgreSQL 16  ·  7 schemas\n"
            "asyncpg · AuditMixin · bulk insert\n"
            "sales · warehouse · marketing · …"
        )
        redis = Redis("Redis ★  ·  Cache-Aside\nDimension TTL 1–3h")

    # ── Stage 4: Processing & Quality ★ ───────────────────────────────────────
    with Cluster("④ Processing  &  Quality  ★", graph_attr=C_PROC):
        spark = Spark(
            "Apache Spark  ·  ETL + Streaming\n"
            "Kafka consumer → Bronze → Silver\n"
            "SparkSubmitOperator via Airflow"
        )
        dbt = Dbt(
            "dbt ★  ·  Silver → Gold\n"
            "Staging · Intermediate · Marts\n"
            "not_null · unique · relationships"
        )
        ge = GreatExpectations(
            "Great Expectations ★\nValidation Suite · DQ Log\nQuarantine for chaos data"
        )

    # ── Stage 5: Data Lake ─────────────────────────────────────────────────────
    with Cluster("⑤ Data Lake  ·  MinIO  S3-compatible", graph_attr=C_DEF):
        with Cluster(
            "Delta Lake  ·  cataloged by Hive Metastore", graph_attr={**C_DEF, "margin": "12"}
        ):
            landing = S3("Landing\nRaw CSV / JSON")
            bronze = S3("Bronze\nDelta · raw ingest")
            silver = S3("Silver\nDelta · cleansed")
            gold = S3("Gold\nDelta Marts")
            landing >> E() >> bronze >> E() >> silver >> E() >> gold

    # ── Stage 6: Query Federation · CQRS ★ ────────────────────────────────────
    with Cluster("⑥ CQRS  ·  Read Path  ★", graph_attr=C_GOLD):
        trino = Trino(
            "Trino  ·  Federated SQL · OLAP\n"
            "Catalogs: Delta Lake · PostgreSQL\n"
            "Write path: PostgreSQL   Read: Delta"
        )

    # ── Stage 7: Presentation ──────────────────────────────────────────────────
    with Cluster("⑦ BI & Analytics", graph_attr=C_PRES):
        superset = Superset("Apache\nSuperset\nSelf-service BI")
        powerbi = PowerBI("Power BI\nReports")
        ml = Blank("ML /\nFeature Store")
        analysts = Blank("Data Analysts\nAd-hoc SQL")

    # ══════════════════════════════════════════════════════════════════════════
    # BACKBONE — 6 edges, strictly one stage at a time (no stage skipping)
    # Each edge goes: stage N → stage N+1 only, guaranteeing clean LR layout
    # ══════════════════════════════════════════════════════════════════════════

    # 1 → 2
    api >> EK("publish Avro\n(Outbox Relay)") >> kafka

    # 2 → 3
    kafka >> EK("consumer group\nbulk insert") >> pg

    # 3 → 4  (PostgreSQL extract feeds Spark via landing; modeled as 3→4)
    pg >> E("extract → Landing\n→ Spark ETL") >> spark

    # 4 → 5
    spark >> E("write Iceberg\nbronze · silver") >> bronze

    # Within stage 5: silver → dbt → gold (already connected inside cluster)
    silver >> E("source") >> dbt
    dbt >> E("materialize") >> gold

    # 5 → 6
    gold >> EP("OLAP reads\n· Iceberg") >> trino

    # 6 → 7
    trino >> EP() >> superset
    trino >> EP() >> powerbi
    trino >> EP() >> ml
    trino >> EP() >> analysts

    # GE validates silver
    ge >> EQ("validate &\nquarantine") >> silver

    # Seeders also publish
    seeders >> EK("publish bulk") >> kafka

    # CQRS: federated OLTP reads
    pg >> EP("federated\nOLTP") >> trino


print("✅  Pipeline diagram: data_platform_pipeline.png")


# ══════════════════════════════════════════════════════════════════════════════
# DIAGRAM 2 — Support Stack
# Orchestration · Observability · Catalog & Lineage · Data Quality (dbt)
# ══════════════════════════════════════════════════════════════════════════════
GRAPH_2 = {
    "fontsize": "26",
    "fontname": "Helvetica Bold",
    "bgcolor": "#0d1117",
    "fontcolor": "#e6edf3",
    "pad": "2.5",
    "splines": "ortho",
    "nodesep": "0.9",
    "ranksep": "2.4",
    "labeljust": "c",
    "labelloc": "t",
    "dpi": "180",
}

with Diagram(
    "\nData Platform  —  Support Stack\n"
    "Orchestration  ·  Observability  ·  Catalog & Lineage  ·  Data Quality",
    filename="data_platform_support",
    outformat="png",
    direction="LR",
    graph_attr=GRAPH_2,
    node_attr=NODE,
    show=False,
):
    # ── Orchestration ──────────────────────────────────────────────────────────
    with Cluster("Orchestration  ·  Apache Airflow", graph_attr=C_ORCH):
        airflow = Airflow("DAG Scheduler\nCeleryExecutor")
        redis_b = Redis("Redis\nCelery Broker")
        op_spark = Blank("SparkSubmitOperator\nETL + Streaming jobs")
        op_bash = Blank("BashOperator\nSeeders · dbt · GE")
        op_kafka = Blank("KafkaProduceOperator ★\nEvent publishing")
        op_sensor = Blank("Sensors\nExternalTask · S3 · SQL")
        airflow >> E() >> redis_b
        airflow - op_spark - op_bash - op_kafka - op_sensor

    # ── Catalog & Governance ★ ─────────────────────────────────────────────────
    with Cluster("Catalog  &  Governance  ★", graph_attr=C_NEW):
        hms = Git("Hive Metastore\nCatalog · Delta + Iceberg\nThrift metastore (PostgreSQL)")
        marquez = Zabbix("Marquez  (OpenLineage UI) ★\nAuto-lineage:\nAirflow · Spark · dbt")
        hms >> EC("catalog ops") >> marquez

    # ── Data Quality & Transformations ★ ──────────────────────────────────────
    with Cluster("Data Quality  &  Transformations  ★", graph_attr=C_NEW):
        with Cluster("dbt Project  ·  Silver → Gold", graph_attr={**C_NEW, "margin": "12"}):
            dbt_staging = Blank("staging/\nstg_sales_orders · stg_customers")
            dbt_int = Blank("intermediate/\nint_sales_enriched · int_customer_360")
            dbt_marts = Blank("marts/\nfct_sales_orders · dim_customers\nfct_warehouse_movements")
            dbt_staging >> E() >> dbt_int >> E() >> dbt_marts
        ge_suite = GreatExpectations("Great Expectations\nCheckpoint Suite\nQuarantine + DQ Log")
        dbt_staging - ge_suite

    # ── Observability ★ ────────────────────────────────────────────────────────
    with Cluster("Observability Stack  ★", graph_attr=C_OBS):
        otel_col = Datadog("OTel Collector\nOTLP gRPC")
        with Cluster("Traces", graph_attr={**C_OBS, "margin": "10"}):
            jaeger = Jaeger("Jaeger\nDistributed Traces")
        with Cluster("Metrics", graph_attr={**C_OBS, "margin": "10"}):
            prometheus = Prometheus("Prometheus\nScrape + AlertManager")
        with Cluster("Dashboards", graph_attr={**C_OBS, "margin": "10"}):
            grafana = Grafana("Grafana\nTraces · Metrics · Logs")
        otel_col >> EQ("traces") >> jaeger
        otel_col >> EQ("metrics") >> prometheus
        jaeger >> EQ() >> grafana
        prometheus >> EQ() >> grafana

    # ── Flows ──────────────────────────────────────────────────────────────────
    airflow >> EA("SparkSubmit") >> op_spark
    airflow >> EA("BashOperator") >> op_bash
    airflow >> EA("KafkaProduce ★") >> op_kafka
    airflow >> EA("Sensors") >> op_sensor
    airflow >> EQ("OpenLineage events") >> marquez
    op_bash >> EQ("OpenLineage events") >> marquez
    uc >> EC("Delta catalog ops") >> marquez


print("✅  Support diagram:   data_platform_support.png")
print()
print("Generated diagrams:")
print("  • data_platform_pipeline.png — main pipeline (README / LinkedIn)")
print("  • data_platform_support.png  — Airflow · Observability · Catalog · dbt")
