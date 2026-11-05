from spark.src.common.incremental_reader import IncrementalReader
from spark.src.common.transformer import CompositeTransformer
from spark.src.common.writer import DeltaTableWriter, LoggingWriter 
from spark.src.common.reader import LoggingReader, ParquetLandingReader 

from pyspark.sql import SparkSession

from spark.src.transformers.audit import AuditTransformer
from spark.src.transformers.metadata import MetadataTransformer

def ingestion_seller_raw(spark: SparkSession) -> dict:
    """Landing -> Bronze for person.seller.

    Copies rows as-is (no source value is changed) and only adds lineage columns:
    _ingested_at/_source_layer (Audit) and __metadata__/date_partition (Metadata).
    The first run (no Bronze table yet) loads everything; later runs only load rows newer
    than what Bronze already holds. Append-only, partitioned by date_partition.

    Returns the writer result: {"status": ..., "table": ...}.
    """
    
    reader = IncrementalReader(LoggingReader(ParquetLandingReader()))
    writer = LoggingWriter(DeltaTableWriter())
    metadata = CompositeTransformer([AuditTransformer("bronze"),
                                     MetadataTransformer("person_seller")
    ])
    
    df = metadata.transform(reader.read(spark,
                                        schema = "person",
                                        table = "seller"
    ))
    
    return writer.write(spark,
                        df,
                        layer = "bronze",
                        schema = "person",
                        table = "seller",
                        partition_column= "date_partition"
    )