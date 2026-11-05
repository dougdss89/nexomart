from spark.src.common.incremental_reader import IncrementalReader
from spark.src.common.transformer import CompositeTransformer
from spark.src.common.writer import DeltaTableWriter, LoggingWriter 
from spark.src.common.reader import LoggingReader, ParquetLandingReader 

from pyspark.sql import SparkSession

from spark.src.transformers.audit import AuditTransformer
from spark.src.transformers.metadata import MetadataTransformer

def ingestion_countryregion_raw(spark: SparkSession) -> dict:
    """Landing -> Bronze for person.countryregion.

    Copies rows as-is (no source value is changed) and only adds lineage columns:
    _ingested_at/_source_layer (Audit) and __metadata__/date_partition (Metadata).
    The first run (no Bronze table yet) loads everything; later runs only load rows newer
    than what Bronze already holds. Append-only, partitioned by date_partition.

    Returns the writer result: {"status": ..., "table": ...}.
    """
    
    reader = IncrementalReader(LoggingReader(ParquetLandingReader()))
    writer = LoggingWriter(DeltaTableWriter())
    metadata = CompositeTransformer([AuditTransformer(layer = "bronze"),
                                     MetadataTransformer(dataset= "person_countryregion")
    ])
    
    df = metadata.transform(reader.read(spark,
                                        schema = "person",
                                        table = "countryregion"
    ))
    
    return writer.write(spark,
                 df,
                 layer ="bronze",
                 schema = "person",
                 table = "countryregion",
                 partition_column = "date_partition"
    )