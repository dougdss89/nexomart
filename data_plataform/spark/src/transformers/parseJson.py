import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from spark.src.common.column_validator import ColumnValidator
from spark.src.common.transformer import BaseTransformer

class ParseJsonTransformer(BaseTransformer):
    """Specific to customer: demographics arrives as a raw JSON string. Opens it and flattens
    every nested level into columns, dropping the original column's own name as a prefix (root
    fields keep their bare name: social, address, metrics).
    Only touches columns this transformer itself produced, 
    tracked explicitly, so it never touches an unrelated struct elsewhere in the df
    (e.g. __metadata__ from MetadataTransformer). Casting/cleaning values is a later step."""
    
    def __init__(self, 
                 columns: list[str],
                 validator:ColumnValidator = ColumnValidator(StringType)
                ):
        
        self._validator = validator
        self._columns = columns
        
    def transform(self, 
                  df: DataFrame) -> DataFrame:
        
        valid_columns = self._validator.validate(df, self._columns)
        
        if not valid_columns:
            return df
        
        for col_name in valid_columns:
            
                non_null = df.select(col_name).filter(F.col(col_name).isNotNull())
                
                inferred_schema = df.sparkSession.read.json(non_null.rdd.map(lambda row: row[0])).schema
                
                df = df.withColumn(col_name, F.from_json(F.col(col_name), inferred_schema))

        return df