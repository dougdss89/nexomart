import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spark.src.common.transformer import BaseTransformer

class CustomerConsentTransformer(BaseTransformer):
    
    """ Contactability from marketing consent. Missing consent counts
    as NO consent and consent_missing keeps two cases distinguishable"""
    
    
    def __init__(
        self,
        email_col: str = "marketing_consent_email",
        sms_col: str = "marketing_consent_sms",
    ):
        
        self._email = email_col
        self._sms = sms_col
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        email, sms = F.col(self._email), F.col(self._sms)
        can_email = F.coalesce(email, F.lit(False))
        can_sms = F.coalesce(sms, F.lit(False))
        
        return df.withColumns({
            "can_email": can_email,
            "can_sms": can_sms,
            "is_contactable": can_email | can_sms,
            "consent_missing": email.isNull() & sms.isNull() 
        })