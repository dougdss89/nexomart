import uuid
import pycountry
# ADDED: model_validator to the import
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime

class CountryRegion(BaseModel):
    
    country_id: int = Field(gt=0)
    
    # ISO 3166-1 alpha-2 (e.g.: BR, US, CA) has 2 letters
    country_code: str = Field(min_length=2, max_length=2) 
    
    country_region: str = Field(max_length=100)
    
    country_name: str = Field(None, max_length=100) 
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- CODE VALIDATION ---
    @field_validator('country_code')
    @classmethod
    def validate_iso_code(cls, v: str) -> str:
        code = v.upper() # Ensures standardization (br -> BR)

        # pycountry looks up by the Alpha-2 code
        country = pycountry.countries.get(alpha_2=code)

        if not country:
            raise ValueError(f"The country code '{v}' is not a valid ISO 3166-1 code.")

        return code

    # --- AUTO-FILL (ENRICHMENT) ---
    @model_validator(mode='after')
    def fill_official_name(self):
        # Looks up the country object by the already-validated code
        country = pycountry.countries.get(alpha_2=self.country_code)

        if country:
            # THIS IS THE STANDARDIZATION LOGIC:
            # If the user didn't send a name, OR if you want to force the official name (Brazil)
            # Uncomment the line below to always force the official English name:
            # self.country_name = country.name

            # If you prefer to keep the name the user sent (e.g.: "Brasil")
            # and only fill it if it's empty:
            if not self.country_name:
                self.country_name = country.name
        
        return self

    class Config:
        from_attributes = True