import uuid
import pycountry
# ADICIONADO: model_validator na importação
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime

class CountryRegion(BaseModel):
    
    country_id: int = Field(gt=0)
    
    # ISO 3166-1 alpha-2 (ex: BR, US, CA) tem 2 letras
    country_code: str = Field(min_length=2, max_length=2) 
    
    country_region: str = Field(max_length=100)
    
    # DICA: Se a gente vai preencher automático, pode ser Opcional na entrada
    # Mas se for obrigatório enviar, mantemos str sem Optional
    country_name: str = Field(None, max_length=100) 
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- VALIDAÇÃO DE CÓDIGO ---
    @field_validator('country_code')
    @classmethod
    def validate_iso_code(cls, v: str) -> str:
        code = v.upper() # Garante padronização (br -> BR)
        
        # O pycountry busca pelo código Alpha-2
        country = pycountry.countries.get(alpha_2=code)
        
        if not country:
            raise ValueError(f"O código de país '{v}' não é um código ISO 3166-1 válido.")
            
        return code

    # --- AUTO-PREENCHIMENTO (ENRICHMENT) ---
    @model_validator(mode='after')
    def fill_official_name(self):
        # Busca o objeto país pelo código já validado
        country = pycountry.countries.get(alpha_2=self.country_code)
        
        if country:
            # AQUI ESTÁ A LÓGICA DE PADRONIZAÇÃO:
            # Se o usuário não mandou nome, OU se você quer forçar o nome oficial (Brazil)
            # Descomente a linha abaixo para forçar sempre o nome oficial em inglês:
            # self.country_name = country.name 
            
            # Se você prefere manter o nome que o usuário mandou (ex: "Brasil") 
            # e só preencher se estiver vazio:
            if not self.country_name:
                self.country_name = country.name
        
        return self

    class Config:
        from_attributes = True