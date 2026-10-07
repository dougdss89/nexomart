import uuid
from pydantic import BaseModel, Field
from datetime import datetime

class Customer_email(BaseModel):
    customer_id: int = Field(gt=0)
    customer_email: str
    is_primary: bool = False
    email_uuid: uuid.UUID = Field(default_factory=uuid.uuid4)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    class Config:
        from_attributes = True #pydantic lendo modelos do SQLAlchemy