from sqlalchemy import Column, Integer, String, Foreign_Key
from sqlalchemy.dialects.postgresql import UUID
from api.models.base import AuditMixin, Base
import uuid

class CustomerEmailModel(Base, AuditMixin):
    
    __tablename__ = "customer_email"
    __table_args__ = ({"schema": "person"})
    
    customer_email_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, Foreign_Key("person.customer.customer_id"), nullable = False)
    customer_email = Column(String(200), nullable=False)
    email_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
    