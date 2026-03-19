from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from api.models.base import AuditMixin, Base
import uuid

class CustomerEmailModel(Base, AuditMixin):
    
    __tablename__ = "customer_email"
    __table_args__ = ({"schema": "person"})
    
    customer_email_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("person.customer.customer_id"), nullable=False)
    customer_email = Column(String(200), nullable=False)
    is_primary = Column(Integer, nullable=False, default=1) # 1 para true, 0 para false (ou Boolean se DB suportar)
    email_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)