import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl, model_validator


class VendorContact(BaseModel):
    name: str = Field(max_length=100)
    email: EmailStr
    phone: str = Field(max_length=20)


class VendorDetails(BaseModel):
    tax_id: str = Field(max_length=20)
    payment_terms: str = Field(max_length=100)
    certifications: list = Field(default_factory=list)
    primary_contact: VendorContact
    average_lead_time_days: int = Field(gt=0)
    on_time_delivery_rate: float = Field(ge=0.0, le=1.0)


class Vendor(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    vendor_id: int = Field(gt=0)
    business_entity_id: int = Field(gt=0)
    account_number: str = Field(max_length=20)

    vendor_name: str = Field(max_length=100)
    credit_rating: Decimal = Field(ge=1, le=5)

    vendor_details: VendorDetails
    purchasing_webservice_url: HttpUrl | None = None

    # --- SCD TYPE 2 (History) ---
    effective_start_date: datetime
    effective_end_date: datetime | None = None
    is_current: bool

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime | None = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def validate_vendor_status(self):
        # Business Rule: If the risk is High (5), the vendor cannot be 'is_active'
        # unless it has a special approval (simplified here)
        if self.credit_rating and self.credit_rating <= 2 and self.is_active:
            # This is optional, depends on your company's rule
            raise ValueError(
                "Vendors with a credit rating less than or equal to 2 cannot be automatically active."
            )

        return self

    @model_validator(mode="after")
    def validate_scd_consistency(self):
        # Temporal validation of the history
        if self.effective_end_date and self.effective_end_date < self.effective_start_date:
            raise ValueError(
                "The effective end date cannot be earlier than the effective start date."
            )

        # Consistency of the is_current flag
        if self.effective_end_date is not None and self.is_current:
            raise ValueError(
                "A finished historical record (with an end date) cannot be marked as 'is_current=True'."
            )

        if self.effective_end_date is None and self.is_current is False:
            raise ValueError(
                "A current record (without an end date) must be marked as 'is_current=True'."
            )

        return self
