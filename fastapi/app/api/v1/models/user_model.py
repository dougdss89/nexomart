from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.api.db.base import Base


class User_model(Base):
    __tablename__ = "users"

    user_id: Mapped[uuid4] = mapped_column(Integer, primary_key=True, index=True)
    first_name: Mapped[str] = mapped_column(String(255))
    last_name: Mapped[str] = mapped_column(String(255))
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[Boolean] = mapped_column(Boolean, default=True)
    is_superuser: Mapped[Boolean] = mapped_column(Boolean, default=True)
    is_verified: Mapped[Boolean] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now())


    def __repr__(self) -> str:
        return f"<User {self.email}>"

    def __str__(self) -> str:
        return self.email

    def __hash__(self) -> int:
        return hash(self.email)

    # verifica se já existe um valor igual. compara objetos
    def __eq__(self, other: object) -> Boolean:
        if isinstance(other, User_model):
            return self.email == other.email
        return False

    @property
    def create(self) -> datetime:
        return self.id.generation_time

    @classmethod
    async def by_email(self, email: str) -> "User_model":
        return await self.find_one(self.email == email)

    class Config:
        from_attributes = True


# class UserCreate(Base):
#     first_name: Mapped[str] = mapped_column(String(255))
#     last_name: Mapped[str] = mapped_column(String(255))
#     email: Mapped[str] = mapped_column(String(255), nullable=False)
#     password: Mapped[str] = mapped_column(String(255), nullable=False)


# class UserUpdate(Base):
#     first_name: Optional[str] = mapped_column(String(255))
#     last_name: Optional[str] = mapped_column(String(255))
#     email: Optional[str] = mapped_column(String(255))
#     password: Optional[str] = mapped_column(String(255))
#     is_active: Optional[Boolean] = mapped_column(Boolean, default=True)
#     is_superuser: Optional[Boolean] = mapped_column(Boolean, default=True)
#     is_verified: Optional[Boolean] = mapped_column(Boolean, default=True)


# class UserResponse(Base):
#     user_id: uuid4
#     first_name: Mapped[str] = mapped_column(String(255))
#     last_name: Mapped[str] = mapped_column(String(255))
#     email: Mapped[str] = mapped_column(String(255))
#     is_active: Mapped[Boolean]
#     is_superuser: Mapped[Boolean]
#     is_verified: Mapped[Boolean]
#     created_at: Mapped[datetime]
#     updated_at: Mapped[datetime]

#     class Config:
#         from_attributes = True
