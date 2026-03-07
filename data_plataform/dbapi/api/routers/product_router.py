
from fastapi import HTTPException, Depends, status
from sqlalchemy.orm import Session


# Função para validar se os IDs existem
def validate_references(db: Session, category_id: int, model_id: int):
    #importar do models
    
    # Verifica categoria
    category_exists = db.query(ProductCategorySchema).filter(ProductCategorySchema.category_id == category_id).first()
    if not category_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Category ID {category_id} not found"
        )
    
    # Verifica modelo
    model_exists = db.query(ProductModelSchema).filter(ProductModelSchema.model_id == model_id).first()
    if not model_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Model ID {model_id} not found"
        )
        
        