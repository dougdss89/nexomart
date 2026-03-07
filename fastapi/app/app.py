import sys
from contextlib import asynccontextmanager

from sqlalchemy.orm import Session

from app.api.db.base import Base
from app.api.db.database import DatabaseConnection
from app.api.db.settings import settings
from app.api.v1.core.config import settings as coresettings
from app.api.v1.handlers.router import router
from fastapi import Depends, FastAPI, HTTPException, Request


#lifecycle manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    
    #manage application lifecycle
    
    print("INFO: [LIFESPAN] Starting application ...")
    
    try:
        print("INFO: [LIFESPAN] Creating database connection instance...")
        db_conn = DatabaseConnection(db_url=settings.DATABASE_URL)
        
        print("INFO: [LIFESPAN] Starting test connection...")
        if not db_conn.test_connection():
        
            print("FATAL ERROR [LIFESPAN] Database connection failed")
            raise RuntimeError("Database connection failed")
        
        print("INFO: [LIFESPAN] Database connection successful")
        
        app.state.db_connection = db_conn
        
        print("INFO: [LIFESPAN] Verifying database tables...")
        engine = db_conn._engine
        
        Base.metadata.create_all(bind=engine)
        
        print("INFO: [LIFESPAN] Database tables created successfully")
        
    except Exception as e:
        print(f"FATAL ERROR: [LIFESPAN] Unnexpected error occurr. {e}")
        
        sys.exit(1)
        
    print("INFO: [LIFESPAN] Application started successfully ")
    print("-" * 40)
    
    yield
    
    print("-" * 40)
    print("INFO: [LIFESPAN] Shutting down application ...")
    
    app.state.db_connection = None
    print("INFO: [LIFESPAN] Application shut down successfully")
    
#INSTANTIATING FASTAPI
app = FastAPI(
    title=coresettings.PROJECT_NAME,
    lifespan=lifespan,
    openapi_url= f"{coresettings.API_V1_STR}/openapi.json"

)

def get_db(request: Request):
    
    #get database connection dependencies from app.state
    
    db_conn: DatabaseConnection = request.app.state.db_connection
    if not db_conn:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    session = db_conn.get_session()
    try:
        yield session
    finally:
        session.close()
        
@app.get("/")
async def root():
    return {"message": "API is running!"}

@app.get("/db-status")
async def db_status(db: Session = Depends(get_db)):
    return {"status": "OK"}

app.include_router(router, prefix=coresettings.API_V1_STR)