import os
from sqlalchemy import create_engine
def get_engine():
    host=os.getenv("PG_HOST","localhost"); port=os.getenv("PG_PORT","5432")
    db=os.getenv("PG_DB","brasilcap"); user=os.getenv("PG_USER","postgres"); pwd=os.getenv("PG_PASSWORD","postgres")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}", pool_pre_ping=True)