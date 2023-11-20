from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Script that we use to connect the database to the API server.py
DATABASE_URL = "postgresql://postgres:password@localhost/nordeus_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
