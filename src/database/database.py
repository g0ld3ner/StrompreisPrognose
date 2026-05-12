import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

# Default to local SQLite database in data/ directory
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/strompreisprognose.db")

engine = create_engine(
    DATABASE_URL,
    echo=False,  # Set to True for SQL debugging
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Dependency for getting a database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
