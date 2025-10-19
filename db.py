from sqlalchemy import create_engine, text

# Replace credentials with your own
DB_URL = "postgresql://postgre:twitter77@localhost:5432/mod_system"

engine = create_engine(DB_URL)
