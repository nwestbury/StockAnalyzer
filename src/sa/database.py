from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

"""
Defines the database class that gives functions that allow certain functions
on the database. It is important to seperate the actual implementation of the db
from the general code.

Two gotchas:
     -Try/Except expected in __init__() for custom handling
     -This class assumes the tables are already present
"""

engine = create_engine('postgresql://root:toor@localhost/stockanalyzer')
Session = sessionmaker(bind=engine)
