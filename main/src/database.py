from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Models

class Database:

    """
    def status(self):
            return "Pool size: %d  Connections in pool: %d "\
                "Current Overflow: %d Current Checked out "\
                "connections: %d" % (self.size(),
                                    self.checkedin(),
                                    self.overflow(),
                                    self.checkedout())
    """

    def __init__(self) -> None:
        self.ENGINE = create_engine("sqlite+pysqlite:///data/database.db", max_overflow=-1) #pool_size=10
        self.SESSION = sessionmaker(bind=self.ENGINE)
        self.MODELS = Models()

    def createSession(self):
        return self.SESSION()
    
    def createTables(self):
        self.MODELS.getBase().metadata.create_all(bind=self.ENGINE)