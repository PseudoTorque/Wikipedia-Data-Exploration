from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, Text

class Models:
      BASE = declarative_base()   

      def getBase(self):
          return self.BASE
    
      class Hyperlink(BASE):

            __tablename__ = "Hyperlinks"

            ID = Column(Integer, primary_key = True)
            PARENT_HYPERLINK = Column(String)
            ATTEMPTS = Column(Integer)
            HYPERLINKS_SCRAPED = Column(Boolean)
            CONTENT_SCRAPED = Column(Boolean)
            HYPERLINK = Column(String)
            PARENT_PRIORITY = Column(Integer)
            TIMESTAMP = Column(Float)

      class Page(BASE):
           
            __tablename__ = "Pages"

            ID = Column(Integer, primary_key = True)
            HYPERLINK = Column(String)
            TITLE = Column(String)
            HEADING = Column(String)
            CONTENT = Column(Text)
            TIMESTAMP = Column(Float)
