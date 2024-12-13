from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)
    stock = Column(Integer)

class DatabaseManager:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def add_product(self, name, price, stock):
        session = self.Session()
        product = Product(name=name, price=price, stock=stock)
        session.add(product)
        session.commit()
        session.close()