from sqlalchemy import create_engine, Column, String, DateTime, Float, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship
from urllib.parse import quote_plus
import pandas as pd
from datetime import datetime
import os

# Create base class for declarative models
Base = declarative_base()

# Define ORM classes
class Location(Base):
    __tablename__ = 'locations'
    
    location_code = Column(String(3), primary_key=True)
    location_name = Column(String(100), nullable=False)
    readings = relationship("SensorReading", back_populates="location")
    
    def __repr__(self):
        return f"<Location(code={self.location_code}, name={self.location_name})>"

class SensorReading(Base):
    __tablename__ = 'sensor_readings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    location_code = Column(String(3), ForeignKey('locations.location_code'), nullable=False)
    temperature = Column(Float(precision=1), nullable=False)
    humidity = Column(Float(precision=1), nullable=False)
    
    location = relationship("Location", back_populates="readings")
    
    def __repr__(self):
        return f"<SensorReading(timestamp={self.timestamp}, location={self.location_code})>"

def create_db_engine():
    """Create and return SQLAlchemy engine"""
    DB_USER = ''
    DB_PASS = os.getenv('DBPASS')
    DB_HOST = ''
    DB_NAME = ''
    DB_PORT = '3306'
    
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def ingest_data():
    """Ingest data from CSV files into MySQL tables"""

    try:
        # Create engine and tables
        engine = create_db_engine()
        Base.metadata.create_all(engine)
        
        # Read CSV files
        locations_df = pd.read_csv('locations.csv')
        readings_df = pd.read_csv('sensor_readings.csv')
        
        # Create database session
        session = Session(engine)
        
        try:
            # Insert locations
            print("Inserting locations...")
            for _, row in locations_df.iterrows():
                location = Location(
                    location_code=row['Location Code'],
                    location_name=row['Location Name']
                )
                session.merge(location)  # merge instead of add to handle duplicates
            
            # Commit locations
            session.commit()
            
            # Insert sensor readings in chunks
            print("Inserting sensor readings...")
            chunk_size = 1000
            for i in range(0, len(readings_df), chunk_size):
                chunk = readings_df.iloc[i:i + chunk_size]
                
                for _, row in chunk.iterrows():
                    reading = SensorReading(
                        timestamp=pd.to_datetime(row['Timestamp']),
                        location_code=row['Location Code'],
                        temperature=float(row['Temperature (°C)']),
                        humidity=float(row['Humidity (%)'])
                    )
                    session.add(reading)
                
                session.commit()
                print(f"Processed {i + len(chunk)} records...")
            
            print("Data ingestion completed successfully!")
            
        except Exception as e:
            session.rollback()
            raise e
        
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error during data ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        ingest_data()
    except Exception as e:
        print(f"Script failed: {str(e)}")
