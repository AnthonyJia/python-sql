from sqlalchemy.orm import Session
from sqlalchemy import func
from ingest import create_db_engine, Location, SensorReading
import pandas as pd
from datetime import datetime, timedelta

def get_location_stats(location_code: str):
    """Get statistical summary for a specific location"""
    engine = create_db_engine()
    session = Session(engine)
    
    try:
        # Get location details
        location = session.query(Location).filter_by(location_code=location_code).first()
        if not location:
            print(f"Location code {location_code} not found!")
            return
        
        print(f"\nStats for {location.location_name} ({location_code}):")
        print("-" * 50)
        
        # Get aggregate statistics
        stats = session.query(
            func.count(SensorReading.id).label('total_readings'),
            func.min(SensorReading.temperature).label('min_temp'),
            func.max(SensorReading.temperature).label('max_temp'),
            func.avg(SensorReading.temperature).label('avg_temp'),
            func.min(SensorReading.humidity).label('min_humidity'),
            func.max(SensorReading.humidity).label('max_humidity'),
            func.avg(SensorReading.humidity).label('avg_humidity')
        ).filter_by(location_code=location_code).first()
        
        print(f"Total readings: {stats.total_readings}")
        print(f"Temperature range: {stats.min_temp:.1f}°C to {stats.max_temp:.1f}°C")
        print(f"Average temperature: {stats.avg_temp:.1f}°C")
        print(f"Humidity range: {stats.min_humidity:.1f}% to {stats.max_humidity:.1f}%")
        print(f"Average humidity: {stats.avg_humidity:.1f}%")
        
    finally:
        session.close()

def get_latest_readings(location_code: str, limit: int = 5):
    """Get most recent readings for a specific location"""
    engine = create_db_engine()
    session = Session(engine)
    
    try:
        readings = session.query(SensorReading)\
            .filter_by(location_code=location_code)\
            .order_by(SensorReading.timestamp.desc())\
            .limit(limit)\
            .all()
        
        print(f"\nLatest {limit} readings for {location_code}:")
        print("-" * 50)
        for reading in readings:
            print(f"Timestamp: {reading.timestamp}, "
                  f"Temperature: {reading.temperature:.1f}°C, "
                  f"Humidity: {reading.humidity:.1f}%")
    
    finally:
        session.close()

def get_daily_averages(location_code: str, days: int = 7):
    """Get daily averages for the last N days"""
    engine = create_db_engine()
    session = Session(engine)
    
    try:
        # Calculate date range
        end_date = session.query(func.max(SensorReading.timestamp)).scalar()
        start_date = end_date - timedelta(days=days)
        
        # Query daily averages
        daily_stats = session.query(
            func.date(SensorReading.timestamp).label('date'),
            func.avg(SensorReading.temperature).label('avg_temp'),
            func.avg(SensorReading.humidity).label('avg_humidity')
        ).filter(
            SensorReading.location_code == location_code,
            SensorReading.timestamp >= start_date
        ).group_by(
            func.date(SensorReading.timestamp)
        ).all()
        
        print(f"\nDaily averages for {location_code} (last {days} days):")
        print("-" * 50)
        for stat in daily_stats:
            print(f"Date: {stat.date}, "
                  f"Avg Temp: {stat.avg_temp:.1f}°C, "
                  f"Avg Humidity: {stat.avg_humidity:.1f}%")
    
    finally:
        session.close()

def list_all_locations():
    """List all available location codes and names"""
    engine = create_db_engine()
    session = Session(engine)
    
    try:
        locations = session.query(Location).all()
        print("\nAvailable Locations:")
        print("-" * 50)
        for loc in locations:
            print(f"{loc.location_code}: {loc.location_name}")
    
    finally:
        session.close()

if __name__ == "__main__":
    # Example usage
    list_all_locations()
    
    # Example for NYC
    location_code = "NYC"
    get_location_stats(location_code)
    get_latest_readings(location_code)
    get_daily_averages(location_code)
