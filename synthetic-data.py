import csv
import random
from datetime import datetime, timedelta

# Define locations
locations = [
    ("New York City", "NYC"),
    ("Los Angeles", "LAX"),
    ("Chicago", "CHI"),
    ("Miami", "MIA"),
    ("Denver", "DEN"),
    ("Seattle", "SEA"),
    ("Phoenix", "PHX"),
    ("Boston", "BOS")
]

# Create locations CSV
with open('locations.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Location Name', 'Location Code'])
    writer.writerows(locations)

# Generate sensor readings
readings = []
start_date = datetime(2024, 1, 1)

for i in range(1000):
    location_code = random.choice([loc[1] for loc in locations])
    
    # Generate realistic temperature based on location
    base_temp = {
        'NYC': 15, 'LAX': 22, 'CHI': 12, 'MIA': 25,
        'DEN': 10, 'SEA': 13, 'PHX': 30, 'BOS': 12
    }
    
    # Add some random variation
    temperature = round(base_temp[location_code] + random.uniform(-5, 5), 1)
    humidity = round(random.uniform(30, 80), 1)
    
    # Generate timestamp
    timestamp = start_date + timedelta(hours=i)
    
    readings.append([
        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        location_code,
        temperature,
        humidity
    ])

# Create readings CSV
with open('sensor_readings.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Timestamp', 'Location Code', 'Temperature (°C)', 'Humidity (%)'])
    writer.writerows(readings)

print("Files created successfully!")
