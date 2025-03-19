# Python + SQL

Load some mock data into SQL tables using the ORM (Object Relational Model) built into SQLAlchemy.

## Create synthetic data

Use `synthetic-data.py` to create two CSV files, one with locations and their codes, and another with
synthetic temperature and humidity data tied to those locations.

## Load the Data 

Use `ingest.py` to establish a connection to a MySQL endpoint, create tables based on class definitions,
and load the two CSV files into the database.

That file also contains the classes used by other files.

## Fetch Statistics

Use `get-stats.py` to fetch basic statistics for a specific location in the dataset.
