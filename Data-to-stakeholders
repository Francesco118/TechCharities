import pandas as pd
from sqlalchemy import create_engine

# ----------------------------
# Step 1: Read the CSV Data
# ----------------------------
csv_file = 'device_data.csv'  # Replace with your CSV file path
try:
    df = pd.read_csv(csv_file)
    print("CSV data loaded successfully!")
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit(1)

# ----------------------------
# Step 2: Data Cleaning & Transformation
# ----------------------------
# Example cleaning: Remove rows missing critical columns
required_columns = ['DeviceID', 'Model', 'SerialNumber']
df_clean = df.dropna(subset=required_columns)

# Convert columns to appropriate data types if needed
# For example, converting RAM and Storage to numeric types (if they exist in your data)
if 'RAM' in df_clean.columns:
    df_clean['RAM'] = pd.to_numeric(df_clean['RAM'], errors='coerce')
if 'Storage' in df_clean.columns:
    df_clean['Storage'] = pd.to_numeric(df_clean['Storage'], errors='coerce')

# Optionally, drop rows that have become NaN after conversion
df_clean = df_clean.dropna(subset=['RAM', 'Storage'])

print("Data cleaned and transformed:")
print(df_clean.head())

# ----------------------------
# Step 3: Load Data into a SQLite Database
# ----------------------------
# Create a database engine using SQLAlchemy (here using SQLite for demonstration)
engine = create_engine('sqlite:///device_data.db')  # This creates a local SQLite database

# Write the cleaned data into a table named 'devices'
try:
    df_clean.to_sql('devices', con=engine, if_exists='replace', index=False)
    print("Data successfully loaded into the SQLite database!")
except Exception as e:
    print(f"Error loading data into the database: {e}")

# ----------------------------
# Optional Step 4: Query the Database for Verification
# ----------------------------
# Example: Read back the data from the database to verify the load
try:
    df_db = pd.read_sql('SELECT * FROM devices', con=engine)
    print("Data loaded from database:")
    print(df_db.head())
except Exception as e:
    print(f"Error querying the database: {e}")
