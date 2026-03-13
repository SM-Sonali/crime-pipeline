import pandas as pd
import psycopg2

# -----------------------------
# PostgreSQL Connection
# -----------------------------
conn = psycopg2.connect(
    host="localhost",
    database="crime",
    user="postgres",
    password="sonali",
    port="5432"
)

cursor = conn.cursor()

print("✅ Connected to PostgreSQL")

# -----------------------------
# Load data
# -----------------------------
cursor.execute("SELECT * FROM crime_raw")
rows = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]

df = pd.DataFrame(rows, columns=columns)

print("✅ Data loaded from crime_raw")

# -----------------------------
# Convert dates
# -----------------------------
df['date_reported'] = pd.to_datetime(df['date_reported'], errors='coerce', dayfirst=True)
df['date_of_occurrence'] = pd.to_datetime(df['date_of_occurrence'], errors='coerce', dayfirst=True)
df['time_of_occurrence'] = pd.to_datetime(df['time_of_occurrence'], errors='coerce', dayfirst=True)
df['date_case_closed'] = pd.to_datetime(df['date_case_closed'], errors='coerce', dayfirst=True)

# -----------------------------
# Feature Engineering
# -----------------------------
df['hour'] = df['time_of_occurrence'].dt.hour
df['day'] = df['date_of_occurrence'].dt.day
df['month'] = df['date_of_occurrence'].dt.month
df['year'] = df['date_of_occurrence'].dt.year

print("✅ Feature engineering completed")

# -----------------------------
# Create table if not exists
# -----------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS crime_processed(
report_number INT,
date_reported TIMESTAMP,
date_of_occurrence TIMESTAMP,
time_of_occurrence TIMESTAMP,
city TEXT,
crime_code INT,
crime_description TEXT,
victim_age INT,
victim_gender TEXT,
weapon_used TEXT,
crime_domain TEXT,
police_deployed INT,
case_closed TEXT,
date_case_closed TIMESTAMP,
hour INT,
day INT,
month INT,
year INT
)
""")

conn.commit()

print("✅ Table check completed")

# -----------------------------
# Clear existing data
# -----------------------------
cursor.execute("TRUNCATE TABLE crime_processed")
conn.commit()

print("✅ Old processed data cleared")

# -----------------------------
# Insert processed data
# -----------------------------
insert_query = """
INSERT INTO crime_processed VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

for _, row in df.iterrows():

    # Convert NaT to None
    cleaned_row = [
        None if pd.isna(value) else value
        for value in row
    ]

    cursor.execute(insert_query, cleaned_row)

conn.commit()

print("✅ Processed data inserted into crime_processed")

cursor.close()
conn.close()

print("🎉 Data processing pipeline completed successfully!")