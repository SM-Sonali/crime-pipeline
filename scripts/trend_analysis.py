import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="crime",
    user="postgres",
    password="sonali",
    port="5432"
)

cursor = conn.cursor()

print("Connected to PostgreSQL")

# Load processed data
cursor.execute("SELECT city, month FROM crime_processed")
rows = cursor.fetchall()

df = pd.DataFrame(rows, columns=["city", "month"])

# Trend calculation
trend_df = df.groupby(["city", "month"]).size().reset_index(name="crime_count")

print("Trend analysis generated")

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS crime_trends(
city TEXT,
month INT,
crime_count INT
)
""")

conn.commit()

# Clear old data
cursor.execute("TRUNCATE TABLE crime_trends")
conn.commit()

# Insert new data
insert_query = """
INSERT INTO crime_trends VALUES (%s,%s,%s)
"""

for _, row in trend_df.iterrows():
    cursor.execute(insert_query, (
        row["city"],
        int(row["month"]),
        int(row["crime_count"])
    ))

conn.commit()

cursor.close()
conn.close()

print("Trend table updated")