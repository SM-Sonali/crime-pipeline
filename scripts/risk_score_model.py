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

# Load data
cursor.execute("SELECT city, victim_age FROM crime_processed")
rows = cursor.fetchall()

df = pd.DataFrame(rows, columns=["city", "victim_age"])

# Calculate risk metrics
risk_df = df.groupby("city").agg(
    crime_count=("city", "count"),
    avg_victim_age=("victim_age", "mean")
).reset_index()

# Risk score formula
risk_df["risk_score"] = (
    risk_df["crime_count"] * 0.7 +
    risk_df["avg_victim_age"] * 0.3
)

print("Risk scores calculated")

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS crime_risk_scores(
city TEXT,
crime_count INT,
avg_victim_age FLOAT,
risk_score FLOAT
)
""")

conn.commit()

# Clear table
cursor.execute("TRUNCATE TABLE crime_risk_scores")
conn.commit()

# Insert data
insert_query = """
INSERT INTO crime_risk_scores(city, crime_count, avg_victim_age, risk_score)
VALUES (%s,%s,%s,%s)
"""

for _, row in risk_df.iterrows():
    cursor.execute(insert_query, (
        row["city"],
        int(row["crime_count"]),
        float(row["avg_victim_age"]),
        float(row["risk_score"])
    ))

conn.commit()

cursor.close()
conn.close()

print("Risk score table updated successfully")