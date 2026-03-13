import pandas as pd
import psycopg2
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder

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

print("Connected to PostgreSQL")

# -----------------------------
# Load processed data
# -----------------------------
cursor.execute("SELECT * FROM crime_processed")
rows = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]

df = pd.DataFrame(rows, columns=columns)

print("Processed data loaded")

# -----------------------------
# Feature Selection
# -----------------------------
data = df[['city', 'crime_code', 'victim_age', 'hour']].copy()

# Convert city to numeric
le = LabelEncoder()
data['city'] = le.fit_transform(data['city'])

# Fill missing values
data = data.fillna(0)

# -----------------------------
# K-Means Clustering
# -----------------------------
kmeans = KMeans(n_clusters=3, random_state=42)

df['crime_cluster'] = kmeans.fit_predict(data)

print("Hotspot clusters generated")

# -----------------------------
# Create Hotspot Table
# -----------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS crime_hotspots(
report_number INT,
city TEXT,
crime_code INT,
hour INT,
crime_cluster INT
)
""")

conn.commit()

# Clear old data
cursor.execute("TRUNCATE TABLE crime_hotspots")
conn.commit()

# -----------------------------
# Insert hotspot results
# -----------------------------
insert_query = """
INSERT INTO crime_hotspots VALUES (%s,%s,%s,%s,%s)
"""

for _, row in df.iterrows():

    cursor.execute(insert_query, (
        int(row['report_number']),
        row['city'],
        int(row['crime_code']),
        int(row['hour']),
        int(row['crime_cluster'])
    ))

conn.commit()

cursor.close()
conn.close()

print("Hotspot detection completed")