import boto3
from boto3.session import Session
import psycopg2
 
# --- Connexion à MinIO ---
session = Session(
    aws_access_key_id="studentSDV",
    aws_secret_access_key="coucou44",
)
s3 = session.client(
    service_name="s3",
    endpoint_url="http://51.77.215.42:9010",
)
 
bucket_name = "wintershoplogs"
 
# --- Connexion à PostgreSQL ---
conn = psycopg2.connect(
    host="51.77.215.42",
    port=6543,
    dbname="wintershop_student",
    user="wintershop",
    password="wintershop"
)
cursor = conn.cursor()
 
# --- Créer la table bronze si elle n'existe pas ---
create_table_query = """
CREATE TABLE IF NOT EXISTS wintershop_prod.bronze (
    id SERIAL PRIMARY KEY,
    log TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW()
);
"""
cursor.execute(create_table_query)
conn.commit()
 
# --- Lister les fichiers dans le bucket ---
response = s3.list_objects_v2(Bucket=bucket_name)
for obj in response.get("Contents", []):
    key = obj["Key"]
    print(f"Ingesting {key} ...")
   
    # Télécharger le fichier
    file_obj = s3.get_object(Bucket=bucket_name, Key=key)
    file_content = file_obj['Body'].read().decode('utf-8')
   
    # Insérer chaque ligne dans la table bronze
    for line in file_content.splitlines():
        line = line.strip()
        if line:
            cursor.execute(
                "INSERT INTO wintershop_prod.bronze (log) VALUES (%s)",
                (line,)
            )
 
# Commit et fermeture
conn.commit()
cursor.close()
conn.close()
 
print("Table bronze créée et tous les logs ingérés depuis MinIO !")