import os
import logging
from datetime import datetime, timezone
from pathlib import Path

from boto3.session import Session
import psycopg2

# =========================
# CONFIG
# =========================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://51.77.215.42:9010")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "studentSDV")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "coucou44")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "wintershoplogs")

PG_HOST = os.getenv("PG_HOST", "51.77.215.42")
PG_PORT = int(os.getenv("PG_PORT", "6543"))
PG_DBNAME = os.getenv("PG_DBNAME", "wintershop_student")
PG_USER = os.getenv("PG_USER", "wintershop")
PG_PASSWORD = os.getenv("PG_PASSWORD", "wintershop")

TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "wintershop_prod")
TARGET_TABLE = os.getenv("TARGET_TABLE", "bronze")

# Optionnel : limiter le nombre de fichiers traités par run (utile si trop de logs)
MAX_FILES_PER_RUN = int(os.getenv("MAX_FILES_PER_RUN", "0"))  # 0 = no limit

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# =========================
# HELPERS
# =========================
def get_s3_client():
    session = Session(
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    return session.client(
        service_name="s3",
        endpoint_url=MINIO_ENDPOINT,
    )


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def ensure_table_and_constraint(cursor):
    # Schema + table
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            id SERIAL PRIMARY KEY,
            log TEXT NOT NULL,
            ingested_at TIMESTAMP DEFAULT NOW(),
            source_file TEXT,
            line_no INT
        );
        """
    )

    # Anti-doublon : unique index (idempotent, simple, robuste)
    # IMPORTANT : échoue si la table contient déjà des doublons (source_file,line_no)
    cursor.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS bronze_uq_source_line_idx
        ON {TARGET_SCHEMA}.{TARGET_TABLE} (source_file, line_no);
        """
    )


def list_all_objects(s3, bucket_name):
    """Pagination-safe list_objects_v2"""
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket_name}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys[-10:]


def is_log_file(key: str) -> bool:
    # Adapte si besoin
    return key.lower().endswith(".log")


def main():
    s3 = get_s3_client()
    conn = get_pg_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cursor:
            ensure_table_and_constraint(cursor)
            conn.commit()

            keys = list_all_objects(s3, MINIO_BUCKET)
            keys = [k for k in keys if is_log_file(k)]
            keys.sort()

            if MAX_FILES_PER_RUN > 0:
                keys = keys[:MAX_FILES_PER_RUN]

            if not keys:
                logging.info("Aucun fichier .log trouvé dans le bucket.")
                return

            # Insert anti-doublon : basé sur (source_file, line_no)
            insert_sql = (
                f"INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (log, source_file, line_no) "
                f"VALUES (%s, %s, %s) "
                f"ON CONFLICT (source_file, line_no) DO NOTHING"
            )

            total_lines = 0
            total_insert_attempts = 0

            for key in keys:
                logging.info(f"Ingesting {key} ...")

                file_obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
                content = file_obj["Body"].read().decode("utf-8", errors="replace")

                # 1-based line numbering
                for i, line in enumerate(content.splitlines(), start=1):
                    line = line.strip()
                    if not line:
                        continue

                    total_lines += 1
                    total_insert_attempts += 1
                    cursor.execute(insert_sql, (line, key, i))

                # Commit par fichier (évite gros rollback si un fichier pose problème)
                conn.commit()

            logging.info(
                f"Terminé. Lignes non vides traitées: {total_lines} | Tentatives d'insert: {total_insert_attempts}"
            )
            logging.info("Table bronze prête, ingestion anti-doublons active.")

    except Exception as e:
        conn.rollback()
        logging.exception(f"Erreur ingestion: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
