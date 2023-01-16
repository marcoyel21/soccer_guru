# SCHEMA FUTURE MATCHES
from google.cloud import bigquery

PROJECT_ID = "soccerguru"
DATASET_ID = "matches"
TABLE_ID = "future"

client = bigquery.Client()

# 1) create table
schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("league_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("season_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("localteam_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("visitorteam_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("winner_team_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("localteam_score", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("visitorteam_score", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("localteam_position", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("visitorteam_position", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("time", "TIME", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
    ]
       
table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=schema)
table = client.create_table(table)
print(
    "Created table {}.{}.{}".format(
        table.project, table.dataset_id, table.table_id
    )
)

