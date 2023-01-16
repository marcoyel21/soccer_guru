# SCHEMA HISTORIC
from google.cloud import bigquery

PROJECT_ID = "soccerguru"
DATASET_ID = "matches"
TABLE_ID = "historic"

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
    bigquery.SchemaField(
        "l_passes",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("total", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("accurate", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("percentage", "FLOAT64", mode="NULLABLE")]),
   bigquery.SchemaField(
        "l_attack",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("attacks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("dangerous_attacks", "INT64", mode="NULLABLE")]),
   bigquery.SchemaField(
        "l_shots",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("total", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("ongoal", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("blocked", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("offgoal", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("insidebox", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("outsidebox", "INT64", mode="NULLABLE")]),
    bigquery.SchemaField("l_fouls", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_corners", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_offsides", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_possessiontime", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_yellowcards", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_redcards", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_saves", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_substitutions", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_goal_kick", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_goal_attempts", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_free_kick", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("l_tackles", "INT64", mode="NULLABLE"),

    bigquery.SchemaField(
        "v_passes",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("total", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("accurate", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("percentage", "FLOAT64", mode="NULLABLE")]),
   bigquery.SchemaField(
        "v_attack",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("attacks", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("dangerous_attacks", "INT64", mode="NULLABLE")]),
   bigquery.SchemaField(
        "v_shots",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("total", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("ongoal", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("blocked", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("offgoal", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("insidebox", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("outsidebox", "INT64", mode="NULLABLE")]),
    bigquery.SchemaField("v_fouls", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_corners", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_offsides", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_possessiontime", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_yellowcards", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_redcards", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_saves", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_substitutions", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_goal_kick", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_goal_attempts", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_free_kick", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("v_tackles", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("Y", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("Y_goals", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
]
       
table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=schema)
table = client.create_table(table)
print(
    "Created table {}.{}.{}".format(
        table.project, table.dataset_id, table.table_id
    )
)
