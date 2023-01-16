from google.cloud import bigquery
def delete_model(model_id):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_model(model_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(model_id))

def delete_table(table_id):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id)) 

def deploy_model(query):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # Start the query, passing in the extra configuration.
    query_job = client.query(query)  # Make an API request.
    query_job.result()  
    print("Model deployed")
    
def predict_next_week(query):
    client = bigquery.Client()
    query_job = client.query(query)
    return query_job

def process_data(query,table_id):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # TODO(developer): Set table_id to the ID of the destination table.
    table_id = "soccerguru.matches.processed_data"
    job_config = bigquery.QueryJobConfig(destination=table_id)
    # Start the query, passing in the extra configuration.
    query_job = client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    print("Query results loaded to the table {}".format(table_id))