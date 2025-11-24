import logging
import json
import azure.functions as func
from azure.data.tables import TableServiceClient
import os
from utils import get_cur_CI
import uuid

def main(mytimer: func.TimerRequest) -> None:
    AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
    AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
    DEPLOYMENT_STORAGE_CONNECTION_STRING = os.environ.get("DEPLOYMENT_STORAGE_CONNECTION_STRING")
    ELECTRICITY_MAPS_API_KEY = os.environ.get("ELECTRICITY_MAPS_API_KEY")

    if not AZURE_OPENAI_ENDPOINT:
        return func.HttpResponse(
            body=json.dumps({"error": "AZ_OPENAI_ENDPOINT not configured"}),
            status_code=500,
            mimetype="application/json"
        )
    
    if not AZURE_OPENAI_KEY:
        return func.HttpResponse(
            body=json.dumps({"error": "AZ_OPENAI_KEY not configured"}),
            status_code=500,
            mimetype="application/json"
        )
    
    if not DEPLOYMENT_STORAGE_CONNECTION_STRING:
        return func.HttpResponse(
            body=json.dumps({"error": "DEPLOYMENT_STORAGE_CONNECTION_STRING not configured"}),
            status_code=500,
            mimetype="application/json"
        )
    
    if not ELECTRICITY_MAPS_API_KEY:
        return func.HttpResponse(
            body=json.dumps({"error": "ELECTRICITY_MAPS_API_KEY not configured"}),
            status_code=500,
            mimetype="application/json"
        )

    # Lastly, store the CI in a table
    cur_CI, cur_zone, timestamp = get_cur_CI(ELECTRICITY_MAPS_API_KEY)

    table_name  = "carbonintensities"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)

    data = {
        "PartitionKey": timestamp, # Effectively table name
        "RowKey": str(uuid.uuid4()), # Generates a key 
        "Zone": cur_zone,
        "CI": cur_CI,
    }

    table_client.create_entity(entity=data)

    logging.info('Python timer trigger executed successfully.')
