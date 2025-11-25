import logging
import json
import azure.functions as func
from azure.data.tables import TableServiceClient, UpdateMode
import os
from utils import get_cur_CI
import uuid
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
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
    
    cur_CI, cur_zone, timestamp = get_cur_CI(ELECTRICITY_MAPS_API_KEY)

    table_name  = "prompttable"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)

    entities = table_client.query_entities(
        query_filter="Status eq 'pending'"
    )

    for entity in entities:
        try:
            entity["Status"] = "completed"
            entity["CompletedAt"] = datetime.now().isoformat()
            entity["Response"] = "Test Test"
            entity["carbonIntensity_C"] = cur_CI

            table_client.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
            
        except Exception as e:
            logging.error(f"Error updating entity {entity.get('RowKey')}: {e}")
            return func.HttpResponse(
                body=json.dumps({"error": f"Failed to update entity: {str(e)}"}),
                status_code=500,
                mimetype="application/json"
            )
    # Lastly, store the CI in a table
    # table_name  = "carbonintensities"
    # table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)

    # data = {
    #     "PartitionKey": timestamp, # Effectively table name
    #     "RowKey": str(uuid.uuid4()), # Generates a key 
    #     "Zone": cur_zone,
    #     "CI": cur_CI,
    # }

    # table_client.create_entity(entity=data)

    rows = [dict(e) for e in entities]
    return func.HttpResponse(
            json.dumps(rows),
            status_code=200
    )
