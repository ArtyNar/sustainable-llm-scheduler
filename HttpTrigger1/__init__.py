import logging
import json
import azure.functions as func
from azure.data.tables import TableServiceClient
import os

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
    

    tabl_neame = "prompttable"

    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(tabl_neame)

    entities = table_client.list_entities()
    rows = [dict(e) for e in entities]
    
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             json.dumps(rows),
             status_code=200
        )
