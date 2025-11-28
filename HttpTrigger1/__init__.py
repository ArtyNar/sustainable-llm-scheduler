import logging
import json
import azure.functions as func
from azure.data.tables import TableServiceClient
import os
from utils import get_cur_CI, get_bin, execute, get_execution_probability
import uuid
from datetime import datetime, timezone
import random

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    # Ensure Env Variables are all set
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
    
    # Get latest carbon intensity
    cur_CI, _, _ = get_cur_CI(ELECTRICITY_MAPS_API_KEY)

    table_name  = "prompttable"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)

    entities = table_client.query_entities(
        query_filter="Status eq 'pending'"
    )

    try:
        for entity in entities:
            model = entity["Model"]
            prompt_text = entity["Prompt"]
            expirationDate = datetime.fromisoformat(entity['expirationDate'])
            CI_old = entity['CarbonIntensity_s'].value
            now = datetime.now(timezone.utc)
            
            # Time remaining
            delta = expirationDate - now
            remaining_hours = delta.total_seconds() / 3600

            # Get bins for current and past CI (0-5)
            bin_old, bin_new = get_bin(CI_old, cur_CI, DEPLOYMENT_STORAGE_CONNECTION_STRING)

            # If scheduler failed to execute in time, execute 
            if now > expirationDate:
                execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY)
           
            # If carbon intensity is very low, execute
            elif bin_new == 0: 
                execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY)
            
            else:
                prob = get_execution_probability(bin_old, bin_new, remaining_hours)

                if random.random() < prob:
                    execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY)
                
    except Exception as e:
        logging.error(f"Something went wrong: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": f"Something went wrong: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )
    
    return func.HttpResponse(
            json.dumps({"success": f"Everything went well"}),
            status_code=200
    )
