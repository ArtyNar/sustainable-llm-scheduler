import logging
import json
import azure.functions as func
from azure.data.tables import TableServiceClient
import os
from utils import get_cur_CI, get_bin, execute, get_execution_probability, get_ci_history
import uuid
from datetime import datetime, timezone
import random
from datetime import  timedelta, timezone, datetime

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
    cur_CI, cur_zone, _ = get_cur_CI(ELECTRICITY_MAPS_API_KEY)
    
    # Get last weeks history for binning and rate of change information
    CIs_all, CIs_latest = get_ci_history(DEPLOYMENT_STORAGE_CONNECTION_STRING)

    # Access prompt table
    try:
        table_name  = "prompttable"
        table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)

        entities = table_client.query_entities(
            query_filter="Status eq 'pending'" # Filter on pending only
        )

    except Exception as e:
        logging.error(f"Something went wrong accessing the prompt table: {e}")
    
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
            bin_old, bin_new = get_bin(CI_old, cur_CI, CIs_all)
            execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, 5)

            # # If scheduler failed to execute in time, execute 
            # if now > expirationDate:
            #     logging.info('Prompt expired.')
            #     execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, -1)
            #     continue
            # # If carbon intensity is very low, execute
            # elif bin_new == 0 or bin_new == 1: 
            #     execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, bin_new)
            
            # else:
            #     prob = get_execution_probability(bin_old, bin_new, CIs_latest, remaining_hours)
            #     r = random.random()
            #     if r < prob:
            #         logging.info(f'Rand: {r}')

            #         execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, prob)
                
    except Exception as e:
        logging.error(f"Something went wrong with the scheduler: {e}")


    table_name  = "prompttable"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)
        
    entities = table_client.query_entities(
        query_filter="Status eq 'completed'"
    )

    # rows = [dict(e) for e in entities]

    table_name  = "carbonintensities"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)
    
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")
    
    # query = (
    #     f"PartitionKey eq 'ci' and "
    #     f"Timestamp ge datetime'{cutoff_str}'"
    # )

    query = (
        f"PartitionKey eq 'ci' "
    )

    entities = table_client.query_entities(query)
    rows = []
    for e in entities:
        ts = e.metadata["timestamp"]
        rows.append({
            "CI": e.get("CI"),
            "Timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
        })


    return func.HttpResponse(
        json.dumps(rows),
        status_code=200
    )
