import requests
from openai import AzureOpenAI
from datetime import  timedelta, timezone, datetime
from azure.data.tables import TableServiceClient
from azure.data.tables import TableServiceClient, UpdateMode
import logging
import math

# Gets latest Carbon Intensity from Electricity Maps 
def get_cur_CI(EM_KEY):
    url = "https://api.electricitymaps.com/v3/carbon-intensity/latest?dataCenterRegion=eastus2&dataCenterProvider=azure&disableEstimations=true&emissionFactorType=direct"
    headers={"auth-token": EM_KEY}
    response = requests.get(url,headers=headers)
    response.raise_for_status()

    cur_CI = response.json()["carbonIntensity"]
    cur_zone = response.json()["zone"]
    timestamp = response.json()["datetime"]

    return cur_CI, cur_zone, timestamp

# Queries Az Open AI for llm response
def use_llm(model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY):
    api_version = "2024-12-01-preview"
    deployment = model

    client = AzureOpenAI(
        api_version=api_version,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_KEY,
    )

    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant. The response will be embedded into an HTML div, so make sure you provide HTML formatted prompts. Bootstrap 5 is used, so feel free to use that formatting to prettify. Do not prettify for very simple short text responses. Do not mention anything about formatting you did.",
            },
            {
                "role": "user",
                "content": prompt_text,
            }
        ],
        max_tokens=1000,
        temperature=1.0,
        top_p=1.0,
        model=deployment
    )

    return response

# Gets carbon intensity history for the past 7 days
def get_ci_history(DEPLOYMENT_STORAGE_CONNECTION_STRING):
    table_name  = "carbonintensities"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)
    
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")
    
    query = (
        f"PartitionKey eq 'ci' and "
        f"Timestamp ge datetime'{cutoff_str}'"
    )
    
    entities = table_client.query_entities(query)
    rows = [
        {
            "CI": e["CI"],
            "Timestamp": e.metadata["timestamp"],
        }
        for e in entities
    ]

    CIs_all = [row['CI'] for row in rows]
    
    rows_sorted = sorted(rows, key=lambda x: x['Timestamp'])
    CIs_latest = [row["CI"] for row in rows_sorted[-3:]]
    
    return CIs_all, CIs_latest
# Gets last weeks hourly carbon intensity data, and splits it into bins 0-5
# Returns what bin old and new CI belong to 
def get_bin(ci_old, cur_CI, CIs):


    min_ci = min(CIs)
    max_ci = max(CIs)

    logging.info(f'Min: {min_ci}, Max: {max_ci}')

    # For 6 bins
    bin_width = (max_ci - min_ci) / 5

    if ci_old >= max_ci:
        old = 5 
    elif ci_old <= min_ci:
        old =  0
    else:
        old = int((ci_old - min_ci) / bin_width + 1)

    if cur_CI >= max_ci:
        new = 5 
    elif cur_CI <= min_ci:
        new =  0
    else:
        new = int((cur_CI - min_ci) / bin_width + 1)

    return old, new

def get_execution_probability(bin_old, bin_new, recent_CIs, time_remaining_hours):
    benefit = bin_old - bin_new
    
    # Don't execute with no benefit
    if benefit <= 0:
        return 0.0
    
    base_prob = 1 / (1 + math.exp(-2 * (benefit - 3)))
    
    # Force high benefits
    if benefit >= 4:
        base_prob = 1.0

    # Urgency factor
    urgency_factor = 0.15 + 2.15 / (
        1 + math.exp(0.5 * (time_remaining_hours - 6))
    )
    
    # Trend adjustment
    if len(recent_CIs) >= 2:
        ci_trend = recent_CIs[-2] - recent_CIs[-1]  # positive = CI dropping (good)
        
        if ci_trend > 0 and benefit < 4:
            urgency_factor *= 0.1  # Be more patient
        elif ci_trend < -5 and benefit >= 2:
            # CI is rising - execute now before it gets worse
            urgency_factor *= 40.0  # Be more aggressive
    
    final_prob = min(1.0, base_prob * urgency_factor)
    return final_prob

# Run the prompt, and update the prompt table with response
def execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, final_prob):
    response = use_llm(model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY)

    response_text = response.choices[0].message.content
    out_tokens = response.usage.completion_tokens

    entity["Status"] = "completed"
    entity["CompletedAt"] = datetime.now(timezone.utc).isoformat()
    entity["Response"] = response_text
    entity["CarbonIntensity_c"] = cur_CI
    entity["OutTokens"] = out_tokens

    table_client.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
    
    logging.info(f"Prompt completed: {str(prompt_text)} at probability: {str(final_prob)}")
