import requests
from openai import AzureOpenAI
from datetime import  timedelta, timezone, datetime
from azure.data.tables import TableServiceClient
from azure.data.tables import TableServiceClient, UpdateMode
import logging
import json

def get_cur_CI(EM_KEY):
    url = "https://api.electricitymaps.com/v3/carbon-intensity/latest?dataCenterRegion=eastus2&dataCenterProvider=azure&disableEstimations=true&emissionFactorType=direct"
    headers={"auth-token": EM_KEY}
    response = requests.get(url,headers=headers)
    response.raise_for_status()

    cur_CI = response.json()["carbonIntensity"]
    cur_zone = response.json()["zone"]
    timestamp = response.json()["datetime"]

    return cur_CI, cur_zone, timestamp

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
                "content": "You are a helpful assistant. The response will be embedded into an HTML div, so make sure you provide HTML formatted prompts. Bootstrap 5 is used, so feel free to use that formatting to prettify. Do not mention anything about formatting.",
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

def get_bin(ci_old, cur_CI, DEPLOYMENT_STORAGE_CONNECTION_STRING):
    table_name  = "carbonintensities"
    table_client = TableServiceClient.from_connection_string(DEPLOYMENT_STORAGE_CONNECTION_STRING).get_table_client(table_name)
    
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")

    query = f"Timestamp ge datetime'{cutoff_str}'"

    entities = table_client.query_entities(query)
    rows = [dict(e) for e in entities]
    CIs = [float(row['CI']) for row in rows]
    min_ci = min(CIs)
    max_ci = max(CIs)

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



def execute(entity, cur_CI, table_client, model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY):
    response = use_llm(model, prompt_text, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY)

    response_text = response.choices[0].message.content
    out_tokens = response.usage.completion_tokens

    entity["Status"] = "completed"
    entity["CompletedAt"] = datetime.now().isoformat()
    entity["Response"] = response_text
    entity["CarbonIntensity_c"] = cur_CI
    entity["OutTokens"] = out_tokens

    table_client.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
    
