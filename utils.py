import requests
from openai import AzureOpenAI

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
