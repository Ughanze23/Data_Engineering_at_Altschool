import requests
import json



def get_request_json(url) -> str:
    """ "Fetches data from a URL using a GET request.

    Returns:
    str: Returns a string representaion of the get request json response
    """
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    return json.dumps(data)

def get_request_jsonl(url) -> str:
    """ "Fetches data from a URL using a GET request.

    Returns:
    str: Returns a line-delimited JSON of the get request json response
    """
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    return '\n'.join([json.dumps(record) for record in data])
