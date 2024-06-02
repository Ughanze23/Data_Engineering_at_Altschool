import requests
import json
import config as c


def get_request(url) -> str:
    """ "Fetches data from a URL using a GET request and returns it as a JSON string.

    Returns:
    str: Returns a string representaion of the get request json response
    """
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    return json.dumps(data)
