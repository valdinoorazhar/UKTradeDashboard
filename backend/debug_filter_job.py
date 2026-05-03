import requests
import json
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_filter(version_number):
# create filter function from ONS
    filter_url = "https://api.beta.ons.gov.uk/v1/filters"

    payload = {
            "dataset": {
                "id": "trade",
                "edition": "time-series",
                "version": version_number
            },
            "dimensions": [
                {
                    "name": "time",
                    "options": ["Oct-25"]
                },
                {
                    "name": "countriesandterritories",
                    "options": ["AT"]
                }
            ]
    }


    try:
        response = requests.post(filter_url
            , json=payload
            , params={"submitted": "true"}
        )
        response.raise_for_status()
        filter_data = response.json()
        filter_id = filter_data.get('filter_id')
        logger.info(f"Filter job created: {filter_id}")
        return filter_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating filter job: {e}")
        logger.error(f"Response: {response.text if response else 'No response'}")
        return None
    
if __name__ == "__main__":
    version_number = 65
    filter_output = create_filter(version_number)
    with open('filter_job_debug.txt', 'w') as txt_file:
        txt_file.write(json.dumps(filter_output, indent=4))