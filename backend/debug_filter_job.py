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
            "datasets": {
                "id": "trade",
                "editions": "time-series",
                "versions": int(version_number)
            },
            "dimensions": [
                {
                    "name": "time",
                    "options": "Oct-2025"
                },
                {
                    "name": "countriesandterritories",
                    "options": "AT"
                }
            ]
    }


    try:
        response = requests.post(filter_url, json=payload)
        response.raise_for_status()
        filter_data = response.json()
        filter_id = filter_data.get('filter_id')
        logger.info(f"Filter job created: {filter_id}")
        return filter_id

    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating filter job: {e}")
        logger.error(f"Response: {response.text if response else 'No response'}")
        return None
    
if __name__ == "__main__":
    version_number = '63'
    filter_id = create_filter(version_number)