import requests
import json
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s') 
logger = logging.getLogger(__name__)

def get_filter_output(filter_id):
    output_url = f"https://api.beta.ons.gov.uk/v1/filter-outputs/{filter_id}"
    try:
        response = requests.get(
            url = output_url
            #, params={"submitted": "true"}
            #, data = json.dumps(payload)
            ,# headers = {"If-Match": "*"}
        )
        response.raise_for_status()
        output_data = json.loads(response.text)
        logger.info(f"Filter output retrieved for filter ID: {filter_id}")
        return output_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error retrieving filter output: {e}")
        logger.error(f"Response: {response.text if response else 'No response'}")
        return None
    
if __name__ == "__main__":
    filter_id = 'ccdc9768-3a46-48af-ae5d-693dcbbfdb38'
    filter_output = get_filter_output(filter_id)
    with open('filter_output_debug.txt', 'w') as txt_file:
        txt_file.write(json.dumps(filter_output, indent=4))