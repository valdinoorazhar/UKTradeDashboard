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
    output_url = f"https://api.beta.ons.gov.uk/v1/filters/{filter_id}"
    try:
        response = requests.get(output_url)
        response.raise_for_status()
        output_data = response.json()
        logger.info(f"Filter output retrieved for filter ID: {filter_id}")
        return output_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error retrieving filter output: {e}")
        logger.error(f"Response: {response.text if response else 'No response'}")
        return None
    
if __name__ == "__main__":
    filter_id = 'f63bb50f-7cad-447a-9690-99a41abeaebb'
    filter_output = get_filter_output(filter_id)