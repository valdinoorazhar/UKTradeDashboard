import requests
import pandas as pd
from clickhouse_driver import Client
#from datetime import datetime
import logging
import time
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TradeDataPipeline:
    def __init__(self, clickhouse_host='localhost', clickhouse_port=9000, 
                 clickhouse_database='trade_data', clickhouse_user='default', 
                 clickhouse_password=''):
        """Initialize the trade data pipeline."""
        self.ch_client = Client(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_database,
            user=clickhouse_user,
            password=clickhouse_password
        )
        self.base_url = "https://api.beta.ons.gov.uk/v1"  # ONS Beta API v1
        logger.info("Trade pipeline initialized")
    
    def get_latest_version(self, dataset_id, edition):
        """Get the latest version number for a dataset edition."""
        url = f"{self.base_url}/datasets/{dataset_id}/editions/{edition}/versions"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            versions = response.json().get('items', [])
            
            if not versions:
                logger.error(f"No versions found for {dataset_id}/{edition}")
                return None
            
            sorted_versions = sorted(versions, key=lambda x: x.get('version', 0), reverse=True)
            latest_version = sorted_versions[0].get('version')
            
            logger.info(f"Latest version: {latest_version}")
            return str(latest_version)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching versions: {e}")
            return None
    
    def create_filter_job(self, dataset_id, edition, version, dimensions_filter):
        """
        Create a filter job using POST method.
        This is the MOST EFFICIENT way for multi-dimensional queries.
        
        Args:
            dataset_id: Dataset identifier
            edition: Edition name
            version: Version number
            dimensions_filter: Dict with dimension names as keys and list of options as values
                              e.g., {'time': ['2024-01'], 'geography': ['K02000001']}
        
        Returns:
            filter_id: ID of the created filter job
        """
        url = f"{self.base_url}/filters"
        
        payload = {
            "dataset": {
                "id": dataset_id,
                "edition": edition,
                "version": int(version)
            },
            "dimensions": [
                {
                    "name": dim_name,
                    "options": options
                }
                for dim_name, options in dimensions_filter.items()
            ]
        }
        
        try:
            logger.info(f"Creating filter job with dimensions: {list(dimensions_filter.keys())}")
            logger.info(f"Payload: {payload}")
            response = requests.post(url, json=payload, params={"submitted": "true"})
            response.raise_for_status()
            
            filter_data = response.json()
            filter_id = filter_data.get('filter_id')
            filter_output_id = filter_data.get('links', {}).get('filter_output', {}).get('id')
            logger.info(f"Filter job created: {filter_id}")

            #debug filter data
            with open('debug_filter_data.txt', 'w') as txt_file:
                txt_file.write(json.dumps(filter_data, indent=4))
            
            return filter_output_id  # Return filter_output_id for direct use in submission
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating filter job: {e}")
            logger.error(f"Response: {response.text if response else 'No response'}")
            return None
    
    def submit_filter_job(self, filter_id, dataset_id, edition, version, dimensions_filter):
        """
        Submit the filter job for processing using PUT request.
        This will return the filter_output_id needed for downloading.
        """
        url = f"{self.base_url}/filters/{filter_id}"
        
        payload = {
            "dataset": {
                "id": dataset_id,
                "edition": edition,
                "version": int(version)
            },
            "dimensions": [
                {
                    "name": dim_name,
                    "options": options
                }
                for dim_name, options in dimensions_filter.items()
            ]
        }
        
        try:
            # Important: Use get with submitted=true query parameter
            response = requests.get(url, json=payload, params={"submitted": "true"})
            response.raise_for_status()
            
            job_data = response.json()
            
            # Extract filter_output_id from response
            filter_output_id = job_data.get('links', {}).get('filter_output', {}).get('id')
            
            if filter_output_id:
                logger.info(f"Filter job submitted. Filter output ID: {filter_output_id}")
            else:
                logger.warning("Filter output ID not found in response")
            
            return job_data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error submitting filter job: {e}")
            logger.error(f"Response: {response.text if response else 'No response'}")
            return None
    
    def wait_for_filter_job(self, filter_output_id, max_wait_seconds=300, check_interval=5):
        """Wait for filter output to be ready."""
        url = f"{self.base_url}/filter-outputs/{filter_output_id}"
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                output_data = response.json()
                state = output_data.get('state', 'unknown')
                
                logger.info(f"Filter output state: {state}")
                
                if state == 'completed':
                    return output_data
                elif state == 'failed':
                    logger.error("Filter output failed")
                    return None
                
                time.sleep(check_interval)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error checking filter output: {e}")
                return None
        
        logger.error("Filter output timeout")
        return None
    
    def download_filter_results(self, filter_output_id):
        """Download the CSV results from completed filter output."""
        url = f"{self.base_url}/filter-outputs/{filter_output_id}"
        
        try:
            # Get the filter output details
            response = requests.get(url)
            response.raise_for_status()
            
            output_data = json.loads(response.text)
            downloads = output_data.get('downloads', {})
            csv_info = downloads.get('csv', {})
            with open('debug_output_data.txt', 'w') as txt_file:
                txt_file.write(json.dumps(output_data, indent=4))
            
            
            # csv url
            csv_url = csv_info.get('href')
            
            if not csv_url:
                logger.error("No CSV download URL found")
                logger.debug(f"Available downloads: {downloads}")
                return None
            
            logger.info(f"Downloading from: {csv_url}")
            csv_response = requests.get(csv_url)
            csv_response.raise_for_status()
            
            # Parse CSV into DataFrame
            from io import StringIO
            df = pd.read_csv(StringIO(csv_response.text))
            logger.info(f"Downloaded {len(df)} rows")
            
            return df
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading results: {e}")
            return None
    
    def get_dimension_options(self, dataset_id, edition, version, dimension_name):
        """Get all available options for a dimension."""
        url = f"{self.base_url}/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension_name}/options"
        
        try:
            response = requests.get(url, params={'limit': 1000})
            response.raise_for_status()
            
            options_data = response.json()
            options = [opt.get('option') for opt in options_data.get('items', [])]
            logger.info(f"Found {len(options)} options for dimension '{dimension_name}'")
            
            return options
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching dimension options: {e}")
            return []
    
    def create_table_for_trade(self, table_name):
        """Create ClickHouse table optimized for trade data."""
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            time String,
            geography String,
            sitc String,
            country String,
            direction String,
            value Float64,
            ingestion_timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(toDate(time))
        ORDER BY (time, geography, sitc, country, direction)
        """
        
        try:
            self.ch_client.execute(create_query)
            logger.info(f"Table {table_name} created/verified")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def ingest_to_clickhouse(self, df, table_name):
        """Insert trade data into ClickHouse."""
        if df is None or df.empty:
            logger.warning("No data to ingest")
            return
        
        # Create table
        self.create_table_for_trade(table_name)
        
        # Prepare data for insertion
        columns = list(df.columns)
        data_to_insert = df.values.tolist()
        
        try:
            self.ch_client.execute(
                f"INSERT INTO {table_name} ({','.join(columns)}) VALUES",
                data_to_insert
            )
            logger.info(f"Inserted {len(data_to_insert)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise
    
    def run_monthly_batch(self, dataset_id, edition, target_month, table_name='trade_data'):
        """
        MOST EFFICIENT METHOD: Run monthly batch using filter API.
        This method filters by month and retrieves ALL combinations of other dimensions.
        
        Args:
            dataset_id: e.g., 'trade'
            edition: e.g., 'time-series'
            target_month: e.g., '2024-01' or ['2024-01', '2024-02']
            table_name: Target ClickHouse table
        """
        logger.info(f"Starting monthly batch for {target_month}")
        
        # Get latest version
        version = self.get_latest_version(dataset_id, edition)
        if not version:
            return
        
        # Prepare filter dimensions
        # Only filter by time dimension, leave others unfiltered (get all options)
        if isinstance(target_month, str):
            target_month = [target_month]
        
        dimensions_filter = {
            'time': target_month
            # DO NOT include other dimensions - they will all be returned
        }
        
        # Create and submit filter job
        filter_id = self.create_filter_job(dataset_id, edition, version, dimensions_filter)
        if not filter_id:
            return
        
        # Submit job with PUT request to get filter_output_id
        job_data = self.submit_filter_job(filter_id, dataset_id, edition, version, dimensions_filter)
        if not job_data:
            return
        
        # Extract filter_output_id from the response
        filter_output_id = job_data.get('links', {}).get('filter_output', {}).get('id')
        if not filter_output_id:
            logger.error("Could not extract filter_output_id from response")
            return
        
        logger.info(f"Filter output ID: {filter_output_id}")
        
        # Wait for completion
        completed_data = self.wait_for_filter_job(filter_output_id)
        if not completed_data:
            return
        
        # Download results
        df = self.download_filter_results(filter_output_id)
        if df is not None:
            # Ingest into ClickHouse
            self.ingest_to_clickhouse(df, table_name)
            logger.info("Monthly batch completed successfully")
    
    def close(self):
        """Close ClickHouse connection."""
        self.ch_client.disconnect()
        logger.info("Connection closed")


# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = TradeDataPipeline(
        clickhouse_host='localhost',
        clickhouse_port=18123,
        clickhouse_database='STG_ONS',
        clickhouse_user='default',
        clickhouse_password='changeme'
    )

    target_month = 'Dec-25'  # or ['2024-01', '2024-02'] for multiple months
    dataset_id = 'trade'
    edition = 'time-series'
    
    logger.info(f"Starting monthly batch for {target_month}")
        
    # Get latest version
    version = pipeline.get_latest_version(dataset_id, edition)
    if not version:
        exit()
    
    # Prepare filter dimensions
    # Only filter by time dimension, leave others unfiltered (get all options)
    if isinstance(target_month, str):
        target_month = [target_month]
    
    dimensions_filter = {
        'time': target_month
        # DO NOT include other dimensions - they will all be returned
    }
    
    # Create and submit filter job
    filter_output_id = pipeline.create_filter_job(dataset_id, edition, version, dimensions_filter)
    if not filter_output_id:
        exit()
    
    
    logger.info(f"Filter output ID: {filter_output_id}")

    dataframe = pipeline.download_filter_results(filter_output_id)
    #if not dataframe:
    #    exit()

    print(dataframe.head(10))

    # Run monthly batch - THIS IS THE MOST EFFICIENT WAY
    # It filters by month and automatically gets ALL combinations of:
    # - geography
    # - standardindustrialtradeclassification (SITC)
    # - countriesandterritories
    # - direction
    #pipeline.run_monthly_batch(
    #    dataset_id='your-trade-dataset-id',
    #    edition='time-series',
    #    target_month='2024-01',  # or ['2024-01', '2024-02'] for multiple months
    #    table_name='trade_data'
    #)
    
    pipeline.close()