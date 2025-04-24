import os
from dotenv import load_dotenv
import pandas_gbq
import logging
from transform import transform_data

# Set up logging
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_to_bigquery(df):
    """
    Load DataFrame to BigQuery table.

    Args:
        df: DataFrame containing Billboard and Spotify data

    Returns:
      if it was successful or not.
    """
    try:
        # Load environment variables
        load_dotenv()
        project_id = os.getenv('PROJECT_ID')

        if not project_id:
            raise ValueError("PROJECT_ID not found in .env file")

        # BigQuery destination configuration
        destination_table = "billboard_spotify.billboard_spotify_table"

        # Define schema for BigQuery
        schema = [
            {'name': 'CHART_DATE', 'type': 'DATE'},
            {'name': 'RANK', 'type': 'INTEGER'},
            {'name': 'TITLE', 'type': 'STRING'},
            {'name': 'ARTIST', 'type': 'STRING'},
            {'name': 'ALBUM', 'type': 'STRING'},
            {'name': 'RELEASE_DATE', 'type': 'DATE'},
            {'name': 'POPULARITY', 'type': 'INTEGER'},
            {'name': 'SPOTIFY_ID', 'type': 'STRING'},
            {'name': 'AGE_OF_SONG', 'type': 'INTEGER'}
        ]

        # Load to BigQuery
        pandas_gbq.to_gbq(
            df,
            destination_table=destination_table,
            project_id=project_id,
            if_exists='append',
            table_schema=schema
        )

        logging.info(f"Successfully loaded {len(df)} rows to BigQuery: {destination_table}")
        return True

    except Exception as e:
        logging.error(f"Error loading to BigQuery: {str(e)}")
        return False

#Example Usage
if __name__ == "__main__":
    df = transform_data()
    if df is not None:
        load_to_bigquery(df)
