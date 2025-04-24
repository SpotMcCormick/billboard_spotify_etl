import logging
from transform import transform_data
from load import load_to_bigquery

# Set up logging
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def run_etl():
    """
    Execute the full ETL pipeline:
    1. Transform data from Billboard and Spotify
    2. Load transformed data to BigQuery
    """
    try:
        logging.info("Starting ETL process...")

        # Transform data
        logging.info("Starting data transformation...")
        df = transform_data()

        if df is not None and not df.empty:
            logging.info(f"Transformation complete. DataFrame shape: {df.shape}")

            # Load to BigQuery
            logging.info("Starting BigQuery load...")
            if load_to_bigquery(df):
                logging.info("ETL process completed successfully ✨")
                return True
            else:
                logging.error("Failed to load data to BigQuery")
                return False
        else:
            logging.error("Transform returned None or empty DataFrame")
            return False

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        return False


# Example Usage
if __name__ == "__main__":
    run_etl()
