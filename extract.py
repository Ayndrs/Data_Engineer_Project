import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract(file_path: str) -> pd.DataFrame:
    """
        Extracts data from a CSV file.

        Parameters:
            file_path (str): Path to the CSV file.

        Returns:
            pd.DataFrame: DataFrame containing the raw data.
    """
    logger.info(f"Starting data extraction from {file_path}")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Failed to extract data from {file_path}: {e}")
        raise
