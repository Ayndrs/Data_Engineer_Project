import sqlite3
import pandas as pd
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def load(summary: pd.DataFrame, lineage: dict):
    """
        Loads transformed data into a SQLite database.

        Parameters:
            summary (pd.DataFrame): Transformed summary data.
            lineage (dict): Lineage information for the pipeline.

        Returns:
            pd.DataFrame: DataFrame containing the raw data.
    """
    logger.info("Starting load step")

    try:
        conn = sqlite3.connect("database/diabetes_data.db")
        logger.info("Connected to SQLite database")

        summary.to_sql("diabetes_summary", conn, if_exists="replace", index=False)
        logger.info(f"Inserted {len(summary)} rows into 'diabetes_summary'")

        lineage["end_time"] = datetime.now(timezone.utc)
        lineage_df = pd.DataFrame([lineage])
        lineage_df.to_sql("pipeline_lineage", conn, if_exists="append", index=False)
        logger.info("Pipeline lineage recorded")

    except Exception as e:
        logger.error(f"Error during load step: {e}")
        raise

    finally:
        conn.close()
        logger.info("Database connection closed")