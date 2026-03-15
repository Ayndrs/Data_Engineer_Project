import sqlite3
import pandas as pd
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def load(schema_dataframes: dict, lineage: dict):
    """
        Loads star schema dataframes into the database.

        Parameters:
            schema_dataframes (dict): Dictionary containing 'dim_patient', 'dim_risk_category', and 'fact_measurements' dataframes.
            lineage (dict): Lineage information for the pipeline.

        Returns:
            None
    """
    logger.info("Starting load step for star schema")

    try:
        conn = sqlite3.connect("database/diabetes_data.db")
        logger.info("Connected to SQLite database")

        # Load dimension tables first
        schema_dataframes['dim_patient'].to_sql("dim_patient", conn, if_exists="replace", index=False)
        logger.info(f"Loaded {len(schema_dataframes['dim_patient'])} patients into dim_patient")

        schema_dataframes['dim_risk_category'].to_sql("dim_risk_category", conn, if_exists="replace", index=False)
        logger.info(f"Loaded {len(schema_dataframes['dim_risk_category'])} risk categories into dim_risk_category")

        # Load fact table
        schema_dataframes['fact_measurements'].to_sql("fact_patient_measurements", conn, if_exists="replace", index=False)
        logger.info(f"Loaded {len(schema_dataframes['fact_measurements'])} measurements into fact_patient_measurements")

        # Load lineage
        lineage["end_time"] = datetime.now(timezone.utc).isoformat()
        lineage_df = pd.DataFrame([lineage])
        lineage_df.to_sql("pipeline_lineage", conn, if_exists="append", index=False)
        logger.info("Pipeline lineage recorded")
    except Exception as e:
        logger.error(f"Error during load step: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")
