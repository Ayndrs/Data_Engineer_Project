import pandas as pd
import logging

logger = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
        Transforms raw patient data into a summary format.

        Parameters:
            df (pd.DataFrame): DataFrame containing the raw patient data.

        Returns:
            pd.DataFrame: DataFrame containing the transformed summary data.
    """
    logger.info(f"Starting transformation on {len(df)} records")

    summary = (
        df
        .groupby(["gender", "diabetes_risk_category"])
        .agg(
            count=("Patient_ID", "size"),
            avg_bmi=("bmi", "mean"),
            avg_hba1c=("HbA1c_level", "mean"),
            avg_glucose=("fasting_glucose_level", "mean"),
            avg_calorie_intake=("daily_calorie_intake", "mean"),
            avg_stress_level=("stress_level", "mean"),
            avg_sleep_hours=("sleep_hours", "mean")
        )
        .reset_index()
    )
    summary = summary.round(2)

    logger.info(f"Transformation complete. Generated {len(summary)} summary rows")

    return summary