import pandas as pd
import logging

logger = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> dict:
    """
        Transforms raw patient data into star schema dataframes.

        Parameters:
            df (pd.DataFrame): DataFrame containing the raw patient data.

        Returns:
            dict: Dictionary containing 'dim_patient', 'dim_risk_category', and 'fact_measurements' dataframes.
    """
    logger.info(f"Starting transformation on {len(df)} records into star schema")

    # Create dimension tables
    dim_patient = create_dim_patient(df)
    dim_risk_category = create_dim_risk_category(df)
    fact_measurements = create_fact_measurements(df, dim_patient, dim_risk_category)

    logger.info(f"Transformation complete. Created {len(dim_patient)} patients, {len(dim_risk_category)} risk categories, {len(fact_measurements)} measurements")

    return {
        'dim_patient': dim_patient,
        'dim_risk_category': dim_risk_category,
        'fact_measurements': fact_measurements
    }


def create_dim_patient(df: pd.DataFrame) -> pd.DataFrame:
    """Create patient dimension dataframe."""
    patient_df = df[['Patient_ID', 'age', 'gender', 'family_history_diabetes']].drop_duplicates()
    patient_df['patient_key'] = range(1, len(patient_df) + 1)
    patient_df = patient_df[['patient_key', 'Patient_ID', 'age', 'gender', 'family_history_diabetes']]
    return patient_df


def create_dim_risk_category(df: pd.DataFrame) -> pd.DataFrame:
    """Create risk category dimension dataframe."""
    risk_df = df[['diabetes_risk_category']].drop_duplicates()
    risk_df['risk_category_key'] = range(1, len(risk_df) + 1)
    risk_df = risk_df[['risk_category_key', 'diabetes_risk_category']]
    return risk_df


def create_fact_measurements(df: pd.DataFrame, dim_patient: pd.DataFrame, dim_risk_category: pd.DataFrame) -> pd.DataFrame:
    """Create fact measurements dataframe."""
    # Create mapping dictionaries
    patient_map = dict(zip(dim_patient['Patient_ID'], dim_patient['patient_key']))
    risk_map = dict(zip(dim_risk_category['diabetes_risk_category'], dim_risk_category['risk_category_key']))

    # Prepare fact table data
    fact_df = df.copy()
    fact_df['patient_id'] = fact_df['Patient_ID'].map(patient_map)
    fact_df['risk_category_id'] = fact_df['diabetes_risk_category'].map(risk_map)

    # Select only the columns for the fact table
    fact_columns = [
        'patient_id', 'risk_category_id', 'bmi', 'blood_pressure', 'fasting_glucose_level',
        'insulin_level', 'HbA1c_level', 'cholesterol_level', 'triglycerides_level',
        'physical_activity_level', 'daily_calorie_intake', 'sugar_intake_grams_per_day',
        'sleep_hours', 'stress_level', 'waist_circumference_cm', 'diabetes_risk_score'
    ]
    fact_df = fact_df[fact_columns]

    return fact_df
