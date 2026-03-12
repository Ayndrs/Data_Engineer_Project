import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .groupby(["gender", "diabetes_risk_category"])
        .agg(
            count=("Patient_ID", "size"),
            avg_bmi=("bmi", "mean"),
            avg_hba1c=("HbA1c_level", "mean"),
            avg_glucose=("fasting_glucose_level", "mean"),
            avg_calorie_intake=("daily_calorie_intake", "mean"),
            avg_stress_level=("stress_level", "mean"),
        )
        .reset_index()
    )
