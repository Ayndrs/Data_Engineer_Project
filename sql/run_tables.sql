-- Star Schema for Diabetes Risk Data Warehouse
-- Fact table for patient measurements
CREATE TABLE IF NOT EXISTS fact_patient_measurements (
    measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_id INTEGER NOT NULL,
    risk_category_id INTEGER NOT NULL,
    bmi REAL,
    blood_pressure INTEGER,
    fasting_glucose_level INTEGER,
    insulin_level REAL,
    HbA1c_level REAL,
    cholesterol_level INTEGER,
    triglycerides_level INTEGER,
    physical_activity_level TEXT,
    daily_calorie_intake INTEGER,
    sugar_intake_grams_per_day REAL,
    sleep_hours REAL,
    stress_level INTEGER,
    waist_circumference_cm REAL,
    diabetes_risk_score REAL,
    FOREIGN KEY (patient_id) REFERENCES dim_patient (patient_key),
    FOREIGN KEY (risk_category_id) REFERENCES dim_risk_category (risk_category_key)
);

-- Dimension table for patient information
CREATE TABLE IF NOT EXISTS dim_patient (
    patient_key INTEGER PRIMARY KEY AUTOINCREMENT,
    Patient_ID TEXT UNIQUE NOT NULL,
    age INTEGER,
    gender TEXT,
    family_history_diabetes TEXT
);

-- Dimension table for risk categories
CREATE TABLE IF NOT EXISTS dim_risk_category (
    risk_category_key INTEGER PRIMARY KEY AUTOINCREMENT,
    diabetes_risk_category TEXT UNIQUE NOT NULL
);

-- Pipeline lineage table (keeping existing structure)
CREATE TABLE IF NOT EXISTS pipeline_lineage (
    run_id TEXT,
    source_file TEXT,
    start_time TEXT,
    end_time TEXT,
    rows_extracted INTEGER,
    rows_after_transform INTEGER
);

-- Sample queries for the star schema
.headers on
.mode column

-- Max calories by age group
SELECT MAX(f.daily_calorie_intake) as max_calories, p.age
FROM fact_patient_measurements f
JOIN dim_patient p ON f.patient_id = p.patient_key
WHERE p.age > 50;

-- Average BMI by risk category
SELECT AVG(f.bmi) as avg_bmi, rc.diabetes_risk_category
FROM fact_patient_measurements f
JOIN dim_risk_category rc ON f.risk_category_id = rc.risk_category_key
GROUP BY rc.diabetes_risk_category;

-- Count of patients by gender and risk category
SELECT COUNT(*) as patient_count, p.gender, rc.diabetes_risk_category
FROM fact_patient_measurements f
JOIN dim_patient p ON f.patient_id = p.patient_key
JOIN dim_risk_category rc ON f.risk_category_id = rc.risk_category_key
GROUP BY p.gender, rc.diabetes_risk_category;
