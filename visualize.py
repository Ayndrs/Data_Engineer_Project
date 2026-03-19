import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Create graphs directory if it doesn't exist
graphs_dir = Path('./graphs')
graphs_dir.mkdir(exist_ok=True)

def create_visualizations():
    """Create various visualizations from the diabetes risk data."""

    # Connect to database
    conn = sqlite3.connect('database/diabetes_data.db')

    # Query 1: Risk category distribution
    risk_query = """
    SELECT rc.diabetes_risk_category, COUNT(*) as count
    FROM fact_patient_measurements f
    JOIN dim_risk_category rc ON f.risk_category_id = rc.risk_category_key
    GROUP BY rc.diabetes_risk_category
    ORDER BY count DESC
    """
    risk_df = pd.read_sql(risk_query, conn)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(risk_df['diabetes_risk_category'], risk_df['count'])
    plt.title('Distribution of Diabetes Risk Categories')
    plt.xlabel('Risk Category')
    plt.ylabel('Number of Patients')
    plt.xticks(rotation=45)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig('./graphs/risk_category_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    # Query 2: BMI vs Age scatter plot
    bmi_age_query = """
    SELECT p.age, f.bmi, rc.diabetes_risk_category
    FROM fact_patient_measurements f
    JOIN dim_patient p ON f.patient_id = p.patient_key
    JOIN dim_risk_category rc ON f.risk_category_id = rc.risk_category_key
    """
    bmi_age_df = pd.read_sql(bmi_age_query, conn)

    plt.figure(figsize=(12, 8))
    colors = {'Low Risk': 'green', 'Prediabetes': 'orange', 'High Risk': 'red'}
    for category in bmi_age_df['diabetes_risk_category'].unique():
        subset = bmi_age_df[bmi_age_df['diabetes_risk_category'] == category]
        plt.scatter(subset['age'], subset['bmi'],
                   c=colors.get(category, 'blue'),
                   label=category, alpha=0.6, s=50)

    plt.title('BMI vs Age by Diabetes Risk Category')
    plt.xlabel('Age')
    plt.ylabel('BMI')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('./graphs/bmi_age_scatter.png', dpi=300, bbox_inches='tight')
    plt.close()

    # Query 3: Average calorie intake by risk category
    calories_query = """
    SELECT rc.diabetes_risk_category, AVG(f.daily_calorie_intake) as avg_calories
    FROM fact_patient_measurements f
    JOIN dim_risk_category rc ON f.risk_category_id = rc.risk_category_key
    GROUP BY rc.diabetes_risk_category
    ORDER BY avg_calories DESC
    """
    calories_df = pd.read_sql(calories_query, conn)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(calories_df['diabetes_risk_category'], calories_df['avg_calories'])
    plt.title('Average Daily Calorie Intake by Risk Category')
    plt.xlabel('Risk Category')
    plt.ylabel('Average Daily Calories')
    plt.xticks(rotation=45)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig('./graphs/calories_by_risk.png', dpi=300, bbox_inches='tight')
    plt.close()

    # Query 4: Age distribution by gender
    age_gender_query = """
    SELECT p.age, p.gender
    FROM dim_patient p
    """
    age_gender_df = pd.read_sql(age_gender_query, conn)

    plt.figure(figsize=(12, 6))

    # Create subplots for male and female
    plt.subplot(1, 2, 1)
    male_ages = age_gender_df[age_gender_df['gender'] == 'Male']['age']
    plt.hist(male_ages, bins=15, alpha=0.7, color='skyblue', edgecolor='black')
    plt.title('Age Distribution - Male Patients')
    plt.xlabel('Age')
    plt.ylabel('Count')

    plt.subplot(1, 2, 2)
    female_ages = age_gender_df[age_gender_df['gender'] == 'Female']['age']
    plt.hist(female_ages, bins=15, alpha=0.7, color='lightcoral', edgecolor='black')
    plt.title('Age Distribution - Female Patients')
    plt.xlabel('Age')
    plt.ylabel('Count')

    plt.tight_layout()
    plt.savefig('./graphs/age_distribution_by_gender.png', dpi=300, bbox_inches='tight')
    plt.close()

    # Query 5: Correlation heatmap of health metrics
    correlation_query = """
    SELECT f.bmi, f.blood_pressure, f.fasting_glucose_level, f.HbA1c_level,
           f.cholesterol_level, f.daily_calorie_intake, f.sleep_hours, f.stress_level
    FROM fact_patient_measurements f
    """
    corr_df = pd.read_sql(correlation_query, conn)

    plt.figure(figsize=(10, 8))
    correlation_matrix = corr_df.corr()
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0,
                square=True, linewidths=0.5, cbar_kws={"shrink": 0.8})
    plt.title('Correlation Matrix of Health Metrics')
    plt.tight_layout()
    plt.savefig('./graphs/health_metrics_correlation.png', dpi=300, bbox_inches='tight')
    plt.close()

    # Query 6: Physical activity level distribution
    activity_query = """
    SELECT f.physical_activity_level, COUNT(*) as count
    FROM fact_patient_measurements f
    GROUP BY f.physical_activity_level
    ORDER BY count DESC
    """
    activity_df = pd.read_sql(activity_query, conn)

    plt.figure(figsize=(10, 6))
    plt.pie(activity_df['count'], labels=activity_df['physical_activity_level'],
            autopct='%1.1f%%', startangle=90)
    plt.title('Distribution of Physical Activity Levels')
    plt.axis('equal')
    plt.tight_layout()
    plt.savefig('./graphs/physical_activity_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    conn.close()

    print("All visualizations created and saved in ./graphs directory!")
    print("Generated graphs:")
    print("1. risk_category_distribution.png")
    print("2. bmi_age_scatter.png")
    print("3. calories_by_risk.png")
    print("4. age_distribution_by_gender.png")
    print("5. health_metrics_correlation.png")
    print("6. physical_activity_distribution.png")

if __name__ == "__main__":
    create_visualizations()
