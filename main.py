from pipeline import Pipeline
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

if __name__ == "__main__":
    pipeline = Pipeline("./raw_data/diabetes_risk_dataset.csv")
    pipeline.run()
