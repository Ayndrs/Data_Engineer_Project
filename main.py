from pipeline import DiabetesPipeline

if __name__ == "__main__":
    pipeline = DiabetesPipeline("./raw_data/diabetes_risk_dataset.csv")
    pipeline.run()
