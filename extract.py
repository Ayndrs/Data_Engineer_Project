import pandas as pd


def extract(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)
