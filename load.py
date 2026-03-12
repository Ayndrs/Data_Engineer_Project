import sqlite3
import pandas as pd
from datetime import datetime, timezone

def load(summary, lineage):

    conn = sqlite3.connect("database/diabetes_data.db")

    summary.to_sql("diabetes_summary", conn, if_exists="replace", index=False)

    lineage["end_time"] = datetime.now(timezone.utc)
    lineage_df = pd.DataFrame([lineage])
    lineage_df.to_sql("pipeline_lineage", conn, if_exists="append", index=False)

    conn.close()
