from datetime import datetime, timezone
from extract import extract
from transform import transform
from load import load


class DiabetesPipeline:

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.lineage = {}

    def run(self):
        self._start_lineage()

        df = extract(self.csv_path)
        self.lineage["rows_extracted"] = len(df)

        summary = transform(df)
        self.lineage["rows_after_transform"] = len(summary)

        load(summary, self.lineage)

        self._end_lineage()

    def _start_lineage(self):
        self.lineage["run_id"] = datetime.now(timezone.utc).isoformat()
        self.lineage["source_file"] = self.csv_path
        self.lineage["start_time"] = datetime.now(timezone.utc).isoformat()

    def _end_lineage(self):
        self.lineage["end_time"] = datetime.now(timezone.utc).isoformat()
