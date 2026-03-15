from datetime import datetime, timezone
from extract import extract
from transform import transform
from load import load


class Pipeline:
    """
    ETL pipeline for processing diabetes risk data into a star schema.

    This pipeline performs the following steps:
    1. Extract data from a CSV file.
    2. Load the raw data into fact and dimension tables in a star schema.
    3. Record pipeline lineage metadata.

    Attributes:
        csv_path (str): Path to the CSV file containing raw patient data.
        lineage (dict): Dictionary storing metadata about the pipeline run,
                        such as start/end times, run ID, and record counts.
    """

    def __init__(self, csv_path: str):
        """
        Initialize the Pipeline with the CSV path.

        Args:
            csv_path (str): Path to the CSV file.
        """
        self.csv_path = csv_path
        self.lineage = {}

    def run(self):
        """
        Execute the ETL pipeline.

        Steps:
        1. Start lineage tracking.
        2. Extract data from CSV.
        3. Transform the extracted data into star schema dataframes.
        4. Load the star schema dataframes into the database.
        5. End lineage tracking.
        """
        self._start_lineage()

        df = extract(self.csv_path)
        self.lineage["rows_extracted"] = len(df)

        schema_dataframes = transform(df)
        self.lineage["rows_after_transform"] = len(schema_dataframes['fact_measurements'])

        load(schema_dataframes, self.lineage)

        self._end_lineage()

    def _start_lineage(self):
        """
        Initialize pipeline lineage metadata.

        Sets:
            run_id (str): Unique identifier for this pipeline run.
            source_file (str): Path to the input CSV file.
            start_time (str): UTC ISO-formatted timestamp of the pipeline start.
        """
        self.lineage["run_id"] = datetime.now(timezone.utc).isoformat()
        self.lineage["source_file"] = self.csv_path
        self.lineage["start_time"] = datetime.now(timezone.utc).isoformat()

    def _end_lineage(self):
        """
        Finalize pipeline lineage metadata.

        Sets:
            end_time (str): UTC ISO-formatted timestamp of the pipeline end.
        """
        self.lineage["end_time"] = datetime.now(timezone.utc).isoformat()
