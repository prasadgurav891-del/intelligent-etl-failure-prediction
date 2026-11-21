import csv
import os

import psycopg2
from psycopg2.extras import DictCursor

from etl.config import Config
from metadata.features import AUDIT_COLUMNS, PRE_EXECUTION_FEATURE_COLUMNS, TARGET_COLUMN


def export(config=None, output="data/processed/ml_training_data.csv",
           audit_output="data/processed/ml_training_data_audit.csv"):
    config = config or Config.from_env()
    os.makedirs(os.path.dirname(output), exist_ok=True)
    os.makedirs(os.path.dirname(audit_output), exist_ok=True)
    with psycopg2.connect(**config.db_kwargs()) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        selected_columns = (*AUDIT_COLUMNS, *PRE_EXECUTION_FEATURE_COLUMNS, TARGET_COLUMN)
        cur.execute("SELECT " + ",".join(selected_columns) + " FROM ml_training_data ORDER BY etl_run_id,task_position")
        columns = [d.name for d in cur.description]
        feature_columns = (*PRE_EXECUTION_FEATURE_COLUMNS, TARGET_COLUMN)
        audit_columns = (*AUDIT_COLUMNS, "task_name", TARGET_COLUMN)
        feature_indices = [columns.index(name) for name in feature_columns]
        audit_indices = [columns.index(name) for name in audit_columns]
        with (open(output, "w", newline="", encoding="utf-8") as feature_stream,
              open(audit_output, "w", newline="", encoding="utf-8") as audit_stream):
            feature_writer = csv.writer(feature_stream)
            audit_writer = csv.writer(audit_stream)
            feature_writer.writerow(feature_columns)
            audit_writer.writerow(audit_columns)
            while rows := cur.fetchmany(2000):
                for row in rows:
                    values = tuple(row)
                    feature_writer.writerow([values[i] for i in feature_indices])
                    audit_writer.writerow([values[i] for i in audit_indices])
    print(f"Exported model-ready features to {output}")
    print(f"Exported scenario/label audit data to {audit_output}")


if __name__ == "__main__":
    export()
