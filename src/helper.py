import datetime

import google.auth
import pandas_gbq
from google.cloud import storage
from pandas import DataFrame


def read_bigquery(dataset: str, table_name: str):
    credentials, project_id = google.auth.default()
    df = pandas_gbq.read_gbq("select * from `{}.{}.{}`".format(project_id, dataset, table_name),
                             project_id=project_id,
                             credentials=credentials,
                             location="europe-west3")

    return df


def write_storage(df: DataFrame, sink_name: str):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(sink_name)

    csv_name = "{}-result-check.csv".format(
        datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"))

    bucket.blob(csv_name).upload_from_string(
        df.to_csv(header=True, index=False), "text/csv")


def write_bq(df, project_id, output_dataset_id, output_table_name, credentials):
    print("write to bigquery")
    df.to_gbq(
        "{}.{}".format(output_dataset_id, output_table_name),
        project_id=project_id,
        if_exists="append",
        credentials=credentials,
        progress_bar=None
    )
    print("Query complete. The table is updated.")
