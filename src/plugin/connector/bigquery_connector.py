import logging
from google.cloud import bigquery
from .google_cloud_connector import GoogleCloudConnector

__all__ = ["BigQueryConnector"]

_LOGGER = logging.getLogger("spaceone")


class BigQueryConnector(GoogleCloudConnector):
    google_client_service = "bigquery"
    version = "v2"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bigquery_client = bigquery.Client(
            project=self.project_id, credentials=self.credentials
        )

    def list_dataset(self, **query):
        dataset_list = []
        query.update({"projectId": self.project_id})
        request = self.client.datasets().list(**query)
        while request is not None:
            response = request.execute()
            for dataset in response.get("datasets", []):
                dataset_list.append(dataset)
            request = self.client.datasets().list_next(
                previous_request=request, previous_response=response
            )

        return dataset_list

    def create_dataset(self, dataset_id):
        query = {
            "projectId": self.project_id,
            "body": {
                "datasetReference": {
                    "projectId": self.project_id,
                    "datasetId": dataset_id,
                },
                "location": "asia-northeast3",
            },
        }
        response = self.client.datasets().insert(**query).execute()

        return response

    def get_table(self, dataset_id, table_id, **query):
        query.update(
            {"projectId": self.project_id, "datasetId": dataset_id, "tableId": table_id}
        )
        response = self.client.tables().get(**query).execute()

        return response

    def create_table_with_schema(self, dataset_id, table_id):
        query = {
            "projectId": self.project_id,
            "datasetId": dataset_id,
        }
        body = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "schema": {
                "fields": [
                    {
                        "name": "task_options",
                        "type": "JSON",
                        "mode": "REQUIRED",
                    },
                    {
                        "name": "created_at",
                        "type": "STRING",
                        "mode": "REQUIRED",
                    },
                ]
            },
        }
        query.update({"body": body})
        response = self.client.tables().insert(**query).execute()

        return response

    def insert_rows(self, table_id, rows):
        errors = self.bigquery_client.insert_rows_json(table_id, rows)

        if not errors:
            _LOGGER.info("New rows have been added.")
        else:
            _LOGGER.info(f"Encountered errors while inserting rows: {errors}")
