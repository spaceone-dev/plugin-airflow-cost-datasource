import logging
from .google_cloud_connector import GoogleCloudConnector

__all__ = ["CloudComposerConnector"]

_LOGGER = logging.getLogger("spaceone")

CLOUD_COMPOSER_CONFIG = {
    "location": "asia-northeast3",
    "composer_id": "spaceone-dev-composer",
}


class CloudComposerConnector(GoogleCloudConnector):
    google_client_service = "composer"
    version = "v1"

    def execute_airflow_command(self, trigger_dag_name: str, dict_conf: str) -> list:
        environment = f"projects/{self.project_id}/locations/{CLOUD_COMPOSER_CONFIG['location']}/environments/{CLOUD_COMPOSER_CONFIG['composer_id']}"
        query = {
            "command": "dags",
            "subcommand": "trigger",
            "parameters": [trigger_dag_name, "--conf", f"{dict_conf}"],
        }
        request = (
            self.client.projects()
            .locations()
            .environments()
            .executeAirflowCommand(environment=environment, body=query)
        )

        return request.execute()
