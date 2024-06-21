import logging
import json
from typing import Generator

from spaceone.core.manager import BaseManager
from spaceone.core.error import *

from ..connector.cloud_composer_connector import CloudComposerConnector
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger("spaceone")

TRIGGER_DAG_NAME = "dags_create_spaceone_cost_data"


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.space_connector = SpaceONEConnector()

    def get_linked_accounts(
        self, options: dict, secret_data: dict, domain_id: str, schema: dict = None
    ):
        provider = options.get("provider", "aws")
        self.space_connector.init_client(options, secret_data, schema)
        service_accounts = self.space_connector.list_service_accounts(provider)
        linked_accounts = []
        for service_account in service_accounts.get("results", []):
            linked_accounts.append(
                {
                    "account_id": service_account["data"]["account_id"],
                    "name": service_account["name"],
                    "tags": service_account.get("tags", {}),
                }
            )
        return {"results": linked_accounts}

    def get_data(
        self, options: dict, secret_data: dict, task_options: dict, schema: str = None
    ) -> Generator[dict, None, None]:
        for account_id, task_option in task_options.items():
            self._check_task_options(task_option)
        json_data_str = json.dumps(task_options, default=str)

        composer_connector = CloudComposerConnector(options, secret_data, schema)
        execution_info = composer_connector.execute_airflow_command(
            trigger_dag_name=TRIGGER_DAG_NAME, dict_conf=json_data_str
        )
        _LOGGER.info(
            f"[get_data] trigger dag name: {TRIGGER_DAG_NAME}, execution_info: {execution_info}"
        )

        yield {"results": []}

    @staticmethod
    def _check_task_options(task_options):
        if "start" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.start")

        if "account_id" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.account_id")

        if "data_source_id" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.data_source_id")

        if "v_workspace_id" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.v_workspace_id")

        if "domain_id" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.domain_id")
