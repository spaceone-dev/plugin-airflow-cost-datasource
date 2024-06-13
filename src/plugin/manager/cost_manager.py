import logging
from typing import Generator
from datetime import datetime
from dateutil import rrule

from spaceone.core.manager import BaseManager
from spaceone.core.error import *
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger(__name__)


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
        self._check_task_options(task_options)

        start = task_options["start"]
        account_id = task_options["account_id"]
        data_source_id = task_options["data_source_id"]
        workspace_id = task_options["v_workspace_id"]
        domain_id = task_options["domain_id"]

        date_ranges = self._get_date_range(start)

        # TODO: trigger cloud composer

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

    @staticmethod
    def _get_date_range(start):
        date_ranges = []
        start_time = datetime.strptime(start, "%Y-%m")
        now = datetime.utcnow()
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_time, until=now):
            billed_month = dt.strftime("%Y-%m")
            date_ranges.append(billed_month)

        return date_ranges
