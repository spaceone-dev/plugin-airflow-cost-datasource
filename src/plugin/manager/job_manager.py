import logging
from datetime import datetime, timedelta

from spaceone.core.error import *
from spaceone.core.manager import BaseManager
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.space_connector = SpaceONEConnector()

    def get_tasks(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        linked_accounts: list,
        schema: str = None,
        start: str = None,
        last_synchronized_at: datetime = None,
    ) -> dict:
        tasks = []
        changed = []

        start_month = self._get_start_month(start, last_synchronized_at)

        for linked_account in linked_accounts:
            account_id = linked_account["account_id"]
            name = linked_account["name"]
            data_source_id = linked_account["data_source_id"]
            v_workspace_id = linked_account["v_workspace_id"]
            is_sync = linked_account.get("is_sync", "false")

            _LOGGER.debug(f"[get_tasks] account_id: {account_id}, name: {name}")

            task_options = {
                "account_id": account_id,
                "name": name,
                "data_source_id": data_source_id,
                "v_workspace_id": v_workspace_id,
            }

            if is_sync == "false":
                first_sync_month = self._get_start_month(start)
                task_options["start"] = first_sync_month

                changed.append(
                    {
                        "start": first_sync_month,
                        "account_id": account_id,
                    }
                )
            else:
                task_options["start"] = start_month

            tasks.append({"task_options": task_options})

        changed.append({"start": start_month})

        _LOGGER.debug(f"[get_tasks] tasks: {tasks}")
        _LOGGER.debug(f"[get_tasks] changed: {changed}")

        return {"tasks": tasks, "changed": changed}

    def _get_start_month(self, start, last_synchronized_at=None):
        if start:
            start_time: datetime = self._parse_start_time(start)
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

        return start_time.strftime("%Y-%m")

    @staticmethod
    def _parse_start_time(start_str):
        date_format = "%Y-%m"

        try:
            return datetime.strptime(start_str, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)
