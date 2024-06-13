import logging

from spaceone.core.manager import BaseManager
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):

    @staticmethod
    def init_response(options: dict) -> dict:
        return {
            "metadata": {
                "currency": "USD",
                "supported_secret_types": ["MANUAL"],
                "data_source_rules": [
                    {
                        "name": "match_service_account",
                        "conditions_policy": "ALWAYS",
                        "actions": {
                            "match_service_account": {
                                "source": "labels.Account ID",
                                "target": "data.account_id",
                            }
                        },
                        "options": {"stop_processing": True},
                    }
                ],
                "use_account_routing": True,
            }
        }

    @staticmethod
    def verify_plugin(
        options: dict, secret_data: dict, domain_id: str, schema: str = None
    ) -> None:
        space_connector = SpaceONEConnector()
        space_connector.init_client(options, secret_data, schema)
        space_connector.verify_plugin(domain_id)
