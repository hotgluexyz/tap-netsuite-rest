"""Stream type classes for tap-netsuite-rest."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_netsuite_rest.client import NetSuiteStream

import requests

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class SalesOrderStream(NetSuiteStream):
    name = "salesOrder"
    path = "/salesOrder"
    primary_keys = ["id"]

    # replication_key = "modified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType)
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "id": record["id"]
        }

class SalesOrdersStream(NetSuiteStream):
    """Define custom stream."""
    name = "salesOrders"
    path = "/salesOrder/{id}?expandSubResources=true"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = SalesOrderStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("total", th.NumberType),
        th.Property("class", th.CustomType({"type": ["null", "object"]})),
        th.Property("item", th.CustomType({"type": ["null", "object"]}))
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        return [response.json()]
