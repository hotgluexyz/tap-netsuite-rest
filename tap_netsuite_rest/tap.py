"""NetSuite tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_netsuite_rest.streams import (
    NetSuiteStream,
    SalesOrderStream,
    SalesOrdersStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    SalesOrderStream,
    SalesOrdersStream
]


class TapNetSuite(Tap):
    """NetSuite tap class."""
    name = "tap-netsuite-rest"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "ns_account",
            th.StringType,
            required=True
        ),
        th.Property(
            "ns_consumer_key",
            th.StringType,
            required=True
        ),
        th.Property(
            "ns_consumer_secret",
            th.StringType,
            required=True
        ),
        th.Property(
            "ns_token_key",
            th.StringType,
            required=True
        ),
        th.Property(
            "ns_token_secret",
            th.StringType,
            required=True
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
