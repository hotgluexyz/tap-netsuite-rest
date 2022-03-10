"""NetSuite tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_netsuite_rest.streams import (
    NetSuiteStream,
    SalesOrdersStream,
    InventoryItemLocationStream,
    PricingStream,
    ItemStream,
    ClassificationStream,
    InventoryPricingStream,
    CostStream,
    PriceLevelStream,
    SalesTransactionLinesStream,
    SalesTransactionsStream
)

STREAM_TYPES = [
    SalesOrdersStream,
    InventoryItemLocationStream,
    PricingStream,
    ItemStream,
    ClassificationStream,
    InventoryPricingStream,
    CostStream,
    PriceLevelStream,
    SalesTransactionLinesStream,
    SalesTransactionsStream
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

if __name__=="__main__":
    TapNetSuite.cli()