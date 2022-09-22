"""NetSuite tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_netsuite_rest.streams import (
    ClassificationStream,
    CostStream,
    InventoryItemLocationsStream,
    InventoryPricingStream,
    ItemStream,
    LocationsStream,
    NetSuiteStream,
    PriceLevelStream,
    PricingStream,
    SalesOrdersStream,
    SalesTransactionLinesStream,
    SalesTransactionsStream,
    ProfitLossReportStream,
)

STREAM_TYPES = [
    SalesOrdersStream,
    PricingStream,
    ItemStream,
    ClassificationStream,
    InventoryPricingStream,
    CostStream,
    PriceLevelStream,
    SalesTransactionLinesStream,
    SalesTransactionsStream,
    InventoryItemLocationsStream,
    LocationsStream,
    ProfitLossReportStream,
]


class TapNetSuite(Tap):
    """NetSuite tap class."""

    name = "tap-netsuite-rest"

    config_jsonschema = th.PropertiesList(
        th.Property("ns_account", th.StringType, required=True),
        th.Property("ns_consumer_key", th.StringType, required=True),
        th.Property("ns_consumer_secret", th.StringType, required=True),
        th.Property("ns_token_key", th.StringType, required=True),
        th.Property("ns_token_secret", th.StringType, required=True),
        th.Property("window_days", th.IntegerType, default=10),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapNetSuite.cli()
