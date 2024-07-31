"""NetSuite tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

import inspect 

from tap_netsuite_rest import streams

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
        return [
           cls(self) for name, cls in inspect.getmembers(streams,inspect.isclass) if cls.__module__ == 'tap_netsuite_rest.streams'
        ]


if __name__ == "__main__":
    TapNetSuite.cli()
