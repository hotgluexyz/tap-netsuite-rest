"""NetSuite tap class."""

from typing import List, cast

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

import inspect 

from tap_netsuite_rest import streams

class TapNetSuite(Tap):
    """NetSuite tap class."""

    name = "tap-netsuite-rest"
    tables_metadata = {}

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

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        catalog = cast(dict, self._singer_catalog.to_dict())
        streams = catalog["streams"]
        for stream in streams:
            stream_id = stream["tap_stream_id"]
            stream_metadata = self.tables_metadata.get(stream_id, {}).get("properties", {})
            for field in stream["schema"]["properties"]:
                stream["schema"]["properties"][field]["field_meta"] = stream_metadata.get(field, {})
        return catalog


if __name__ == "__main__":
    TapNetSuite.cli()
