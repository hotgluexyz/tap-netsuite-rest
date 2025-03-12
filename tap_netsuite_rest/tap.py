"""NetSuite tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

import inspect 

from tap_netsuite_rest import streams
import os
import logging

# When a new stream is added or changes in the tap, it would break all existing test suites due to dynamic discover.
# By allowing caller to include only streams we need we are able to ensure existing tests continue to pass.
# 1. Get the environment variable INCLUDE_STREAMS and split by commas
include_streams = os.environ.get('INCLUDE_STREAMS', "").split(',') if os.environ.get('INCLUDE_STREAMS', "") else []
logging.info(f"INCLUDE_STREAMS: "+ os.environ.get('INCLUDE_STREAMS', ''))

# 2. Get the environment variable IGNORE_STREAMS and split by commas
ignore_streams = os.environ.get('IGNORE_STREAMS', "").split(',') if os.environ.get('IGNORE_STREAMS', "") else []
logging.info(f"IGNORE_STREAMS: "+ os.environ.get('IGNORE_STREAMS', ''))

# Function to filter streams to be tested
def streams_to_sync(self, include_streams, ignore_streams):
    stream_types = []
    
    for name, cls in inspect.getmembers(streams,inspect.isclass):
        if cls.__module__ == 'tap_netsuite_rest.streams':
            if (include_streams and name not in include_streams) or name in ignore_streams:
                continue
            stream_types.append(cls(self))
    return stream_types

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
        streams = streams_to_sync(self, include_streams, ignore_streams)
        return streams


if __name__ == "__main__":
    TapNetSuite.cli()
