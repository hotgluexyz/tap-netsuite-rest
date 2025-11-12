"""NetSuite tap class."""

from typing import List, Dict, Type

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

import inspect
from singer_sdk.helpers._compat import final 

from tap_netsuite_rest import streams
from tap_netsuite_rest.streams import AccountsStream, TransactionLinesStream
from tap_netsuite_rest.constants import TRANSACTION_REFERENCE_DATA_STREAMS

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
        th.Property("dynamic_child_streams", th.BooleanType, default=False),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
           cls(self) for name, cls in inspect.getmembers(streams,inspect.isclass) if cls.__module__ == 'tap_netsuite_rest.streams'
        ]
    
    def ignore_parent_stream(self, stream, parents):
        if self.input_catalog and stream.name in TRANSACTION_REFERENCE_DATA_STREAMS:
            parent = parents[0]
            raw_catalog = self.input_catalog
            # check if parent stream is selected from raw catalog, otherwise when reading selected the first time it's always true
            is_parent_selected = raw_catalog.get(parent.name).metadata.get((), {}).selected
            if not self.config.get("get_transactions_reference_data") or not is_parent_selected:
                return True
            return False
        return stream.ignore_parent_stream
    
    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        self.logger.debug("Loading streams TESTING DEBUG LOGS!!!")
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type or (hasattr(stream_type, 'parent')):
                # add child streams to parent streams if parent stream type is defined and ignore parent stream is not True
                parent = stream_type.parent if hasattr(stream_type, 'parent') else stream_type.parent_stream_type
                parents = streams_by_type[parent]
                ignore_parent_stream = self.ignore_parent_stream(stream_type, parents)
                stream_type.ignore_parent_stream = ignore_parent_stream

                if ignore_parent_stream:
                    continue

                # add parent and flag to child stream
                stream_type.parent_stream_type = parent

                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        streams = [stream for streams in streams_by_type.values() for stream in streams]
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )


if __name__ == "__main__":
    TapNetSuite.cli()
