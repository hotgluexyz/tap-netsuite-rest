"""Stream type classes for tap-netsuite-rest."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th 

from tap_netsuite_rest.client import NetSuiteStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class SalesOrdersStream(NetSuiteStream):
    name = "salesOrder"
    primary_keys = ["id", "lastmodifieddate"]
    table = "Transaction"
    type_filter = "SalesOrd"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ).to_dict()
