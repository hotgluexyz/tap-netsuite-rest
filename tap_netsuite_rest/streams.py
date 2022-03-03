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
        th.Property("abbrevtype", th.StringType),
        th.Property("balsegstatus", th.StringType),
        th.Property("billingaddress", th.StringType),
        th.Property("billingstatus", th.StringType),
        th.Property("createdby", th.StringType),
        th.Property("createddate", th.DateType),
        th.Property("currency", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("custbodystorefront", th.StringType),
        th.Property("daysopen", th.StringType),
        th.Property("email", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("foreigntotal", th.StringType),
        th.Property("isfinchrg", th.StringType),
        th.Property("isreversal", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("nextbilldate", th.DateType),
        th.Property("nexus", th.StringType),
        th.Property("number", th.StringType),
        th.Property("ordpicked", th.StringType),
        th.Property("paymenthold", th.StringType),
        th.Property("paymentmethod", th.StringType),
        th.Property("paymentoption", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("printedpickingticket", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("shipcomplete", th.StringType),
        th.Property("shipdate", th.DateType),
        th.Property("shippingaddress", th.StringType),
        th.Property("source", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.DateType),
        th.Property("trandisplayname", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("type", th.StringType),
        th.Property("typebaseddocumentnumber", th.StringType),
        th.Property("userevenuearrangement", th.StringType),
        th.Property("visibletocustomer", th.StringType),
        th.Property("void", th.StringType),
        th.Property("voided", th.StringType),
    ).to_dict()
