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
        th.Property("voided", th.StringType)
    ).to_dict()

class TransactionLinesStream(NetSuiteStream):
    name = "transactionLine"
    primary_keys = ["id", "linelastmodifieddate"]
    table = "TransactionLine"
    type_filter = False
    replication_key = "linelastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("amountlinked", th.StringType),
        th.Property("blandedcost", th.StringType),
        th.Property("class", th.StringType),
        th.Property("cleared", th.StringType),
        th.Property("commitinventory", th.StringType),
        th.Property("commitmentfirm", th.StringType),
        th.Property("createdfrom", th.StringType),
        th.Property("department", th.StringType),
        th.Property("donotdisplayline", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("fulfillable", th.StringType),
        th.Property("hasfulfillableitems", th.StringType),
        th.Property("isbillable", th.StringType),
        th.Property("isclosed", th.StringType),
        th.Property("iscogs", th.StringType),
        th.Property("isfullyshipped", th.StringType),
        th.Property("isfxvariance", th.StringType),
        th.Property("isinventoryaffecting", th.StringType),
        th.Property("item", th.StringType),
        th.Property("itemtype", th.StringType),
        th.Property("kitcomponent", th.StringType),
        th.Property("landedcostperline", th.StringType),
        th.Property("linelastmodifieddate", th.DateTimeType),
        th.Property("linesequencenumber", th.StringType),
        th.Property("location", th.StringType),
        th.Property("mainline", th.StringType),
        th.Property("matchbilltoreceipt", th.StringType),
        th.Property("oldcommitmentfirm", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("quantitybilled", th.StringType),
        th.Property("quantitypacked", th.StringType),
        th.Property("quantitypicked", th.StringType),
        th.Property("quantityrejected", th.StringType),
        th.Property("quantityshiprecv", th.StringType),
        th.Property("shipmethod", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("taxline", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("transactiondiscount", th.StringType),
        th.Property("uniquekey", th.StringType),
        th.Property("units", th.StringType)        
    ).to_dict()


class PricingStream(NetSuiteStream):
    name = "pricing"
    primary_keys = ["internalid"]
    table = "pricing"
    type_filter = False
    
    schema = th.PropertiesList(
        th.Property("internalid", th.StringType),
        th.Property("item", th.StringType),
        th.Property("pricelevel", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("saleunit", th.StringType),
        th.Property("unitprice", th.StringType),
    ).to_dict()


class ItemStream(NetSuiteStream):
    name = "item"
    primary_keys = ["id", "lastmodifieddate"]
    table = "item"
    type_filter = False
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("assetaccount", th.StringType),
        th.Property("atpmethod", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("costingmethod", th.StringType),
        th.Property("costingmethoddisplay", th.StringType),
        th.Property("createddate", th.DateType),
        th.Property("description", th.StringType),
        th.Property("displayname", th.StringType),
        th.Property("dontshowprice", th.StringType),
        th.Property("enforceminqtyinternally", th.StringType),
        th.Property("excludefromsitemap", th.StringType),
        th.Property("expenseaccount", th.StringType),
        th.Property("froogleproductfeed", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("fxcost", th.StringType),
        th.Property("generateaccruals", th.StringType),
        th.Property("includechildren", th.StringType),
        th.Property("incomeaccount", th.StringType),
        th.Property("isdonationitem", th.StringType),
        th.Property("isfulfillable", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("isonline", th.StringType),
        th.Property("isserialitem", th.StringType),
        th.Property("itemid", th.StringType),
        th.Property("itemtype", th.StringType),
        th.Property("lastpurchaseprice", th.StringType),
        th.Property("manufacturer", th.StringType),
        th.Property("matchbilltoreceipt", th.StringType),
        th.Property("nextagproductfeed", th.StringType),
        th.Property("printitems", th.StringType),
        th.Property("purchasedescription", th.StringType),
        th.Property("purchaseunit", th.StringType),
        th.Property("saleunit", th.StringType),
        th.Property("seasonaldemand", th.StringType),
        th.Property("shipindividually", th.StringType),
        th.Property("shoppingproductfeed", th.StringType),
        th.Property("shopzillaproductfeed", th.StringType),
        th.Property("showdefaultdonationamount", th.StringType),
        th.Property("stockunit", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("supplyreplenishmentmethod", th.StringType),
        th.Property("totalquantityonhand", th.StringType),
        th.Property("totalvalue", th.StringType),
        th.Property("tracklandedcost", th.StringType),
        th.Property("unitstype", th.StringType),
        th.Property("upccode", th.StringType),
        th.Property("usemarginalrates", th.StringType),
        th.Property("weight", th.StringType),
        th.Property("weightunit", th.StringType),
        th.Property("weightunits", th.StringType),
        th.Property("yahooproductfeed", th.StringType),
        
    ).to_dict()


class ClassificationStream(NetSuiteStream):
    name = "classification"
    primary_keys = ["id", "lastmodifieddate"]
    table = "classification"
    type_filter = False
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("includechildren", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("name", th.DateTimeType),
        th.Property("subsidiary", th.DateTimeType),
    ).to_dict()
