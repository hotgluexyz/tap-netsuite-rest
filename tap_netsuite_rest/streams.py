"""Stream type classes for tap-netsuite-rest."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th 

from tap_netsuite_rest.client import NetSuiteStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class SalesOrdersStream(NetSuiteStream):
    name = "sales orders"
    primary_keys = ["transaction_id"]
    select = "t.trandate, t.recordtype, tl.item AS ns_item_id, tl.class, tl.quantity, so.amount, t.id AS transaction_id, tl.id AS transaction_line_id"
    table = "transaction t"
    join = "INNER JOIN transactionline tl ON t.id = tl.transaction INNER JOIN salesordered so ON (so.transaction = t.id AND so.tranline = tl.id)"
    custom_filter = "tl.itemtype='InvtPart' AND t.recordtype = 'salesorder'"
    replication_key_prefix = "t."
    
    replication_key = "trandate"

    schema = th.PropertiesList(
        th.Property("amount", th.StringType),
        th.Property("class", th.StringType),
        th.Property("ns_item_id", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("trandate", th.DateTimeType),
        th.Property("transaction_id", th.DateType),
        th.Property("transaction_line_id", th.DateType),
    ).to_dict()

class InventoryItemLocationStream(NetSuiteStream):
    name = "inventory_item_location"
    primary_keys = ["ns_item_id"]
    select = "i.id AS ns_item_id, i.itemid AS sku, iil.quantity"
    table = "item i"
    join = "INNER JOIN (SELECT item, SUM(quantityavailable) AS quantity FROM inventoryitemlocations GROUP BY item) iil ON i.id = iil.item"
    custom_filter = "i.isinactive='F' AND i.itemtype='InvtPart'"

    schema = th.PropertiesList(
        th.Property("ns_item_id", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("sku", th.StringType)
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


class InventoryPricingStream(NetSuiteStream):
    name = "inventory pricing"
    primary_keys = ["ns_item_id"]
    select = "p.item AS ns_item_id, p.pricelevel AS price_level_id, p.unitprice AS price"
    table = "pricing p"
    join = "INNER JOIN item i ON p.item = i.id"
    custom_filter = "i.itemtype='InvtPart'"
    
    schema = th.PropertiesList(
        th.Property("ns_item_id", th.StringType),
        th.Property("price", th.StringType),
        th.Property("price_level_id", th.StringType)
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
