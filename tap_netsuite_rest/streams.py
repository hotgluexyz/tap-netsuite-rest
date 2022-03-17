"""Stream type classes for tap-netsuite-rest."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th

from tap_netsuite_rest.client import NetSuiteStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class SalesOrdersStream(NetSuiteStream):
    name = "sales_orders"
    primary_keys = ["transaction_id", "lastmodifieddate"]
    select = """
        TO_CHAR (t.trandate, 'YYYY-MM-DD HH24:MI:SS') AS trandate,
        TO_CHAR (t.lastmodifieddate, 'YYYY-MM-DD HH24:MI:SS') AS lastmodifieddate,
        t.recordtype, tl.item AS ns_item_id, tl.class,
        tl.quantity, so.amount, t.id AS transaction_id, tl.id AS transaction_line_id
        """
    table = "transaction t"
    join = """
        INNER JOIN transactionline tl ON t.id = tl.transaction
        INNER JOIN salesordered so ON (so.transaction = t.id AND so.tranline = tl.id)
        """
    custom_filter = "tl.itemtype='InvtPart' AND t.recordtype = 'salesorder'"
    replication_key_prefix = "t"

    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("amount", th.StringType),
        th.Property("class", th.StringType),
        th.Property("ns_item_id", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("trandate", th.DateTimeType),
        th.Property("transaction_id", th.StringType),
        th.Property("transaction_line_id", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
    ).to_dict()


class SalesTransactionsStream(NetSuiteStream):
    name = "sales_transactions"
    primary_keys = ["id", "lastmodifieddate"]
    table = "transaction"
    replication_key = "lastmodifieddate"
    custom_filter = "recordtype = 'salesorder'"

    schema = th.PropertiesList(
        th.Property("abbrevtype", th.StringType),
        th.Property("actualshipdate", th.DateTimeType),
        th.Property("balsegstatus", th.StringType),
        th.Property("billingaddress", th.StringType),
        th.Property("billingstatus", th.StringType),
        th.Property("closedate", th.DateTimeType),
        th.Property("createdby", th.StringType),
        th.Property("createddate", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("custbody1", th.StringType),
        th.Property("custbody_call_paypal_again", th.StringType),
        th.Property("custbody_charge_payflow", th.StringType),
        th.Property("custbody_do_not_autobill", th.StringType),
        th.Property("custbody_fa_shipping_tax", th.StringType),
        th.Property("custbody_invoice_customer", th.StringType),
        th.Property("custbody_pj_sscod", th.StringType),
        th.Property("custbody_pj_ssliftgate", th.StringType),
        th.Property("custbody_pj_sssdelivery", th.StringType),
        th.Property("custbody_pj_sssigreq", th.StringType),
        th.Property("custbody_rrw_2_day_shipping", th.StringType),
        th.Property("custbody_rrw_addr_is_verified", th.StringType),
        th.Property("custbody_rrw_estimated_cost", th.StringType),
        th.Property("custbody_rrw_estimated_delivery_date", th.DateTimeType),
        th.Property("custbody_rrw_estimated_transit_time", th.StringType),
        th.Property("custbody_rrw_expected_delivery_date_t", th.StringType),
        th.Property("custbody_rrw_incl_in_dropship_statemnt", th.StringType),
        th.Property("custbody_rrw_is_prime", th.StringType),
        th.Property("custbody_rrw_pacejet_request", th.StringType),
        th.Property("custbody_rrw_pacejet_response", th.StringType),
        th.Property("custbody_rrw_rma_notification_sent", th.StringType),
        th.Property("custbody_rrw_skip_pacejet_quoting", th.StringType),
        th.Property("custbody_solupay_billingschd_autopay", th.StringType),
        th.Property("custbody_storefront_order", th.StringType),
        th.Property("custbody_upaya_paypal_approve", th.StringType),
        th.Property("custbody_upaya_paypal_hold", th.StringType),
        th.Property("custbodyreference_order", th.StringType),
        th.Property("custbodystorefront", th.StringType),
        th.Property("daysopen", th.StringType),
        th.Property("email", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("foreigntotal", th.StringType),
        th.Property("id", th.StringType),
        th.Property("isfinchrg", th.StringType),
        th.Property("isreversal", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("linkedtrackingnumberlist", th.StringType),
        th.Property("nexus", th.StringType),
        th.Property("number", th.StringType),
        th.Property("ordpicked", th.StringType),
        th.Property("paymenthold", th.StringType),
        th.Property("paymentoption", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("printedpickingticket", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("shipcomplete", th.StringType),
        th.Property("shipdate", th.DateTimeType),
        th.Property("shippingaddress", th.StringType),
        th.Property("source", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.DateTimeType),
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


class SalesTransactionLinesStream(NetSuiteStream):
    name = "sales_transactions_lines"
    primary_keys = ["id", "linelastmodifieddate"]
    table = "transaction t"
    replication_key = "linelastmodifieddate"
    join = "INNER JOIN transactionLine tl ON tl.transaction = t.id"
    custom_filter = "t.recordtype = 'salesorder'"
    replication_key_prefix = "tl"
    select_prefix = "tl"

    schema = th.PropertiesList(
        th.Property("amountlinked", th.StringType),
        th.Property("blandedcost", th.StringType),
        th.Property("class", th.StringType),
        th.Property("cleared", th.StringType),
        th.Property("commitinventory", th.StringType),
        th.Property("commitmentfirm", th.StringType),
        th.Property("createdfrom", th.StringType),
        th.Property("debitforeignamount", th.StringType),
        th.Property("department", th.StringType),
        th.Property("donotdisplayline", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("expenseaccount", th.StringType),
        th.Property("foreignamount", th.StringType),
        th.Property("fulfillable", th.StringType),
        th.Property("hasfulfillableitems", th.StringType),
        th.Property("id", th.StringType),
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
        th.Property("netamount", th.StringType),
        th.Property("oldcommitmentfirm", th.StringType),
        th.Property("paymentmethod", th.StringType),
        th.Property("processedbyrevcommit", th.StringType),
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
        th.Property("units", th.StringType),
    ).to_dict()


# class InventoryItemLocationStream(NetSuiteStream):
#     name = "inventory_item_location"
#     primary_keys = ["ns_item_id", "lastmodifieddate"]
#     select = """
#         i.id AS ns_item_id,
#         i.itemid AS sku,
#         iil.quantity
#         """
#     table = "item i"
#     join = """
#         INNER JOIN (SELECT item, SUM(quantityavailable)
#         AS quantity FROM inventoryitemlocations GROUP BY item) iil
#         ON i.id = iil.item
#         """
#     custom_filter = "i.isinactive='F' AND i.itemtype='InvtPart'"

#     schema = th.PropertiesList(
#         th.Property("ns_item_id", th.StringType),
#         th.Property("quantity", th.StringType),
#         th.Property("sku", th.StringType),
#     ).to_dict()


class PricingStream(NetSuiteStream):
    name = "pricing"
    primary_keys = ["internalid"]
    table = "pricing"

    schema = th.PropertiesList(
        th.Property("internalid", th.StringType),
        th.Property("item", th.StringType),
        th.Property("pricelevel", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("saleunit", th.StringType),
        th.Property("unitprice", th.StringType),
    ).to_dict()


class InventoryPricingStream(NetSuiteStream):
    name = "inventory_pricing"
    primary_keys = ["ns_item_id"]
    select = (
        "p.item AS ns_item_id, p.pricelevel AS price_level_id, p.unitprice AS price"
    )
    table = "pricing p"
    join = "INNER JOIN item i ON p.item = i.id"
    custom_filter = "i.itemtype='InvtPart'"

    schema = th.PropertiesList(
        th.Property("ns_item_id", th.StringType),
        th.Property("price", th.StringType),
        th.Property("price_level_id", th.StringType),
    ).to_dict()


class PriceLevelStream(NetSuiteStream):
    name = "price_level"
    primary_keys = ["id", "lastmodifieddate"]
    table = "pricelevel"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("isonline", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("name", th.StringType),
    ).to_dict()


class LocationsStream(NetSuiteStream):
    name = "locations"
    primary_keys = ["id", "lastmodifieddate"]
    table = "location"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("custrecord_alt_name", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("id", th.StringType),
        th.Property("includechildren", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("locationtype", th.StringType),
        th.Property("mainaddress", th.StringType),
        th.Property("makeinventoryavailable", th.StringType),
        th.Property("makeinventoryavailablestore", th.StringType),
        th.Property("name", th.StringType),
        th.Property("returnaddress", th.StringType),
        th.Property("subsidiary", th.StringType)
    ).to_dict()


class CostStream(NetSuiteStream):
    name = "cost"
    primary_keys = ["id", "lastmodifieddate"]
    table = "item"
    custom_filter = "itemtype='InvtPart'"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("averagecost", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
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
        th.Property("createddate", th.DateTimeType),
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
        th.Property("name", th.StringType),
        th.Property("subsidiary", th.StringType),
    ).to_dict()


class InventoryItemLocationsStream(NetSuiteStream):
    name = "inventory_item_locations"
    primary_keys = []
    table = "inventoryitemlocations"

    schema = th.PropertiesList(
        th.Property("averagecostmli", th.StringType),
        th.Property("costaccountingstatus", th.StringType),
        th.Property("item", th.StringType),
        th.Property("lastpurchasepricemli", th.StringType),
        th.Property("lastquantityavailablechange", th.DateTimeType),
        th.Property("location", th.StringType),
        th.Property("onhandvaluemli", th.StringType),
        th.Property("quantityavailable", th.StringType),
        th.Property("quantitybackordered", th.StringType),
        th.Property("quantitycommitted", th.StringType),
        th.Property("quantityonhand", th.StringType)
    ).to_dict()