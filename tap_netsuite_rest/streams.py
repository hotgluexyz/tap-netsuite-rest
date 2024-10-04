"""Stream type classes for tap-netsuite-rest."""

from pathlib import Path
from typing import Any, Dict, Optional, Union

from singer_sdk import typing as th

from tap_netsuite_rest.client import NetSuiteStream, NetsuiteDynamicStream, TransactionRootStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from datetime import datetime, timedelta
from pendulum import parse
from uuid import uuid4
from dateutil.relativedelta import relativedelta

import requests


class SalesOrdersStream(NetSuiteStream):
    name = "sales_orders"
    primary_keys = ["transaction_id", "lastmodifieddate"]
    entities_fallback = [
        {
            "name": "salesordered",
            "select_replace": "so.amount,",
            "join_replace": "INNER JOIN salesordered so ON (so.transaction = t.id AND so.tranline = tl.id)",
        }
    ]
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


class SalesTransactionsStream(TransactionRootStream):
    name = "sales_transactions"
    primary_keys = ["id", "lastmodifieddate"]
    table = "transaction"
    replication_key = "lastmodifieddate"
    custom_filter = "recordtype = 'salesorder'"

    schema = th.PropertiesList(
        th.Property("abbrevtype", th.StringType),
        th.Property("actualshipdate", th.DateTimeType),
        th.Property("billingaddress", th.StringType),
        th.Property("billingstatus", th.StringType),
        th.Property("closedate", th.DateTimeType),
        th.Property("createdby", th.StringType),
        th.Property("createddate", th.DateTimeType),
        th.Property("currency", th.StringType),
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


class VendorBillsStream(NetSuiteStream):
    name = "vendor_bill_transactions"
    primary_keys = ["id"]
    table = "transaction"
    replication_key = "lastmodifieddate"
    custom_filter = "recordtype = 'vendorbill'"

    schema = th.PropertiesList(
        th.Property("abbrevtype", th.StringType),
        th.Property("approvalstatus", th.StringType),
        th.Property("balsegstatus", th.StringType),
        th.Property("billingstatus", th.StringType),
        th.Property("closedate", th.DateTimeType),
        th.Property("createdby", th.StringType),
        th.Property("createddate", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("customtype", th.StringType),
        th.Property("daysopen", th.StringType),
        th.Property("daysoverduesearch", th.StringType),
        th.Property("duedate", th.DateTimeType),
        th.Property("entity", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("foreignamountpaid", th.StringType),
        th.Property("foreignamountunpaid", th.StringType),
        th.Property("foreigntotal", th.StringType),
        th.Property("id", th.StringType),
        th.Property("isfinchrg", th.StringType),
        th.Property("isreversal", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("nexus", th.StringType),
        th.Property("number", th.StringType),
        th.Property("ordpicked", th.StringType),
        th.Property("paymenthold", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("printedpickingticket", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.DateTimeType),
        th.Property("trandisplayname", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("type", th.StringType),
        th.Property("userevenuearrangement", th.StringType),
        th.Property("visibletocustomer", th.StringType),
        th.Property("void", th.StringType),
        th.Property("voided", th.StringType),
    ).to_dict()


class SalesTransactionLinesStream(TransactionRootStream):
    name = "sales_transactions_lines"
    primary_keys = ["id", "transaction"]
    table = "transaction t"
    replication_key = "linelastmodifieddate"
    join = "INNER JOIN transactionLine tl ON tl.transaction = t.id"
    custom_filter = "t.recordtype = 'salesorder'"
    replication_key_prefix = "tl"
    select_prefix = "tl"

    schema = th.PropertiesList(
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


class VendorStream(NetsuiteDynamicStream):
    name = "vendor"
    primary_keys = ["id"]
    table = "vendor"
    replication_key = "lastmodifieddate"


class TrialBalanceReportStream(NetSuiteStream):
    name = "trial_balance_report"
    start_date_f = None
    end_date = None
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("account_type", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("period_name", th.StringType),
        th.Property("period_start_date", th.StringType),
        th.Property("period_end_date", th.StringType),
        th.Property("posting_period", th.StringType),
        th.Property("accumulated_amount", th.StringType),
        th.Property("credit_amount", th.StringType),
        th.Property("debit_amount", th.StringType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        return {
            "q": f"""
            SELECT
                Account.AcctType account_type,
                Account.displaynamewithhierarchy as account_name,
                Account.acctnumber as account_number,
                Transaction.currency as currency,
                Entity.altname as company_name,
                AccountingPeriod.PeriodName as period_name,
                AccountingPeriod.StartDate as period_start_date,
                AccountingPeriod.EndDate as period_end_date,
                Transaction.postingperiod as posting_period,
                SUM(COALESCE(TransactionAccountingLine.amount, 0)) AS accumulated_amount,
                SUM(CASE WHEN TransactionAccountingLine.amount > 0 THEN TransactionAccountingLine.amount ELSE 0 END) AS credit_amount,
                SUM(CASE WHEN TransactionAccountingLine.amount < 0 THEN TransactionAccountingLine.amount ELSE 0 END) AS debit_amount
            From
                Account
                INNER JOIN TransactionAccountingLine ON (Account.ID = TransactionAccountingLine.Account)
                INNER JOIN Transaction ON (Transaction.ID = TransactionAccountingLine.Transaction)
                INNER JOIN AccountingPeriod ON (AccountingPeriod.ID = Transaction.PostingPeriod)
                LEFT JOIN Entity ON (Transaction.entity = Entity.id)
            WHERE TransactionAccountingLine.amount != 0 AND (Transaction.Posting = 'T')
                AND (
                    Account.AcctType IN (
                        'Income',
                        'COGS',
                        'Expense',
                        'OthIncome',
                        'OthExpense'
                    )
                )
            GROUP BY
                Account.AcctType,
                Account.displaynamewithhierarchy,
                Account.acctnumber,
                Transaction.currency,
                Entity.altname,
                Transaction.postingperiod,
                AccountingPeriod.PeriodName,
                AccountingPeriod.StartDate,
                AccountingPeriod.EndDate
            ORDER BY
                AccountingPeriod.StartDate ASC
        """
        }


class PriceLevelStream(NetsuiteDynamicStream):
    name = "price_level"
    primary_keys = ["id", "lastmodifieddate"]
    table = "pricelevel"
    replication_key = "lastmodifieddate"


class LocationsStream(NetSuiteStream):
    name = "locations"
    primary_keys = ["id", "lastmodifieddate"]
    table = "location"
    replication_key = "lastmodifieddate"
    select = """
        *
        """
    join = """
        INNER JOIN locationMainAddress ma ON(location.mainaddress = ma.nkey)
        """
    # Merge group and order by
    order_by = """
    ORDER BY location.lastmodifieddate ASC
    """
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("addressee", th.StringType),
        th.Property("addrtext", th.StringType),
        th.Property("country", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("includechildren", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("mainaddress", th.StringType),
        th.Property("name", th.StringType),
        th.Property("nkey", th.StringType),
        th.Property("override", th.StringType),
        th.Property("recordowner", th.StringType),
        th.Property("subsidiary", th.StringType),
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


class ItemStream(TransactionRootStream):
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
        th.Property("billingschedule", th.StringType),
        th.Property("createrevenueplanson", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("revenueallocationgroup", th.StringType),
        th.Property("revenuerecognitionrule", th.StringType),
        th.Property("revrecschedule", th.StringType),
        th.Property("revrecforecastrule", th.StringType),
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
        th.Property("quantityonhand", th.StringType),
    ).to_dict()


class ProfitLossReportStream(NetSuiteStream):
    name = "profit_loss_report"
    start_date_f = None
    end_date = None
    primary_keys = ["id"]
    select = """
        Entity.altname as name, Entity.firstname, Entity.lastname, Subsidiary.fullname as subsidiary, Transaction.tranid, Transaction.externalid, Transaction.abbrevtype as TransactionType, Transaction.postingperiod, Transaction.memo, Transaction.journaltype, Account.accountsearchdisplayname as split, Account.displaynamewithhierarchy as Categories, AccountingPeriod.PeriodName, TO_CHAR (AccountingPeriod.StartDate, 'YYYY-MM-DD HH24:MI:SS') as StartDate, Account.AcctType, TO_CHAR (Transaction.TranDate, 'YYYY-MM-DD HH24:MI:SS') as Date, Account.acctnumber as Num, TransactionLine.amount, Department.name as department, CONCAT(CONCAT(Transaction.id, '_'), TransactionLine.id) as id
        """
    table = "Transaction"
    join = """
        INNER JOIN TransactionLine ON ( TransactionLine.Transaction = Transaction.ID ) LEFT JOIN department ON ( TransactionLine.department = department.ID ) INNER JOIN Account ON ( Account.ID = TransactionLine.Account ) INNER JOIN AccountingPeriod ON ( AccountingPeriod.ID = Transaction.PostingPeriod ) LEFT JOIN Entity ON ( Transaction.entity = Entity.id ) LEFT JOIN subsidiary On ( Transactionline.subsidiary = Subsidiary.id )
        """
    custom_filter = "( Transaction.TranDate BETWEEN TO_DATE( '{start_date}', 'YYYY-MM-DD' ) AND TO_DATE( '{end_date}', 'YYYY-MM-DD' ) ) AND ( Transaction.Posting = 'T' ) AND ( Account.AcctType IN ( 'Income', 'COGS', 'Expense', 'OthIncome','OthExpense' ) ) AND TransactionLine.amount !=0"
    # Merge group and order by
    order_by = """
    ORDER BY CASE WHEN Account.AcctType = 'Income' THEN 1 WHEN Account.AcctType = 'OthIncome' THEN 2 WHEN Account.AcctType = 'COGS' THEN 3  WHEN Account.AcctType = 'Expense' THEN 4 ELSE 9 END ASC, AccountingPeriod.StartDate ASC
    """
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("accttype", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("categories", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("externalid", th.StringType),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("name", th.StringType),
        th.Property("num", th.StringType),
        th.Property("periodname", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("split", th.StringType),
        th.Property("startdate", th.DateTimeType),
        th.Property("tranid", th.StringType),
        th.Property("transactiontype", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("class", th.StringType),
        th.Property("department", th.StringType),
    ).to_dict()

    def get_next_page_token(self, response, previous_token):
        """Return a token for identifying next page or None if no more pages."""
        has_next = next(extract_jsonpath("$.hasMore", response.json()))
        offset = next(extract_jsonpath("$.offset", response.json()))
        offset += self.page_size

        if has_next:
            return offset

        totalResults = next(extract_jsonpath("$.totalResults", response.json()))
        if offset > totalResults:
            self.query_date = (parse(self.end_date) + timedelta(1)).replace(tzinfo=None)
            if self.query_date < datetime.utcnow():
                return self.query_date
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.query_date == next_page_token:
            next_page_token = 0
        params["offset"] = int(next_page_token or 0)
        params["limit"] = self.page_size
        return params


class GeneralLedgerReportStream(ProfitLossReportStream):
    name = "general_ledger_report"
    start_date_f = None
    end_date = None
    primary_keys = ["id"]
    entities_fallback = [
        {
            "name": "department",
            "select_replace": "Department.fullname as department, Department.id as departmentid",
            "join_replace": "LEFT JOIN department ON ( TransactionLine.department = department.ID )",
        },
        {
            "name": "classification",
            "select_replace": ", Classification.name as class, Classification.id as classid,",
            "join_replace": "LEFT JOIN Classification On ( Transactionline.class = Classification.id )",
        },
        {
            "name": "location",
            "select_replace": ", Location.name as locationname",
            "join_replace": "LEFT JOIN Location On ( Transactionline.location = Location.id )",
        },
        {
            "name": "currency",
            "select_replace": ", Currency.name as currency",
            "join_replace": "INNER JOIN Currency ON ( Currency.ID = Transaction.Currency )",
        },
    ]
    select = """
        Entity.altname as name, Entity.firstname, Entity.lastname, Subsidiary.fullname as subsidiary, Transaction.tranid, Transaction.externalid, Transaction.abbrevtype as TransactionType, Transaction.postingperiod, Transaction.memo, Transaction.journaltype, Account.accountsearchdisplayname as split, Account.displaynamewithhierarchy as Categories, TransactionLine.location as locationid, Location.name as locationname, AccountingPeriod.PeriodName, TO_CHAR (AccountingPeriod.StartDate, 'YYYY-MM-DD HH24:MI:SS') as StartDate, Account.AcctType, TO_CHAR (Transaction.TranDate, 'YYYY-MM-DD HH24:MI:SS') as Date, Account.acctnumber as Num, Account.id as accountid, TransactionLine.amount, TransactionLine.subsidiary as subsidiaryid, Department.fullname as department, Department.id as departmentid, (Transaction.id || '_' || TransactionLine.id) AS id, Currency.name as currency, Classification.name as class, Classification.id as classid, Transaction.transactionnumber, Transaction.trandisplayname, Entity.id as entityid, Entity.Type as entitytype
        """
    table = "Transaction"
    join = """
        INNER JOIN TransactionLine ON ( TransactionLine.Transaction = Transaction.ID ) LEFT JOIN department ON ( TransactionLine.department = department.ID ) INNER JOIN Account ON ( Account.ID = TransactionLine.Account ) INNER JOIN AccountingPeriod ON ( AccountingPeriod.ID = Transaction.PostingPeriod ) LEFT JOIN Entity ON ( Transaction.entity = Entity.id ) LEFT JOIN subsidiary On ( Transactionline.subsidiary = Subsidiary.id ) INNER JOIN Currency ON ( Currency.ID = Transaction.Currency )  LEFT JOIN Classification On ( Transactionline.class = Classification.id ) LEFT JOIN Location On ( Transactionline.location = Location.id )
        """

    @property
    def custom_filter(self):
        return "( Transaction.TranDate BETWEEN TO_DATE( '{start_date}', 'YYYY-MM-DD' ) AND TO_DATE( '{end_date}', 'YYYY-MM-DD' ) ) AND ( Transaction.Posting = 'T' ) AND TransactionLine.amount !=0"

    # Merge group and order by
    order_by = """
    ORDER BY AccountingPeriod.StartDate ASC
    """
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("accttype", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("categories", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("subsidiaryid", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("externalid", th.StringType),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("name", th.StringType),
        th.Property("num", th.StringType),
        th.Property("periodname", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("split", th.StringType),
        th.Property("startdate", th.DateTimeType),
        th.Property("tranid", th.StringType),
        th.Property("transactiontype", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("class", th.StringType),
        th.Property("classid", th.StringType),
        th.Property("department", th.StringType),
        th.Property("departmentid", th.StringType),
        th.Property("locationid", th.StringType),
        th.Property("locationname", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("accountid", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("trandisplayname", th.StringType),
        th.Property("entityid", th.StringType),
        th.Property("entitytype", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if "amount" in row:
            try:
                row["amount"] = float(row["amount"])
            except:
                pass
        return row


class TransactionsStream(TransactionRootStream):
    name = "transactions"
    primary_keys = ["id", "lastmodifieddate"]
    table = "transaction"
    replication_key = "lastmodifieddate"
    default_fields = set([
        "abbrevtype",
        "approvalstatus",
        "balsegstatus",
        "basetotalaftertaxes",
        "billingaddress",
        "billingstatus",
        "closedate",
        "createdby",
        "createddate",
        "currency",
        "customform",
        "customtype",
        "daysopen",
        "daysoverduesearch",
        "duedate",
        "entity",
        "entitytaxregnum",
        "exchangerate",
        "foreignamountpaid",
        "foreignamountunpaid",
        "foreigntotal",
        "id",
        "includeinforecast",
        "intercoadj",
        "isfinchrg",
        "isreversal",
        "lastmodifiedby",
        "lastmodifieddate",
        "legacytax",
        "memo",
        "nextapprover",
        "nexus",
        "number",
        "ordpicked",
        "paymenthold",
        "posting",
        "postingperiod",
        "printedpickingticket",
        "recordtype",
        "source",
        "status",
        "subsidiarytaxregnum",
        "taxdetailsoverride",
        "taxpointdate",
        "taxpointdateoverride",
        "taxregoverride",
        "terms",
        "tobeprinted",
        "totalaftertaxes",
        "trandate",
        "trandisplayname",
        "tranid",
        "transactionnumber",
        "type",
        "userevenuearrangement",
        "visibletocustomer",
        "void",
        "voided",
        "recurannually",
        "deferredrevenue",
        "externalid",
        "startdate",
        "enddate",
        "recurquarterly",
        "recurmonthly",
        "recurringbill",
        "recurweekly",
        "onetime",
        "revrecstartdate",
        "revrecenddate",
        "revrecschedule",
        "title",
        "sourcetransaction",
        "journaltype",
        "accountbasednumber",
        "actionitem",
        "actualproductionenddate",
        "actualproductionstartdate",
        "actualshipdate",
        "altsalesrangehigh",
        "altsalesrangelow",
        "projaltsalesamt",
        "isaomautomated",
        "asofdate",
        "assignee",
        "autocalculatelag",
        "bidclosedate",
        "bidopendate",
        "billingaccount",
        "billofmaterials",
        "billofmaterialsrevision",
        "isbudgetapproved",
        "committed",
        "bulkprocsubmission",
        "businesseventsprocessinghistory",
        "buyingreason",
        "buyingtimeframe",
        "completeddate",
        "draccount",
        "transferlocation",
        "effectivitybasedon",
        "email",
        "isactualprodenddateenteredmanually",
        "isactualprodstartdateenteredmanually",
        "totalcostestimate",
        "estgrossprofit",
        "estgrossprofitpercent",
        "estimatedbudget",
        "expectedclosedate",
        "pickupexpirationdate",
        "fax",
        "firmed",
        "forecasttype",
        "fxaccount",
        "fulfillmenttype",
        "basehandlingtaxamount",
        "handlingtaxamount",
        "incoterm",
        "intercostatus",
        "linkedinventorytransfer",
        "isbookspecific",
        "linkedir",
        "revision",
        "leadsource",
        "linkedtrackingnumberlist",
        "location",
        "manufacturingrouting",
        "maximumamount",
        "memdoc",
        "message",
        "minimumamount",
        "nextbilldate",
        "pricingtiers",
        "opportunity",
        "ordreceived",
        "ordertype",
        "outsourced",
        "outsourcingcharge",
        "intercotransaction",
        "partner",
        "foreignpaymentamountunused",
        "foreignpaymentamountused",
        "paymentmethod",
        "paymentoption",
        "payrollbatch",
        "pickedupdate",
        "picktype",
        "ispickupemailnotificationsent",
        "pickuphold",
        "otherrefnum",
        "priority",
        "probability",
        "projectedtotal",
        "promotioncombinations",
        "linkedpo",
        "purchaseorderinstructions",
        "rangehigh",
        "rangelow",
        "recognizedrevenue",
        "releaseddate",
        "revcommittingstatus",
        "reversaldate",
        "reversal",
        "revreconrevcommitment",
        "salesreadiness",
        "employee",
        "schedulingmethod",
        "shipcomplete",
        "shipdate",
        "shippingaddress",
        "baseshippingtaxamount",
        "shippingtaxamount",
        "entitystatus",
        "basetaxtotal",
        "taxtotal",
        "tosubsidiary",
        "trackingnumberlist",
        "tranisvsoebundle",
        "typebaseddocumentnumber",
        "outsourcingchargeunitprice",
        "useitemcostastransfercost",
        "vendor",
        "wavetype",
        "website",
        "weightedtotal",
        "winlossreason",
        "iswip",
    ])


class TransactionLinesStream(TransactionRootStream):
    name = "transaction_lines"
    primary_keys = ["id", "transaction"]
    replication_key = "linelastmodifieddate"
    table = "transactionline"
    start_date = None
    end_date = None

    join = """INNER JOIN Transaction ON ( Transaction.ID = TransactionLine.Transaction )"""

    default_fields = set([
        "recordtype",
        "accountinglinetype",
        "cleared",
        "closedate",
        "commitmentfirm",
        "costestimatetype",
        "createdfrom",
        "debitforeignamount",
        "department",
        "donotdisplayline",
        "eliminate",
        "entity",
        "expenseaccount",
        "foreignamount",
        "foreignamountpaid",
        "foreignamountunpaid",
        "fxamountlinked",
        "hasfulfillableitems",
        "id",
        "invsoebundle",
        "isbillable",
        "isclosed",
        "iscogs",
        "iscustomglline",
        "isfullyshipped",
        "isfxvariance",
        "isinventoryaffecting",
        "isrevrectransaction",
        "deferrevrec",
        "revrecenddate",
        "revrecstartdate",
        "revrecterminmonths",
        "revrecschedule",
        "revcommittingtransaction",
        "revenueelement",
        "kitcomponent",
        "linelastmodifieddate",
        "linesequencenumber",
        "location",
        "mainline",
        "matchbilltoreceipt",
        "needsrevenueelement",
        "netamount",
        "oldcommitmentfirm",
        "processedbyrevcommit",
        "quantitybilled",
        "quantityrejected",
        "quantityshiprecv",
        "quantity",
        "subsidiary",
        "taxline",
        "transaction",
        "transactiondiscount",
        "uniquekey",
        "item",
        "itemtype",
        "isallocation",
        "memo",
        "price",
        "subscription",
        "subscriptionline",
        "transactionlinetype",
        "acknowledgefulfillinstruction",
        "actualshipdate",
        "quantityallocated",
        "quantitydemandallocated",
        "allocationalert",
        "vsoeprice",
        "vsoesopgroup",
        "amortizationenddate",
        "amortizationsched",
        "amortizstartdate",
        "assembly",
        "assemblycomponent",
        "assemblyunits",
        "quantitybackordered",
        "isbillable",
        "billingschedule",
    ])

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        # Avoid using my new logic if the flag is off
        if not self.config.get("transaction_lines_monthly"):
            return super().prepare_request_payload(context, next_page_token)

        filters = [
            "( Transaction.type IN ( 'RevArrng', 'CustCred', 'CustPymt', 'CustDep', 'CustRfnd', 'CustInvc', 'SalesOrd' ) )"
        ]
        # get order query
        prefix = self.table
        order_by = f"ORDER BY {prefix}.{self.replication_key}"

        # get filter query
        start_date = self.start_date or self.get_starting_time(context)
        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"

        if start_date:
            start_date_str = start_date.strftime(time_format)

            self.start_date = start_date
            self.end_date = start_date + self.time_jump
            end_date_str = self.end_date.strftime(time_format)
            timeframe = f"{start_date_str} to {end_date_str}"

            filters.append(f"{prefix}.{self.replication_key}>={start_date_str} AND {prefix}.{self.replication_key}<{end_date_str}")

            filters = "WHERE " + " AND ".join(filters)

        selected_properties = self.get_selected_properties()
        selected_properties.remove('transactionline.recordtype AS recordtype')
        select = "Transaction.type as recordtype, " + ", ".join(selected_properties)

        join = self.join if self.join else ""

        payload = dict(
            q=f"SELECT {select} FROM {self.table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({timeframe})")
        return payload


class TransactionAccountingLinesStream(NetSuiteStream):
    table = "TransactionAccountingLine"
    primary_keys = ["accountingbook", "transaction", "transactionline"]
    name = "transaction_accounting_lines"
    select = "*"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("accountingbook", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("credit", th.StringType),
        th.Property("debit", th.StringType),
        th.Property("netamount", th.StringType),
        th.Property("amountlinked", th.StringType),
        th.Property("amountpaid", th.StringType),
        th.Property("amountunpaid", th.StringType),
        th.Property("overheadParentItem", th.StringType),
        th.Property("paymentamountunused", th.StringType),
        th.Property("paymentamountused", th.StringType),
        th.Property("processedbyrevcommit", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("transactionline", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
    ).to_dict()


class CurrenciesStream(NetsuiteDynamicStream):
    name = "currencies"
    primary_keys = ["id"]
    table = "currency"


class DepartmentsStream(NetsuiteDynamicStream):
    name = "departments"
    primary_keys = ["id"]
    table = "department"
    replication_key = "lastmodifieddate"


class SubsidiariesStream(NetsuiteDynamicStream):
    name = "subsidiaries"
    primary_keys = ["id"]
    table = "subsidiary"
    replication_key = "lastmodifieddate"


class AccountsStream(NetsuiteDynamicStream):
    name = "accounts"
    primary_keys = ["id"]
    table = "account"


class ConsolidatedExchangeRates(NetsuiteDynamicStream):
    name = "consolidated_exchange_rates"
    primary_keys = ["id"]
    table = "consolidatedexchangerate"


class AccountingPeriodsStream(NetsuiteDynamicStream):
    name = "accounting_periods"
    primary_keys = ["id"]
    table = "accountingperiod"


class CustomersStream(NetsuiteDynamicStream):
    name = "customers"
    primary_keys = ["id"]
    table = "customer"


class DeletedRecordsStream(NetSuiteStream):
    name = "deleted_records"
    table = "deletedrecord"
    replication_key = "deleteddate"
    primary_keys = ["recordid", "recordtypeid", "scriptid"]

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("recordid", th.StringType),
        th.Property("recordtypeid", th.StringType),
        th.Property("scriptid", th.StringType),
        th.Property("context", th.StringType),
        th.Property("deletedby", th.StringType),
        th.Property("deleteddate", th.DateTimeType),
        th.Property("iscustomlist", th.StringType),
        th.Property("iscustomrecord", th.StringType),
        th.Property("iscustomtransaction", th.StringType),
        th.Property("type", th.StringType),
    ).to_dict()


class RevenueElementStream(NetSuiteStream):
    name = "revenueelement"
    primary_keys = ["id"]
    table = "revenueelement"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("accountingbook", th.StringType),
        th.Property("allocationamount", th.StringType),
        th.Property("class", th.StringType),
        th.Property("createrevenueplanson", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("deferralaccount", th.StringType),
        th.Property("discountedsalesamount", th.StringType),
        th.Property("elementdate", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("forecastenddate", th.StringType),
        th.Property("forecaststartdate", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("fxproratediscsalesamt", th.StringType),
        th.Property("isbomitemtype", th.StringType),
        th.Property("item", th.StringType),
        th.Property("itemisautoexpand", th.StringType),
        th.Property("lastmodifieddate", th.StringType),
        th.Property("originalchangeorderdiscamount", th.StringType),
        th.Property("originalchangeorderquantity", th.StringType),
        th.Property("postingdiscountapplied", th.StringType),
        th.Property("proratediscsalesamt", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("recognitionaccount", th.StringType),
        th.Property("recordnumber", th.StringType),
        th.Property("referenceid", th.StringType),
        th.Property("requiresrevenueplanupdate", th.StringType),
        th.Property("revenuearrangement", th.StringType),
        th.Property("revenueplanstatus", th.StringType),
        th.Property("revenuerecognitionrule", th.StringType),
        th.Property("revrecenddate", th.StringType),
        th.Property("revrecforecastrule", th.StringType),
        th.Property("revreclassfxaccount", th.StringType),
        th.Property("revrecstartdate", th.StringType),
        th.Property("salesamount", th.StringType),
        th.Property("source", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("treatmentoverride", th.StringType),
        th.Property("effectivestartdate", th.DateTimeType),
        th.Property("effectiveenddate", th.DateTimeType),
        th.Property("externalid", th.StringType),
        th.Property("revenueallocationgroup", th.StringType),
        th.Property("revenueallocationratio", th.StringType),
        th.Property("terminmonths", th.StringType),
        th.Property("termindays", th.StringType),
        th.Property("subscriptionline", th.StringType),
    ).to_dict()


class RelatedTransactionLinesStream(TransactionRootStream):
    name = "related_transaction_lines"
    table = "NextTransactionLineLink"
    start_date_f = None
    end_date = None
    primary_keys = ["compositeid"]
    replication_key = "lastmodifieddate"
    select = """
        DISTINCT
            NextTransactionLineLink.PreviousLine as lineno,
            NextTransactionLineLink.PreviousDoc AS transactionid,
            NextTransactionLineLink.NextDoc AS relatedtransactionid,
            NextTransactionLineLink.NextLine as relatedlineno,
            NextTransactionLineLink.ForeignAmount,
            NextTransactionLineLink.LastModifiedDate,
            NextTransactionLineLink.LinkType,
            NextTransactionLineLink.NextType as relatedtransactiontype,
            NextTransactionLineLink.PreviousType as transactiontype
    """

    schema = th.PropertiesList(
        th.Property("compositeid", th.StringType),
        th.Property("lineno", th.StringType),
        th.Property("transactionid", th.StringType),
        th.Property("relatedtransactionid", th.StringType),
        th.Property("relatedlineno", th.StringType),
        th.Property("foreignamount", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("linktype", th.StringType),
        th.Property("relatedtransactiontype", th.StringType),
        th.Property("transactiontype", th.StringType),
    ).to_dict()


    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["compositeid"] = (
            f"{row['transactionid']}-{row['lineno']}-{row['relatedtransactionid']}"
        )
        return row


class PurchaseOrdersStream(NetSuiteStream):
    name = "purchase_orders"
    primary_keys = ["id", "lastmodifieddate"]
    select = """
        CONCAT(CONCAT(t.id, '_'), tl.id) as id,
        TO_CHAR (t.trandate, 'YYYY-MM-DD HH24:MI:SS') AS trandate,
        TO_CHAR (t.lastmodifieddate, 'YYYY-MM-DD HH24:MI:SS') AS lastmodifieddate,
        t.recordtype, tl.item AS ns_item_id, tl.class,
        t.transactionnumber,t.tranid,t.trandisplayname, tl.creditforeignamount,tl.foreignamount,
        tl.netamount,tl.debitforeignamount,t.foreigntotal,tl.quantitybilled,tl.quantitypacked,
        tl.quantitypicked,tl.quantityrejected,tl.quantityshiprecv,tl.rate,t.status,t.posting,
        tl.quantity, t.id AS transaction_id, tl.id AS transaction_line_id
        """
    table = "transaction t"
    join = """
        INNER JOIN transactionline tl ON t.id = tl.transaction
        """
    custom_filter = "t.recordtype = 'purchaseorder'"
    replication_key_prefix = "t"

    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("trandisplayname", th.StringType),
        th.Property("creditforeignamount", th.StringType),
        th.Property("foreignamount", th.StringType),
        th.Property("netamount", th.StringType),
        th.Property("debitforeignamount", th.StringType),
        th.Property("foreigntotal", th.StringType),
        th.Property("class", th.StringType),
        th.Property("ns_item_id", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("quantitybilled", th.StringType),
        th.Property("quantitypacked", th.StringType),
        th.Property("quantitypicked", th.StringType),
        th.Property("quantityrejected", th.StringType),
        th.Property("quantityshiprecv", th.StringType),
        th.Property("rate", th.StringType),
        th.Property("status", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("trandate", th.DateTimeType),
        th.Property("transaction_id", th.StringType),
        th.Property("transaction_line_id", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
    ).to_dict()


class SubscriptionsStream(NetsuiteDynamicStream):
    name = "subscriptions"
    primary_keys = ["id"]
    table = "subscription"


class SubscriptionLinesStream(NetsuiteDynamicStream):
    name = "subscription_lines"
    primary_keys = ["id"]
    table = "subscriptionline"


class SubscriptionPlansStream(NetsuiteDynamicStream):
    name = "subscription_plans"
    primary_keys = ["id"]
    table = "subscriptionplan"


class SubscriptionTermsStream(NetsuiteDynamicStream):
    name = "subscription_terms"
    primary_keys = ["id"]
    table = "subscriptionterm"


class SalesInvoicedStream(NetSuiteStream):
    name = "sales_invoiced"
    primary_keys = ["id"]
    table = "salesinvoiced"

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amountnet", th.StringType),
        th.Property("class", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("department", th.StringType),
        th.Property("costestimate", th.StringType),
        th.Property("estgrossprofit", th.StringType),
        th.Property("estgrossprofitpercent", th.StringType),
        th.Property("item", th.StringType),
        th.Property("location", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("partner", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("promotioncombinations", th.StringType),
        th.Property("itemcount", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("tranline", th.StringType),
        th.Property("type", th.StringType),
        th.Property("uniquekey", th.StringType),
    ).to_dict()


class SalesOrderedStream(NetSuiteStream):
    name = "sales_ordered"
    primary_keys = ["id"]
    table = "salesordered"

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amountnet", th.StringType),
        th.Property("class", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("department", th.StringType),
        th.Property("costestimate", th.StringType),
        th.Property("estgrossprofit", th.StringType),
        th.Property("estgrossprofitpercent", th.StringType),
        th.Property("item", th.StringType),
        th.Property("location", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("partner", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("promotioncombinations", th.StringType),
        th.Property("itemcount", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("tranline", th.StringType),
        th.Property("type", th.StringType),
        th.Property("uniquekey", th.StringType),
    ).to_dict()


class InvoiceGroupStream(NetsuiteDynamicStream):
    name = "invoice_group"
    primary_keys = ["id"]
    table = "invoicegroup"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("amountdue", th.StringType),
        th.Property("amountpaid", th.StringType),
        th.Property("billaddresslist", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("customer", th.StringType),
        th.Property("customername", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("discounttotal", th.StringType),
        th.Property("duedate", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("groupedbypo", th.StringType),
        th.Property("handlingcost", th.StringType),
        th.Property("id", th.StringType),
        th.Property("invoicegroupnumber", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("memo", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("datedriven", th.StringType),
        th.Property("dayofmonthnetdue", th.StringType),
        th.Property("daysuntilnetdue", th.StringType),
        th.Property("duenextmonthifwithindays", th.StringType),
        th.Property("ponumber", th.StringType),
        th.Property("shippingcost", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("taxtotal", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("total", th.StringType),
    ).to_dict()


class BillingSchedulesStream(NetSuiteStream):
    name = "billing_schedules"
    primary_keys = ["id"]
    table = "billingschedule"
    select = "*"

    schema = th.PropertiesList(
        th.Property("applytosubtotal", th.StringType),
        th.Property("recurrence", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("inarrears", th.StringType),
        th.Property("initialamount", th.StringType),
        th.Property("initialterms", th.StringType),
        th.Property("id", th.StringType),
        th.Property("billforactuals", th.StringType),
        th.Property("milestone", th.StringType),
        th.Property("name", th.StringType),
        th.Property("job", th.StringType),
        th.Property("ispublic", th.StringType),
        th.Property("numberremaining", th.StringType),
        th.Property("frequency", th.DateTimeType),
        th.Property("recurrencepattern", th.StringType),
        th.Property("recurrenceterms", th.StringType),
        th.Property("repeatevery", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("scheduletype", th.StringType),
    ).to_dict()


class PriceBookStream(NetsuiteDynamicStream):
    name = "pricebooks"
    primary_keys = ["id"]
    table = "pricebook"


class PriceBookLineIntervalStream(NetSuiteStream):
    name = "pricebook_line_intervals"
    primary_keys = ["id"]
    table = "pricebooklineinterval"
    select = "*"

    schema = th.PropertiesList(
        th.Property("frequency", th.StringType),
        th.Property("multiplierline", th.StringType),
        th.Property("id", th.StringType),
        th.Property("startoffsetunit", th.StringType),
        th.Property("item", th.StringType),
        th.Property("linenumber", th.StringType),
        th.Property("chargetype", th.StringType),
        th.Property("pricebook", th.StringType),
        th.Property("overagefrequency", th.StringType),
        th.Property("overagepriceplan", th.StringType),
        th.Property("overagerepeatevery", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("prorateby", th.StringType),
        th.Property("repeatevery", th.StringType),
        th.Property("isrequired", th.BooleanType),
        th.Property("startoffsetvalue", th.StringType),
    ).to_dict()


class PriceModelTypeStream(NetSuiteStream):
    name = "price_model_type"
    primary_keys = ["key"]
    table = "pricemodeltype"
    select = "*"

    schema = th.PropertiesList(
        th.Property("isinactive", th.BooleanType),
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()


class PricePlanStream(NetsuiteDynamicStream):
    name = "price_plan"
    primary_keys = ["id"]
    table = "priceplan"


class PriceTiersStream(NetSuiteStream):
    name = "price_tiers"
    primary_keys = ["id"]
    table = "pricetiers"
    select = "*"

    schema = th.PropertiesList(
        th.Property("externalid", th.StringType),
        th.Property("fromval", th.StringType),
        th.Property("id", th.StringType),
        th.Property("lineid", th.StringType),
        th.Property("maxamount", th.StringType),
        th.Property("minamount", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("pricingoption", th.StringType),
    ).to_dict()


class SubscriptionChangeOrderStream(NetsuiteDynamicStream):
    name = "subscription_change_order"
    primary_keys = ["id"]
    table = "subscriptionchangeorder"


class ChangeOrderLineStream(NetSuiteStream):
    name = "change_order_line"
    primary_keys = ["subscriptionchangeorder", "subscriptionline"]
    table = "changeorderline"
    select = "*"

    schema = th.PropertiesList(
        th.Property("discount", th.StringType),
        th.Property("item", th.StringType),
        th.Property("newdiscount", th.StringType),
        th.Property("newpriceplan", th.StringType),
        th.Property("newquantity", th.NumberType),
        th.Property("newstatus", th.StringType),
        th.Property("subscriptionchangeorder", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("status", th.StringType),
        th.Property("subscriptionline", th.StringType),
        th.Property("sequence", th.IntegerType),
    ).to_dict()


# This stream doesn't seem to have a PK nor have enough info to provide any real value. Commenting out for now
# Until I can learn more about it
# class SubscriptionChangeOrderNewLineStream(NetSuiteStream):
#     name = "subscription_change_order_new_line"
#     primary_keys = ["uuid"]
#     table = "subscriptionchangeordernewline"
#     select = "*"
#
#     schema = th.PropertiesList(
#         th.Property("uuid", th.StringType),
#         th.Property("discount", th.StringType),
#         th.Property("include", th.StringType),
#         th.Property("multiplierline", th.IntegerType),
#         th.Property("itemdisplay", th.StringType),
#         th.Property("sequence", th.StringType),
#         th.Property("subscriptionlinetype", th.StringType),
#         th.Property("subscriptionchangeorder", th.StringType),
#         th.Property("priceplan", th.StringType),
#         th.Property("quantity", th.NumberType),
#         th.Property("required", th.StringType),
#         th.Property("status", th.StringType),
#     ).to_dict()
#
#     def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
#         # NOTE: temporarily forcing a pk
#         row["uuid"] = str(uuid4())
#         return row


class SubscriptionChangeOrderRenewalStepsStream(NetSuiteStream):
    name = "subscription_change_order_renewal_steps"
    primary_keys = ["subscription", "subscriptionchangeorder"]
    table = "subscriptionchangeorderrenewalsteps"
    select = "*"

    schema = th.PropertiesList(
        th.Property("error", th.StringType),
        th.Property("subscriptionchangeorder", th.StringType),
        th.Property("step", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("transaction", th.StringType),
    ).to_dict()


class SubscriptionChangeOrderStatusStream(NetSuiteStream):
    name = "subscription_change_order_status"
    primary_keys = ["key"]
    table = "subscriptionchangeorderstatus"
    select = "*"

    schema = th.PropertiesList(
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

class SubscriptionLineRevisionStream(NetSuiteStream):
    name = "subscription_line_revision"
    primary_keys = ["subscription", "subscriptionline", "subscriptionrevision"]
    table = "subscriptionlinerevision"
    select = "*"

    schema = th.PropertiesList(
        th.Property("appliedtochangeorder", th.BooleanType),
        th.Property("changeordereffectivedate", th.DateType),
        th.Property("createdby", th.StringType),
        th.Property("createdfromvoid", th.BooleanType),
        th.Property("datecreated", th.DateType),
        th.Property("deltaamount", th.NumberType),
        th.Property("deltaquantity", th.NumberType),
        th.Property("externalid", th.StringType),
        th.Property("id", th.StringType),
        th.Property("overagepriceplan", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("recurringamount", th.NumberType),
        th.Property("revenueelement", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("changeorder", th.StringType),
        th.Property("subscriptionline", th.StringType),
        th.Property("subscriptionrevision", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("totalcontractvalue", th.NumberType),
    ).to_dict()


class SubscriptionLineStatusStream(NetSuiteStream):
    name = "subscription_line_status"
    primary_keys = ["key"]
    table = "subscriptionlinestatus"
    select = "*"

    schema = th.PropertiesList(
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

class SubscriptionLineTypeStream(NetSuiteStream):
    name = "subscription_line_type"
    primary_keys = ["key"]
    table = "subscriptionlinetype"
    select = "*"

    schema = th.PropertiesList(
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

class SubscriptionPriceIntervalStream(NetSuiteStream):
    name = "subscription_price_interval"
    primary_keys = ["id"]
    table = "subscriptionpriceinterval"
    select = "*"

    schema = th.PropertiesList(
        th.Property("catalogtype", th.StringType),
        th.Property("frequency", th.StringType),
        th.Property("includedquantity", th.NumberType),
        th.Property("multiplierline", th.StringType),
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("item", th.StringType),
        th.Property("linenumber", th.StringType),
        th.Property("chargetype", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("overagefrequency", th.StringType),
        th.Property("overagepriceplan", th.StringType),
        th.Property("overagerepeatevery", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("prorateby", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("recurringamount", th.NumberType),
        th.Property("repeatevery", th.StringType),
        th.Property("startdate", th.DateType),
        th.Property("startoffsetvalue", th.StringType),
    ).to_dict()


class SalesTaxItemStream(NetsuiteDynamicStream):
    name = "sales_tax_item"
    primary_keys = []
    table = "salestaxitem"


class TaxItemGroupStream(NetSuiteStream):
    name = "tax_item_group"
    primary_keys = ["id"]
    table = "taxitemtaxgroup"
    select = "*"

    schema = th.PropertiesList(
        th.Property("description", th.StringType),
        th.Property("isinactive", th.BooleanType),
        th.Property("id", th.StringType),
        th.Property("taxtype", th.StringType),
    ).to_dict()


class TaxTypeStream(NetsuiteDynamicStream):
    name = "tax_type"
    primary_keys = ["id"]
    table = "taxtype"
