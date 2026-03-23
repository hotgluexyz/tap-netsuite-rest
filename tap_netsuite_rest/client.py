"""REST client handling, including NetSuiteStream base class."""

import logging
import backoff
import requests
import pendulum
import copy
import re

from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Any, Callable, Dict, Optional, cast, Iterable, List

from memoization import cached
from oauthlib import oauth1
from requests_oauthlib import OAuth1Session
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk import typing as th
from pendulum import parse
from requests.exceptions import HTTPError
import json
from http.client import RemoteDisconnected
from dateutil.relativedelta import relativedelta
import pytz
from copy import deepcopy
from hotglue_singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error,
)
from hotglue_singer_sdk.exceptions import InvalidStreamSortException
import singer
from singer import StateMessage


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
logging.getLogger("backoff").setLevel(logging.CRITICAL)

class RetryRequest(Exception):
    pass  
class NetSuiteStream(RESTStream):
    """NetSuite stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        return f"https://{url_account}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

    records_jsonpath = "$.items[*]"
    type_filter = None
    page_size = 1000
    path = None
    rest_method = "POST"
    query_date = None
    select = None
    join = None
    custom_filter = None
    replication_key_prefix = None
    select_prefix = None
    order_by = None
    append_select = None
    time_jump = relativedelta(months=1)
    always_add_default_fields = False
    query_table = None

    def get_replication_key_conditions(self, context):
        """Return a list of replication-key filter strings, or None to use default (get_starting_time / query_date)."""
        return None

    def __init__(
        self,
        tap,
        name = None,
        schema = None,
        path = None,
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(name=name, schema=schema, tap=tap, path=path)
        self.record_ids = set()
        self.invalid_fields = []

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Prefer"] = "transient"

        return headers

    def get_session(self) -> requests.Session:
        """Get requests session.

        Returns:
            The `requests.Session`_ object for HTTP requests.

        .. _requests.Session:
            https://docs.python-requests.org/en/latest/api/#request-sessions
        """
        ns_account = self.config["ns_account"].replace("-", "_").upper()

        return OAuth1Session(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        # Generate a new OAuth1 session
        client = self.get_session()

        request = cast(
            requests.PreparedRequest,
            client.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                ),
            ),
        )

        return request

    def _is_transaction_monthly_stream(self) -> bool:
        return (
            (isinstance(self, TransactionRootStream) or isinstance(self, BulkParentStream))
            and self.config.get("transaction_lines_monthly")
            and self.replication_key
        )

    def _reduce_time_jump(self) -> bool:
        reductions = [
            (relativedelta(months=1), relativedelta(weeks=1), "Dropping time_jump to 1 week"),
            (relativedelta(weeks=1), relativedelta(days=3), "Dropping time_jump to 3 days"),
            (relativedelta(days=3), relativedelta(days=1), "Dropping time_jump to 1 day"),
            (relativedelta(days=1), relativedelta(hours=12), "Dropping time_jump to 12 hours"),
            (relativedelta(hours=12), relativedelta(hours=6), "Dropping time_jump to 6 hours"),
            (relativedelta(hours=6), relativedelta(hours=1), "Dropping time_jump to 1 hours"),
            (relativedelta(hours=1), relativedelta(minutes=30), "Dropping time_jump to 30 min"),
            (relativedelta(minutes=30), relativedelta(minutes=5), "Dropping time_jump to 5 min"),
            (relativedelta(minutes=5), relativedelta(minutes=1), "Dropping time_jump to 1 min"),
        ]
        for current_delta, next_delta, message in reductions:
            if self.time_jump == current_delta:
                self.logger.info(message)
                self.time_jump = next_delta
                return True

        self.logger.error(
            "Even with minimum delta we are getting more than 100k records! We will likely infinite loop."
        )
        return False

    def _advance_inventory_item_location_filter(self) -> Optional[int]:
        max_item_value = 100_000  # TODO: this probably should be more dynamic
        interval_increment = 2500  # TODO: maybe we should lower this even further or make it dynamic
        if self.custom_filter == f"item >= {max_item_value}":
            return None

        current_range = self.custom_filter.split("AND")
        min_value = int(current_range[0].split(">=")[1])
        max_value = int(current_range[1].split("<")[1])

        if min_value == max_item_value:
            self.custom_filter = f"item >= {max_item_value}"
        else:
            self.custom_filter = (
                f"item >= {min_value + interval_increment} AND item < {max_value + interval_increment}"
            )
        return 0

    def _advance_transaction_window(self) -> Optional[int]:
        today = datetime.now().replace(tzinfo=pytz.UTC)
        if self.end_date >= today:
            self.logger.info("Reached the end of the line! Stopping")
            return None

        if self.time_jump in [
            relativedelta(minutes=30),
            relativedelta(minutes=5),
            relativedelta(minutes=1),
        ]:
            reset_time_jump = self.start_date.hour != (self.start_date + self.time_jump).hour
        else:
            reset_time_jump = self.start_date.month != (self.start_date + self.time_jump).month

        self.start_date = self.start_date + self.time_jump
        if reset_time_jump:
            self.logger.info("Resetting time_jump to 1 month for next iteration...")
            self.time_jump = relativedelta(months=1)
        self.logger.info(f"Reached end of data for current period. Moving start date to {self.start_date}")
        return 0

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_next = next(extract_jsonpath("$.hasMore", response.json()))
        offset = next(extract_jsonpath("$.offset", response.json()))
        offset += self.page_size

        totalResults = next(extract_jsonpath("$.totalResults", response.json()))
        self.logger.info(f"[{self.name}] Total results = {totalResults}. Offset = {offset}")

        if not self.stream_state.get("replication_key") and self.name == "inventory_item_locations" and totalResults > 100_000:
            # NOTE: this is to avoid a case where we miss data, better to report an error than to miss data
            raise Exception("totalResults is greater than 100,000 records. This should not happen.")

        if has_next:
            if self._is_transaction_monthly_stream() and totalResults > 10000:
                self.logger.info(
                    f"totalResults = {totalResults}, time_jump = {self.time_jump}"
                )
                if self._reduce_time_jump():
                    return 0
            return offset

        if not self.stream_state.get("replication_key") and self.name == "inventory_item_locations" and not has_next:
            return self._advance_inventory_item_location_filter()


        if self._is_transaction_monthly_stream() and not has_next:
            return self._advance_transaction_window()

        if not has_next and offset < totalResults:
            if self.replication_key:
                json_path = f"$.items[-1].{self.replication_key}"
                last_dt = next(extract_jsonpath(json_path, response.json()))
                try:
                    self.query_date = pendulum.parse(last_dt)
                except Exception:
                    self.query_date = datetime.strptime(last_dt, "%d/%m/%Y")
                return offset
        return None

    def get_starting_timestamp(self, context):
        value = self.get_starting_replication_key_value(context)

        if value is None:
            return None

        if not self.is_timestamp_replication_key:
            raise ValueError(
                f"The replication key {self.replication_key} is not of timestamp type"
            )
        try:
            return cast(datetime, pendulum.parse(value))
        except pendulum.exceptions.ParserError:
            formats = [
                'MM/DD/YYYY',
            ]
            for fmt in formats:
                try:
                    parsed_date = pendulum.from_format(value, fmt)
                    return parsed_date
                except ValueError:
                    continue
            else:
                raise ValueError(f"Could not parse date: {value}")

    @cached
    def get_starting_time(self, context):
        start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["offset"] = (next_page_token or 0) % 100000
        params["limit"] = self.page_size
        return params

    def get_date_boundaries(self):
        rep_key = self.stream_state
        window = self.config.get("window_days")
        report_periods = self.config.get("report_periods", 3)
        if self.query_date:
            start_date = self.query_date
            self.start_date_f = start_date.strftime("%Y-%m-%d")
        elif (self.name == "general_ledger_report" and self.config.get("gl_full_sync")) or ("replication_key" not in rep_key):
            start_date = parse(self.config["start_date"])
            self.start_date_f = start_date.strftime("%Y-%m-01")
        elif self.name == "general_ledger_report":
            today = date.today()
            beginning_of_month = today.replace(day=1)
            start_date = (beginning_of_month - relativedelta(months=report_periods - 1))
            self.start_date_f = start_date.strftime("%Y-%m-%d")
            self.logger.info(f"Not initial sync, fetching GL entries for last {report_periods} months, starting from {self.start_date_f}")
        else:
            start_date = self.get_starting_time({})
            self.start_date_f = start_date.strftime("%Y-%m-01")
        self.end_date = (start_date + timedelta(window)).strftime("%Y-%m-%d")

    def format_date_query(self, field_name):
        prefix = self.select_prefix or self.table
        return f"TO_CHAR ({prefix}.{field_name}, 'YYYY-MM-DD HH24:MI:SS') AS {field_name}"

    def get_selected_properties(self, select_all_by_default=False):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and (value.selected if not select_all_by_default else True) and key[1] not in self.invalid_fields:
                field_name = key[-1]
                prefix = self.select_prefix or self.table
                field_type = self.schema["properties"].get(field_name) or dict()
                if field_type.get("format") == "date-time":
                    field_name = self.format_date_query(field_name)
                else:
                    field_name = f"{prefix}.{field_name} AS {field_name}"
                selected_properties.append(field_name)
        return selected_properties

    def _get_replication_filters(
        self, context: Optional[dict], time_format: str
    ) -> tuple[list[str], str]:
        filters: list[str] = []
        order_by = ""
        if self.replication_key and "_report" not in self.name:
            prefix = self.replication_key_prefix or self.table
            order_by = f"ORDER BY {prefix}.{self.replication_key}"

            conditions = self.get_replication_key_conditions(context)
            if conditions is not None:
                filters.extend(conditions)
            else:
                start_date = self.query_date or self.get_starting_time(context)
                if start_date:
                    start_date_str = start_date.strftime(time_format)
                    filters.append(f"{prefix}.{self.replication_key}>{start_date_str}")
        return filters, order_by

    def _get_stream_filters(self) -> list[str]:
        filters = []
        if "_report" in self.name and self.custom_filter:
            self.get_date_boundaries()
            custom_filter = self.custom_filter.format(
                start_date=self.start_date_f, end_date=self.end_date
            )
            filters.append(custom_filter)
            return filters

        if self.type_filter:
            filters.append(f"(Type='{self.type_filter}')")
        if self.custom_filter:
            filters.append(self.custom_filter)
        return filters

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"
        filters, order_by = self._get_replication_filters(context, time_format)

        if self.replication_key_prefix is None and self.order_by is not None:
            order_by = self.order_by

        filters.extend(self._get_stream_filters())

        if filters:
            filters = "WHERE " + " AND ".join(filters)
        else:
            filters = ""

        selected_properties = self.get_selected_properties()

        if self.select:
            select = self.select
        else:
            select = ", ".join(selected_properties)

        if self.append_select:
            select = self.append_select + select

        if not select:
            select = "*"

        join = self.join if self.join else ""
        table = self.query_table or self.table

        payload = dict(
            q=f"SELECT {select} FROM {table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({payload['q']})")
        return payload

    def _apply_entities_fallback(self, response: requests.Response) -> None:
        if not hasattr(self, "entities_fallback") or not self.entities_fallback:
            return

        for entity in self.entities_fallback:
            fallback_error = "Record '{}' was not found.".lower().format(entity["name"])
            if fallback_error not in response.text.lower():
                continue

            self.logger.info(f"Missing {entity['name']} permission. Retrying with updated query...")
            if "select_replace" in entity:
                self.select = self.select.replace(entity["select_replace"], "")
            if "join_replace" in entity:
                self.join = self.join.replace(entity["join_replace"], "")
            if entity["name"] == "accountingbook":
                self.gl_use_only_primary_accounting_book = lambda: False
            raise RetryRequest(response.text)

    def _drop_unsearchable_fields(self, response: requests.Response) -> None:
        if "Search error occurred: Field" not in response.text and "Invalid search query" not in response.text:
            return

        error_details = response.json()["o:errorDetails"][0]["detail"]
        field_matches = re.finditer(r"(?i)field '(\w+)'", error_details)
        for field_match in field_matches:
            field_name = field_match.group(1)
            if not hasattr(self, "invalid_fields"):
                self.invalid_fields = []
            if field_name not in self.invalid_fields:
                self.invalid_fields.append(field_name)
                self.logger.info(f"Field {field_name} is not searchable. Retrying with updated query...")
        if self.invalid_fields:
            self.logger.info(
                f"Following fields are not searchable: {self.invalid_fields}, skipping them from stream {self.name} query"
            )
            raise RetryRequest(response.text)

    def _raise_status_error(self, response: requests.Response, error_type: str) -> None:
        msg = (
            f"{response.status_code} {error_type}: "
            f"{response.reason} for path: {self.path}"
            f"Response: {response.text}"
        )
        if error_type == "Server Error":
            raise RetriableAPIError(msg)
        raise FatalAPIError(msg)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code == 400:
            self._apply_entities_fallback(response)
            self._drop_unsearchable_fields(response)

        if 500 <= response.status_code < 600 or response.status_code in [401, 429]:
            self._raise_status_error(response, "Server Error")
        elif 400 <= response.status_code < 500:
            self._raise_status_error(response, "Client Error")

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                HTTPError,
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                RemoteDisconnected,
                RetryRequest,
            ),
            max_tries=10,
            factor=3,
        )(func)
        return decorator

    def last_day_of_month(self, any_day):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = any_day.replace(day=28) + timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - timedelta(days=next_month.day)

    def make_request(self, context, next_page_token):
        # Retry the request with updated query
        # NOTE: We have to call prepare_request again to properly build the OAuth1 headers or we get 401
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = self._request(prepared_request, context)
        return resp

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # override the request_records method to handle updated query
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self.make_request)

        while not finished:
            resp = decorated_request(context, next_page_token)
            
            # store primary keys to avoid duplicated records if primary keys is available
            for row in self.parse_response(resp):
                # need to use final_row otherwise the pk may be missing
                final_row = self.post_process(row, context)
                if self.primary_keys:
                    if len(self.primary_keys) == 1:
                        pk = final_row[self.primary_keys[0]]
                    else:
                        pk = "-".join([str(final_row[key]) for key in self.primary_keys])
                    if pk not in self.record_ids:
                        self.record_ids.add(pk)
                        yield row
                else:
                    yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = next_page_token is None
    
    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name]["partitions"] = []

        singer.write_message(StateMessage(value=tap_state))
    
    def process_number(self, field, value):
        return_value = value
        # Attempt to cast to float only if the value is a string with decimals
        if isinstance(value, str) and "." in value:
            try:
                return_value = float(value)
            except ValueError:
                self.logger.error(
                    f"Could not cast {field} : `{value}` to number"
                )
                raise Exception(ValueError)
        # only parse if it's a string
        elif isinstance(value, str):
            # Attempt to cast to int if there are no decimals
            try:
                return_value = int(value)
            except ValueError:
                self.logger.error(f"Could not cast {field} : `{value}` to integer")
                raise Exception(ValueError)
        return return_value
    

class NetsuiteDynamicSchema(NetSuiteStream):
    schema_response = None
    fields = None
    date_fields = []
    bool_fields = []
    use_dynamic_fields = False
    filter_fields = False
    default_fields = []

    def __init__(self, *args, **kwargs):
        self.float_fields = []
        self.integer_fields = []
        return super().__init__(*args, **kwargs)

    def _fetch_metadata_schema(self, session: OAuth1Session) -> None:
        try:
            if self.use_dynamic_fields:
                # TODO: refactor this to not force the except like this lol
                raise Exception("Switching to dynamic fields...")

            self.logger.info(f"Getting schema for {self.table} - stream: {self.name}")
            account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
            url = (
                "https://"
                f"{account}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/{self.table}"
            )
            prepared_req = session.prepare_request(
                requests.Request(
                    method="GET",
                    url=url,
                    headers=self.http_headers,
                )
            )
            prepared_req.headers.update({"Accept": "application/schema+json"})
            response = session.send(prepared_req)
            response.raise_for_status()
            self.schema_response = response.json()
        except:
            pass

    def _should_fetch_custom_fields(self) -> bool:
        add_custom_fields_streams = [
            "invoices",
            "bills",
            "invoice_lines",
            "bill_lines",
            "bill_expenses",
        ]
        return (
            not self.schema_response
            and self._tap.custom_fields is None
            and self.name in add_custom_fields_streams
        )

    def _fetch_custom_fields(self, session: OAuth1Session) -> None:
        offset = 0
        custom_fields = {}
        self.logger.info("Fetching custom fields data")

        while offset is not None:
            prepared_req = session.prepare_request(
                requests.Request(
                    method="POST",
                    url=f"{self.url_base}?offset={offset}&limit=1000",
                    headers=self.http_headers,
                    json={"q": "SELECT * FROM customfield"},
                )
            )
            response = session.send(prepared_req)
            if response.status_code not in [200]:
                self.logger.error(
                    f"Failed to fetch custom fields for {self.table} - stream: {self.name}, "
                    f"Error: {response.text}, not able to add custom fields to the schema"
                )
                break
            offset = self.get_next_page_token(response, offset)
            custom_fields.update(
                {
                    cf.get("scriptid").lower(): cf.get("fieldvaluetype")
                    for cf in response.json().get("items", [])
                }
            )

        self._tap.custom_fields = custom_fields

    def _fetch_sample_response_items(self, session: OAuth1Session) -> Optional[list[dict]]:
        self.fields = set()
        self.logger.info(f"Getting schema for {self.table} - stream: {self.name}")
        url = f"{self.url_base}?offset=0&limit=1000"
        query = (
            f"SELECT TOP 1000 * FROM {self.table} ORDER BY {self.replication_key} DESC"
            if self.replication_key
            else f"SELECT TOP 1000 * FROM {self.table}"
        )

        prepared_req = session.prepare_request(
            requests.Request(
                method="POST",
                url=url,
                headers=self.http_headers,
                json={"q": query},
            )
        )
        response = session.send(prepared_req)
        response.raise_for_status()
        return response.json().get("items")

    def _populate_fields(self, items: list[dict]) -> None:
        for item in items:
            self.fields.update(set(item.keys()))

    def _populate_date_fields(self, items: list[dict]) -> None:
        pot_date_fields = [
            field
            for field in self.fields
            if "date" in field and "custbody" not in field and "custrecord" not in field
        ]
        for field in pot_date_fields:
            match = [item for item in items if item.get(field)]
            if not match:
                continue
            try:
                try:
                    parse(match[0][field])
                except:
                    pendulum.from_format(match[0][field], "MM/DD/YYYY")
                self.date_fields.append(field)
            except:
                pass

    def _field_is_boolean(self, field: str, items: list[dict]) -> bool:
        matching = [item for item in items if item.get(field) in ["T", "F", None]]
        return len(matching) == len(items)

    def _populate_bool_fields(self, items: list[dict]) -> None:
        self.bool_fields = [field for field in self.fields if self._field_is_boolean(field, items)]

    def _custom_field_prefix(self) -> Optional[str]:
        if self.name in ["invoices", "bills"]:
            return "custbody"
        if self.name in ["invoice_lines", "bill_lines", "bill_expenses"]:
            return "custcol"
        return None

    def _apply_custom_field_types(self) -> None:
        if not self._tap.custom_fields:
            return

        cf_prefix = self._custom_field_prefix()
        if not cf_prefix:
            return

        table_cf = {
            key: value
            for key, value in self._tap.custom_fields.items()
            if key.startswith(cf_prefix)
        }
        self.fields.update(table_cf.keys())
        for cf, cf_type in table_cf.items():
            if cf_type in ["Decimal Number", "Percent"]:
                self.float_fields.append(cf)
            elif cf_type in ["Integer Number"]:
                self.integer_fields.append(cf)
            elif cf_type in ["Date/Time"]:
                self.date_fields.append(cf)
            elif cf_type in ["Check Box"]:
                self.bool_fields.append(cf)

    def _populate_schema_fields_from_sample(self, session: OAuth1Session) -> None:
        try:
            items = self._fetch_sample_response_items(session)
            self._populate_fields(items)
            self._populate_date_fields(items)
            self._populate_bool_fields(items)
            if "links" in self.fields:
                self.fields.remove("links")
            self._apply_custom_field_types()
        except:
            self.logger.warning(f"Failed to get schema for {self.table} - stream: {self.name}")
            pass

    @backoff.on_exception(backoff.expo, (
        HTTPError,
        RetriableAPIError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectionError,
        RemoteDisconnected,
    ), max_tries=5, factor=2)
    def get_schema(self):
        session = self.get_session()
        self._fetch_metadata_schema(session)

        if self._should_fetch_custom_fields():
            self._fetch_custom_fields(session)

        if not self.schema_response or self.filter_fields:
            self._populate_schema_fields_from_sample(session)

    def _build_schema_from_fields(self) -> dict:
        fields = self.fields
        properties_list = deepcopy(self.default_fields)
        fields = {f for f in fields if not any(df.name == f.lower() for df in self.default_fields)}
        for field in fields:
            if field == self.replication_key or field in self.date_fields:
                properties_list.append(th.Property(field.lower(), th.DateTimeType))
            elif field in self.bool_fields:
                properties_list.append(th.Property(field.lower(), th.BooleanType))
            elif field in self.float_fields:
                properties_list.append(th.Property(field.lower(), th.NumberType))
            elif field in self.integer_fields:
                properties_list.append(th.Property(field.lower(), th.IntegerType))
            else:
                properties_list.append(th.Property(field.lower(), th.StringType))
        return th.PropertiesList(*properties_list).to_dict()

    def _schema_property_type(self, value: dict) -> Any:
        if value.get("format") == "date-time":
            return th.DateTimeType
        if value.get("format") == "date":
            return th.DateType
        if value["type"] == "string":
            return th.StringType
        if value["type"] == "boolean":
            return th.BooleanType
        if value["type"] in ["number", "integer"]:
            return th.NumberType
        return th.CustomType({"type": [value["type"], "string"]})

    def _build_schema_from_response(self) -> dict:
        response = self.schema_response
        properties_list = deepcopy(self.default_fields) if self.always_add_default_fields else []
        if response is not None and response.get("properties"):
            for field, value in response.get("properties").items():
                if self.fields and self.filter_fields and field.lower() not in self.fields:
                    continue
                properties_list.append(
                    th.Property(field.lower(), self._schema_property_type(value))
                )
        return th.PropertiesList(*properties_list).to_dict()

    @property
    def schema(self):
        if self.config.get("use_input_catalog", True) and self._tap.input_catalog and self._tap.input_catalog.get(self.name):
            return self._tap.input_catalog.get(self.name).schema.to_dict()

        # Get netsuite schema for table
        if self.fields is None and self.schema_response is None:
            self.get_schema()

        if self.fields is not None and not self.schema_response:
            return self._build_schema_from_fields()

        if self.schema_response:
            return self._build_schema_from_response()

class NetsuiteDynamicStream(NetsuiteDynamicSchema):
    schema_response = None
    fields = None
    date_fields = []
    bool_fields = []
    use_dynamic_fields = False
    default_fields = []

    @property
    def select(self):
        if not self.selected and self.has_selected_descendents:
            selected_properties = self.get_selected_properties(select_all_by_default=True)
            return ",".join(selected_properties)
        elif self.selected and hasattr(self, "_select"):
            selected_fields = self._select.split(",")
            if any(f for f in selected_fields if f.endswith("*")):
                # For .* queries, keep the wildcard but explicitly format datetime fields
                datetime_fields = []
                for field_name, field_info in self.schema["properties"].items():
                    field_type = field_info.get("type", ["null"])[0]
                    field_format = field_info.get("format")
                    if field_type == "string" and field_format in ["date-time", "date"] and field_name not in self.invalid_fields:
                        datetime_fields.append(self.format_date_query(field_name))

                # Replace any .* with explicit datetime fields + .*
                modified_fields = []
                for field in selected_fields:
                    if datetime_fields:
                        modified_fields.extend(datetime_fields)
                    if field.endswith("*"):
                        modified_fields.insert(0, field)
                    else:
                        modified_fields.append(field)
                return ",".join(modified_fields)
            else:
                return self._select
        else:
            return None
    
    def process_types(self, row, schema=None): # noqa: C901
        if schema is None:
            schema = self.schema["properties"]
        for field, value in row.items():
            if field not in schema:
                # Skip fields not found in the schema
                continue

            field_info = schema[field]
            field_type = field_info.get("type", ["null"])[0]
            # Process nested properties
            if "properties" in field_info:
                row[field] = self.process_types(value, field_info["properties"])
            # Process nested properties
            if "items" in field_info:
                if isinstance(value, list):
                    row[field] = [
                        self.process_types(v, field_info["items"].get("properties"))
                        for v in value
                    ]
            field_format = field_info.get("format", None)
            if field_type == "string" and field_format == "date-time":
                # if it's already correctly a datetime, don't need to do anything
                if isinstance(value, datetime):
                    row[field] = value
                    continue

                try:
                    # Attempt to parse string as date-time
                    # If successful, no need to cast
                    _ = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    # If parsing fails, consider it as a type mismatch and attempt to cast
                    try:
                        row[field] = parse(value)
                    except:
                        row[field] = pendulum.from_format(value, "MM/DD/YYYY")
            elif field_type == "boolean":
                if not isinstance(value, bool):
                    # Attempt to cast to boolean
                    if value.lower() in ["true", "t"]:
                        row[field] = True
                    elif value.lower() in ["false", "f"]:
                        row[field] = False
                    else:
                        # No need to raise an error, just continue with the loop
                        continue

            elif field_type == "number" or field_type == "integer":
                if isinstance(value, str):
                    row[field] = self.process_number(field, value)

            elif field_type == "string":
                if not isinstance(value, str):
                    # Attempt to cast to string
                    row[field] = str(value)
            elif field_type == "array":
                array_types = field_info.get("type", ["null"])
                if isinstance(value, list):
                    continue
                else:
                    for array_type in array_types:
                        if array_type == "string":
                            try:
                                # Attempt to cast to JSON
                                parsed_value = json.loads(value)
                                if isinstance(parsed_value, list):
                                    row[field] = parsed_value
                                else:
                                    # We only want valid lists
                                    raise ValueError
                            except (ValueError, json.JSONDecodeError, TypeError):
                                if not isinstance(value, str):
                                    # Attempt to cast to string
                                    row[field] = str(value)
                        if array_type == "number" or array_type == "integer":
                            row[field] = self.process_number(field, value)

            else:
                # Unsupported type
                # No need to raise an error, just continue with the loop
                continue
        return row
    
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = self.process_types(row)
        return row


class BulkParentStream(NetsuiteDynamicStream):

    child_context_keys = ["ids"]
    start_date = None
    end_date = None

    @property
    def child_context_size(self):
        return self.config.get("child_context_size", 250)

    def get_replication_key_conditions(self, context):
        if not self.config.get("transaction_lines_monthly") or not self.replication_key:
            return None
        start = self.start_date or super().get_starting_time(context)
        if not start:
            return None
        self.start_date = start
        self.end_date = self.start_date + self.time_jump
        time_fmt = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"
        prefix = self.replication_key_prefix or self.table
        start_str = self.start_date.strftime(time_fmt)
        end_str = self.end_date.strftime(time_fmt)
        # Use >= so boundary records are included in the next window (avoids dropping between windows).
        return [
            f"{prefix}.{self.replication_key}>{start_str}",
            f"{prefix}.{self.replication_key}<={end_str}",
        ]

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {key: [] for key in self.child_context_keys}
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    # add id to child_context_bulk ids
                    if child_context:
                        for key, value in child_context.items():
                            child_context_bulk[key].extend(child_context[key]) if value else None
                
                if any(len(v) >= self.child_context_size for v in child_context_bulk.values()):
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {key: [] for key in self.child_context_keys}

                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            # process remaining child context if len < 1000
            if any(v != [] for v in child_context_bulk.values()):
                self._sync_children(child_context_bulk)
            #----
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

class TransactionRootStream(NetsuiteDynamicStream):
    select = None
    start_date = None
    end_date = None
    fields = None
    default_fields = []
    date_fields = []

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        # Avoid using my new logic if the flag is off
        if not self.config.get("transaction_lines_monthly"):
            return super().prepare_request_payload(context, next_page_token)

        filters = []
        # get order query
        prefix = self.replication_key_prefix or self.table
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
        if self.select:
            select = self.select.strip()
        else:
            select = ", ".join(selected_properties)

        join = self.join if self.join else ""

        payload = dict(
            q=f"SELECT {select} FROM {self.table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({timeframe})")
        return payload


    # Remove double spaces that might result from empty address fields
    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Collapse duplicate spaces in address fields
        row = super().post_process(row, context)
        if row.get("shippingaddress"):
            row["shippingaddress"] = re.sub(r'(, )+', ', ', row["shippingaddress"]).strip(', ')
            if row["shippingaddress"] == "":
                row.pop("shippingaddress")
        if row.get("billingaddress"):
            row["billingaddress"] = re.sub(r'(, )+', ', ', row["billingaddress"]).strip(', ')
            if row["billingaddress"] == "":
                row.pop("billingaddress")
        

        return row
    