"""REST client handling, including NetSuiteStream base class."""

import logging
import backoff
import requests
import pendulum
import copy

from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, cast, Iterable

from memoization import cached
from oauthlib import oauth1
from requests_oauthlib import OAuth1Session
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk import typing as th
from pendulum import parse
from requests.exceptions import HTTPError
import copy
import json
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError
from dateutil.relativedelta import relativedelta
import pytz


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
    time_jump = relativedelta(months=1)
    record_ids = []

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
        return OAuth1Session(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=self.config["ns_account"],
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

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_next = next(extract_jsonpath("$.hasMore", response.json()))
        offset = next(extract_jsonpath("$.offset", response.json()))
        offset += self.page_size

        totalResults = next(extract_jsonpath("$.totalResults", response.json()))

        if has_next:
            if (
                isinstance(self, TransactionRootStream)
                and self.config.get("transaction_lines_monthly")
                and totalResults > 100000
            ):
                self.logger.info(
                    f"totalResults = {totalResults}, time_jump = {self.time_jump}"
                )
                if self.time_jump == relativedelta(months=1):
                    self.logger.info("Dropping time_jump to 1 week")
                    self.time_jump = relativedelta(weeks=1)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(weeks=1):
                    self.logger.info("Dropping time_jump to 3 days")
                    self.time_jump = relativedelta(days=3)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(days=3):
                    self.logger.info("Dropping time_jump to 1 day")
                    self.time_jump = relativedelta(days=1)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(days=1):
                    self.logger.info("Dropping time_jump to 12 hours")
                    self.time_jump = relativedelta(hours=12)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(hours=12):
                    self.logger.info("Dropping time_jump to 6 hours")
                    self.time_jump = relativedelta(hours=6)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(hours=6):
                    self.logger.info("Dropping time_jump to 1 hours")
                    self.time_jump = relativedelta(hours=1)
                    # need to reset the offset
                    return 0
                else:
                    self.logger.error(
                        "Even with minimum delta we are getting more than 100k records! We will likely infinite loop."
                    )

            return offset

        if isinstance(self, TransactionRootStream) and self.config.get("transaction_lines_monthly") and not has_next:
            today = datetime.now()
            today = today.replace(tzinfo=pytz.UTC)
            if self.end_date >= today:
                self.logger.info("Reached the end of the line! Stopping")
                return None
            else:
                # reset the time_jump to 1 month if we're going into a new month
                reset_time_jump = self.start_date.month != (self.start_date + self.time_jump).month
                # we should move to the next date range now
                self.start_date = self.start_date + self.time_jump
                if reset_time_jump:
                    self.logger.info("Resetting time_jump to 1 month for next iteration...")
                    self.time_jump = relativedelta(months=1)
                self.logger.info(f"Reached end of data for current period. Moving start date to {self.start_date}")
                return 0

        if not has_next and offset < totalResults:
            if self.replication_key:
                json_path = f"$.items[-1].{self.replication_key}"
                last_dt = next(extract_jsonpath(json_path, response.json()))
                try:
                    self.query_date = pendulum.parse(last_dt)
                except Exception as e:
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
        if self.query_date:
            start_date = self.query_date
            self.start_date_f = start_date.strftime("%Y-%m-%d")
        elif (self.name == "general_ledger_report" and self.config.get("gl_full_sync")) or ("replication_key" not in rep_key):
            start_date = parse(self.config["start_date"])
            self.start_date_f = start_date.strftime("%Y-%m-01")
        else:
            start_date = self.get_starting_time({})
            self.start_date_f = start_date.strftime("%Y-%m-01")
        self.end_date = (start_date + timedelta(window)).strftime("%Y-%m-%d")

    def get_selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                field_name = key[-1]
                prefix = self.select_prefix or self.table
                field_type = self.schema["properties"].get(field_name) or dict()
                if field_type.get("format") == "date-time":
                    field_name = f"TO_CHAR ({prefix}.{field_name}, 'YYYY-MM-DD HH24:MI:SS') AS {field_name}"
                else:
                    field_name = f"{prefix}.{field_name} AS {field_name}"
                selected_properties.append(field_name)
        return selected_properties

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        filters = []
        order_by = ""
        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"

        if self.replication_key and "_report" not in self.name:
            prefix = self.replication_key_prefix or self.table
            order_by = f"ORDER BY {prefix}.{self.replication_key}"

            start_date = self.get_starting_time(context)

            if self.query_date:
                start_date_str = self.query_date.strftime(time_format)
                filters.append(f"{prefix}.{self.replication_key}>{start_date_str}")
            elif start_date:
                start_date_str = start_date.strftime(time_format)
                filters.append(f"{prefix}.{self.replication_key}>{start_date_str}")

        if self.replication_key_prefix is None and self.order_by is not None:
            order_by = self.order_by

        if "_report" in self.name and self.custom_filter:
            self.get_date_boundaries()
            custom_filter = self.custom_filter.format(
                start_date=self.start_date_f , end_date=self.end_date
            )
            filters.append(custom_filter)
        else:
            if self.type_filter:
                filters.append(f"(Type='{self.type_filter}')")
            if self.custom_filter:
                filters.append(self.custom_filter)

        if filters:
            filters = "WHERE " + " AND ".join(filters)
        else:
            filters = ""

        selected_properties = self.get_selected_properties()

        if self.select:
            select = self.select
        else:
            select = ", ".join(selected_properties)

        join = self.join if self.join else ""

        payload = dict(
            q=f"SELECT {select} FROM {self.table} {join} {filters} {order_by}"
        )
        return payload

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if hasattr(self,"entities_fallback") and self.entities_fallback and response.status_code == 400:
            for entity in self.entities_fallback:
                if "Record \'{}\' was not found.".lower().format(entity['name']) in response.text.lower():
                    self.logger.info(f"Missing {entity['name']} permission. Retrying with updated query...")
                    if "select_replace" in entity:
                        self.select = self.select.replace(entity['select_replace'], "")
                    if "join_replace" in entity:  
                        self.join = self.join.replace(entity['join_replace'], "")
                    raise RetryRequest(response.text)
        if 500 <= response.status_code < 600 or response.status_code in [401, 429]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
                f"Response: {response.text}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
                f"Response: {response.text}"
            )
            raise FatalAPIError(msg)

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                Exception,
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
                if self.primary_keys:
                    if len(self.primary_keys) == 1:
                        pk = row[self.primary_keys[0]]
                    else:
                        pk = "-".join([row[key] for key in self.primary_keys])
                    if pk not in self.record_ids:
                        self.record_ids.append(pk)
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


class NetsuiteDynamicStream(NetSuiteStream):
    select = "*"
    schema_response = None

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, factor=2)
    def get_schema(self):
        s = self.get_session()
        self.logger.info(f"Getting schema for {self.table} - stream: {self.name}")

        account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        url = f"https://{account}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/{self.table}"
        prepared_req = s.prepare_request(
            requests.Request(
                method="GET",
                url=url,
                headers=self.http_headers,
            )
        )
        prepared_req.headers.update({"Accept": "application/schema+json"})
        response = s.send(prepared_req)
        response.raise_for_status()
        self.schema_response = response.json()


    @property
    def schema(self):
        # Get netsuite schema for table
        if self.schema_response is None:
            self.get_schema()

        response = self.schema_response
        properties_list = []
        for field, value in response.get("properties").items():
            if value.get("format") == 'date-time':
                properties_list.append(th.Property(field.lower(), th.DateTimeType))
            elif value.get("format") == "date":
                properties_list.append(th.Property(field.lower(), th.DateType))
            elif value["type"] == "string":
                properties_list.append(th.Property(field.lower(), th.StringType))
            elif value["type"] == "boolean":
                properties_list.append(th.Property(field.lower(), th.BooleanType))
            elif value["type"] in ["number", "integer"]:
                properties_list.append(th.Property(field.lower(), th.NumberType))
            else:
                #Object and array as custom types
                properties_list.append(th.Property(field.lower(), th.CustomType({"type": [value["type"],"string"]})))
        return th.PropertiesList(*properties_list).to_dict()
    
    def process_number(self, field, value):
        return_value = value
        # Attempt to cast to float only if the value is a string with decimals
        if isinstance(value, str) and "." in value:
            try:
                return_value = float(value)
            except ValueError:
                self.logger.error(
                    f"Could not cast {field} : `{value}` to number / integer"
                )
                raise Exception(ValueError)
        else:
            # Attempt to cast to int if there are no decimals
            try:
                return_value = int(value)
            except ValueError:
                self.logger.error(f"Could not cast {field} : `{value}` to integer")
                raise Exception(ValueError)
        return return_value
    
    def process_types(self, row, schema=None):
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
                try:
                    # Attempt to parse string as date-time
                    # If successful, no need to cast
                    _ = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    # If parsing fails, consider it as a type mismatch and attempt to cast
                    try:
                        row[field] = datetime.fromisoformat(value)
                    except ValueError:
                        # Default value for type mismatch date-time
                        row[field] = "1979-01-01 00:00:00"
            elif field_type == "boolean":
                if not isinstance(value, bool):
                    # Attempt to cast to boolean
                    if value.lower() == "true":
                        row[field] = True
                    elif value.lower() == "false":
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

class TransactionRootStream(NetSuiteStream):
    start_date = None
    end_date = None

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        # Avoid using my new logic if the flag is off
        if not self.config.get("transaction_lines_monthly"):
            return super().prepare_request_payload(context, next_page_token)

        filters = []
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
        select = ", ".join(selected_properties)

        join = self.join if self.join else ""

        payload = dict(
            q=f"SELECT {select} FROM {self.table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({timeframe})")
        return payload
