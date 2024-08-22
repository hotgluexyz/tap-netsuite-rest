"""REST client handling, including NetSuiteStream base class."""

import logging
import backoff
import requests
import pendulum

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

        if has_next:
            return offset

        totalResults = next(extract_jsonpath("$.totalResults", response.json()))
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
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
            ),
            max_tries=10,
            factor=2,
        )(func)
        return decorator

    def last_day_of_month(self, any_day):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = any_day.replace(day=28) + timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - timedelta(days=next_month.day)
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        #override the request_records method to handle updated query
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            try:
                resp = decorated_request(prepared_request, context)
            except RetryRequest as e:
                #Retry the request with updated query
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token
                )
                resp = decorated_request(prepared_request, context)
                          
            for row in self.parse_response(resp):
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
            finished = not next_page_token   


class NetsuiteDynamicStream(NetSuiteStream):
    select = "*"
    schema_response = None

    @backoff.on_exception(backoff.expo, HTTPError, max_tries=5, factor=2)
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
            else:
                properties_list.append(th.Property(field.lower(), th.CustomType({"type": [value["type"],"string"]})))
        return th.PropertiesList(*properties_list).to_dict()