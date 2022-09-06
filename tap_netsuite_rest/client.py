"""REST client handling, including NetSuiteStream base class."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional, cast

import backoff
import requests
from memoization import cached
from oauthlib import oauth1
from requests_oauthlib import OAuth1Session
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from pendulum import parse

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
logging.getLogger("backoff").setLevel(logging.CRITICAL)


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
                self.query_date = datetime.strptime(last_dt, "%Y-%m-%d %H:%M:%S")
                return offset

        return None

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

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        filters = []
        order_by = ""
        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"
        
        if self.replication_key and self.name !="profit_loss_report" :
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

        if self.name=="profit_loss_report":
            pl_dates = self.get_profit_loss_dates(self.get_starting_time(context), self.config.get("end_date", None),context)
            self.custom_filter = self.custom_filter.format(start_date=pl_dates["start_date"],end_date = pl_dates["end_date"])
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
                field_type = self.schema["properties"][field_name]
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

        if 500 <= response.status_code < 600 or response.status_code in [401, 429]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
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

    def get_profit_loss_dates(self, start_date, end_date=None,context=None):
        rep_key = self.stream_state
        time_format_profit_loss = "%Y-%m-%d"
        start_date_f = start_date.strftime("%Y-%m-01")
        if "replication_key" not in rep_key:
            start_date_f = start_date.strftime("%Y-01-01")
        if end_date is None:
            end_date = self.last_day_of_month(start_date)
            end_date = end_date.strftime(time_format_profit_loss)
        else:
            end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
            end_date = end_date.strftime(time_format_profit_loss)    
        return {"start_date":start_date_f,"end_date":end_date}  

    def last_day_of_month(self,any_day):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = any_day.replace(day=28) + timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - timedelta(days=next_month.day)          
    
