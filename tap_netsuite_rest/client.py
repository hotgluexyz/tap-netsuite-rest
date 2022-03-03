"""REST client handling, including NetSuiteStream base class."""

import requests
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, cast

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from requests_oauthlib import OAuth1Session
from oauthlib import oauth1
import requests

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class NetSuiteStream(RESTStream):
    """NetSuite stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config['ns_account'].replace("_", "-").replace("SB", "sb")
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
                signature_method=oauth1.SIGNATURE_HMAC_SHA256
            )


    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
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
                self.query_date = next(extract_jsonpath(f"$.items[-1].{self.replication_key}", response.json()))
                return offset

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["offset"] = (next_page_token or 0)%100000
        params["limit"] = self.page_size
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        filters = []
        order_by = ""

        if self.replication_key:
            order_by = f"ORDER BY {self.replication_key}"

            start_date = self.get_starting_timestamp(context)

            if self.query_date:
                filters.append(f"{self.replication_key}>='{self.query_date}'")
            elif start_date:
                start_date_str = start_date.strftime("%m/%d/%Y")
                filters.append(f"{self.replication_key}>='{start_date_str}'")
        
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
            if isinstance(key, tuple) and len(key)==2:
                if value.selected:
                    selected_properties.append(key[-1])

        if self.select:
            select = self.select
        else:
            select = ",".join(selected_properties)
        
        join = self.join if self.join else ""

        payload = dict(q = f"SELECT {select} FROM {self.table} {join} {filters} {order_by}")
        return payload

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row
