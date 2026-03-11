import json
import random
import base64
import hashlib
import hmac
import datetime
from logging import Logger

import backoff
import requests
import xmltodict
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class NetsuiteSOAPClient:
    def __init__(self, config: dict, logger: Logger):
        self.logger = logger
        self.config = config


    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        return f"https://{url_account}.suitetalk.api.netsuite.com/services/NetSuitePort_2025_2"


    def http_headers(self, action) -> dict:
        """Return the http headers needed."""
        headers = {
            "content-type": "text/xml; charset=utf-8",
            "SOAPAction": action
        }
        return headers
    
    
    def get_body_header(self, page_size=None):
        consumer_key = self.config["ns_consumer_key"]
        consumer_secret = self.config["ns_consumer_secret"]
        token_key = self.config["ns_token_key"]
        token_secret = self.config["ns_token_secret"]
        account = self.config["ns_account"]

        nonce = "".join([str(random.randint(0, 9)) for _ in range(20)])
        timestamp = str(int(datetime.datetime.now().timestamp()))
        key = f"{consumer_secret}&{token_secret}".encode(encoding="ascii")
        msg = "&".join([account, consumer_key, token_key, nonce, timestamp])
        msg = msg.encode(encoding="ascii")

        hashed_value = hmac.new(key, msg=msg, digestmod=hashlib.sha256)
        dig = hashed_value.digest()
        signature_value = base64.b64encode(dig).decode()
        body_header = {
            "soapenv:Header": {
                "tokenPassport": {
                    "account": account,
                    "consumerKey": consumer_key,
                    "token": token_key,
                    "nonce": nonce,
                    "timestamp": timestamp,
                    "signature": {
                        "@algorithm": "HMAC_SHA256",
                        "#text": signature_value,
                    }
                }
            }
        }

        if page_size:
            body_header["soapenv:Header"]["searchPreferences"] = {
                "pageSize": page_size
            }

        return body_header
    

    def get_request_body(self, content, page_size=100):
        request_body = {
            "soapenv:Envelope": {
                "@xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                "@xmlns:soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
                "@xmlns:platformCore": "urn:core_2025_2.platform.webservices.netsuite.com",
                "@xmlns:tranSales": "urn:sales_2025_2.transactions.webservices.netsuite.com",
                "@xmlns:platformMsgs": "urn:messages_2025_2.platform.webservices.netsuite.com",
                "@xmlns:platformCommon": "urn:common_2025_2.platform.webservices.netsuite.com",
                **self.get_body_header(page_size),
                "soapenv:Body": {
                    **content
                }
            }
        }

        return request_body
    

    def parse_response(self, response):
        parsed_xml = xmltodict.parse(response.text)
        parsed_response = json.loads(json.dumps(parsed_xml))
        return parsed_response


    def format_payload(self, payload, page_size):
        dict_body = self.get_request_body(content=payload, page_size=page_size)
        # transform payload to xml
        body = xmltodict.unparse(dict_body, short_empty_elements=True).encode("utf-8")
        return body


    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def request_api(
        self, action, request_data, extract_json_path, page_size=100
    ):
        """Request records from SOAP endpoint, returning response records."""
        # wrap and format payload
        xml_request_data = self.format_payload(request_data, page_size)
        # send request
        response = self._request(action, xml_request_data)

        try:
            self.validate_response(response, extract_json_path)
            # parse response
            parsed_response = self.parse_response(response)
        except KeyError as e:
            self.logger.error(f"Failed to parse response: {response.text} {e.__repr__()}")
            raise FatalAPIError(f"Malformed response: {response.text} {e.__repr__()}")

        # validate response
        result = next(extract_jsonpath(extract_json_path, input=parsed_response), {})
        return result

 
    def validate_response(self, response, extract_json_path) -> None:
        """Validate HTTP response."""
        try:
            if response.status_code == 429 or 500 <= response.status_code < 600:
                raise RetriableAPIError(response.text)
            elif response.status_code != 200:
                raise FatalAPIError(response.text)
            
            parsed_response = self.parse_response(response)
            response_status = next(extract_jsonpath(extract_json_path, input=parsed_response), {}).get("platformCore:status")
            is_success = response_status.get("@isSuccess") == "true"

            if not is_success:
                error_message = json.dumps(response_status) if response_status else response.text
                raise FatalAPIError(error_message)
        except (KeyError, ValueError, TypeError) as e:
            raise FatalAPIError(f"Failed to parse response: {e.__repr__()}")
    

    def _request(
        self, action, request_data
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        http_method = "POST"
        headers = self.http_headers(action)
        url = self.url_base

        try:
            response = requests.request(
                method=http_method,
                url=url,
                headers=headers,
                data=request_data,
            )
            return response
        
        except requests.RequestException as e:
            self.logger.error(f"Request to {url} failed: {e.__repr__()}")
            raise FatalAPIError(f"HTTP request failed: {e.__repr__()}")


    def format_filter_in(self, field_name, values):
        return {
            "in": {
                "field": field_name,
                "value": values,
            }
        }


    def get(self, type, id):
        extract_json_path = "$['soapenv:Envelope']['soapenv:Body']['getResponse']['readResponse']"

        request_file_body = {
            "platformMsgs:get": {
                "platformMsgs:baseRef": {
                    "@internalId": id,
                    "@type": type,
                    "@xsi:type": "platformCore:RecordRef"
                }
            }
        }

        response = self.request_api("get", request_file_body, extract_json_path)

        return response["record"]
    

    def search(self, payload, extract_json_path, page_size):
        response = self.request_api("search", payload, extract_json_path, page_size)

        while True:
            records = response.get("platformCore:searchRowList", {}).get("platformCore:searchRow", [])

            if isinstance(records, dict):
                records = [records]

            if not records:
                break
            
            for record in records:
                yield record

            page_index = int(response.get("platformCore:pageIndex"))
            total_pages = int(response.get("platformCore:totalPages"))
            searchId = response.get("platformCore:searchId")

            if page_index == total_pages:
                break

            search_more_payload = {
                "platformMsgs:searchMoreWithId": {
                    "searchId": searchId,
                    "pageIndex": page_index + 1
                }
            }

            extract_json_path = "$['soapenv:Envelope']['soapenv:Body']['searchMoreWithIdResponse']['platformCore:searchResult']"
            response = self.request_api("searchMoreWithId", search_more_payload, extract_json_path, page_size)
