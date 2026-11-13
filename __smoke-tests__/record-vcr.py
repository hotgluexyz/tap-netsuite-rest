"""Offline-replayable NetSuite smoke cases and narrowly scoped scrub rules."""

import base64
import json
import re
from urllib.parse import urlsplit, urlunsplit

import vcr
import xmltodict
from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class NetsuiteTestRunner(VCRTapTestRunner):
    """Replay NetSuite fixtures offline and scrub newly recorded customer data."""

    # Pagination controls, relational IDs used in child queries, and dynamic field types.
    PRESERVE_KEYS = {
        "count", "hasMore", "offset", "totalResults", "id", "internalid", "transaction",
        "billingaddress", "shippingaddress", "mainaddress", "returnaddress",
        "defaultshippingaddress", "defaultbillingaddress", "scriptid", "fieldvaluetype",
        "fieldtype", "file_id", "recordid", "key", "nkey", "accountingbook",
        "transactionline", "subscriptionchangeorder", "subscriptionline",
    }
    TOKEN_KEYS = [
        *VCRTapTestRunner.TOKEN_KEYS, "consumerKey", "signature", "nonce",
        "download_token", "redirect_uri",
    ]
    FILTER_HEADERS = [
        *VCRTapTestRunner.FILTER_HEADERS, "cookie", "x-n-operationid", "x-n-identifier",
        "report-to", "nel", "content-disposition",
    ]

    def module(self):
        """Return the tap's importable module name."""
        return "tap_netsuite_rest"

    def launch(self):
        """Import and run the tap after the harness configures its environment."""
        from tap_netsuite_rest.tap import TapNetSuite

        TapNetSuite.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        """Block new interactions during replay and apply recording scrub hooks."""
        return vcr.use_cassette(
            self.vcr_cassette_path,
            record_mode="once" if self.mode == "record" else "none",
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            before_record_request=self.before_record_request,
            before_record_response=self.before_record_response,
        )

    def before_record_request(self, request):
        """Remove SOAP credentials not covered by HTTP header filtering."""
        # SOAP authentication lives in XML, rather than the filtered Authorization header.
        if request.body and b"<tokenPassport>" in request.body:
            request.body = re.sub(
                rb"<tokenPassport>.*?</tokenPassport>",
                b"<tokenPassport>REDACTED</tokenPassport>", request.body, flags=re.DOTALL,
            )
        return request

    def scrub_uri(self, uri):
        """Replace account hostnames and attachment authentication tokens."""
        parts = urlsplit(uri)
        host = re.sub(r"^[^.]+(?=\.(?:suitetalk\.api|restlets\.api|app|extforms)\.netsuite\.com$)",
                      "1234567-sb1", parts.netloc)
        query = re.sub(r"(?i)(download_token|ns-at|h)=([^&]*)", r"\1=fake-token", parts.query)
        return urlunsplit((parts.scheme, host, parts.path, query, parts.fragment))

    def scrub_response_body(self, body, faker, cache):
        """Fake response data while retaining protocol and incremental controls."""
        # Attachment bytes are opaque to the tap: replace the document, not its filename/ID.
        if not body.lstrip().startswith(("{", "[", "<")):
            return "%PDF-1.4\n% Fake smoke-test attachment\n%%EOF\n"
        scrubbed = super().scrub_response_body(body, faker, cache)
        if body.lstrip().startswith(("{", "[")):
            return json.dumps(self.restore_controls(json.loads(body), json.loads(scrubbed)))
        scrubbed = xmltodict.unparse(self.restore_controls(
            xmltodict.parse(body), xmltodict.parse(scrubbed)
        ))
        # SOAP content is base64 encoded; random strings are not a valid replacement file.
        return re.sub(
            r"(<docFileCab:content[^>]*>).*?(</docFileCab:content>)",
            lambda m: m[1] + base64.b64encode(b"Fake smoke-test attachment\n").decode() + m[2],
            scrubbed, flags=re.DOTALL,
        )

    def restore_controls(self, original, scrubbed):
        """Restore structural values needed for pagination, schemas, and fallbacks."""
        if isinstance(original, dict):
            # Vendor metadata schemas drive types and field selection, not customer records.
            if "properties" in original and original.get("type") == "object":
                return original
            restored = {key: self.restore_controls(value, scrubbed[key])
                        for key, value in original.items()}
            for key, value in original.items():
                if key == "download_token":
                    restored[key] = "fake-token"
                # Personal dates are not transaction/replication boundaries.
                if key.lower() in {"hiredate", "birthdate", "dateofbirth", "dob"} and value:
                    restored[key] = "2000-01-01"
                # SOAP attributes/page numbers and standard namespace URNs are protocol data.
                if key in {"@isSuccess", "platformCore:pageIndex", "platformCore:totalPages",
                           "platformCore:totalRecords", "@internalId"} or key.startswith("@xmlns"):
                    restored[key] = value
                if key == "detail" and isinstance(value, str):
                    # Keep only the syntax that drives invalid-field and permission fallbacks.
                    fragments = re.findall(
                        r"(?i)(?:Field '\w+'(?: for record '\w+')? was not found\.|"
                        r"Record '\w+' was not found\.|field '\w+'|Unknown identifier '[\w.]+')",
                        value,
                    )
                    if fragments:
                        restored[key] = "Invalid search query. " + " ".join(fragments)
            return restored
        if isinstance(original, list):
            return [self.restore_controls(value, fake)
                    for value, fake in zip(original, scrubbed)]
        if isinstance(original, bool) or original in ("T", "F"):
            return original
        # NetSuite infers dates from data, then uses them as incremental boundaries.
        if isinstance(original, str) and re.fullmatch(
            r"(?:\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})(?:[T ].*)?", original
        ):
            return original
        return scrubbed


if __name__ == "__main__":
    NetsuiteTestRunner.main()
