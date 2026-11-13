"""Offline regression checks for saved NetSuite recordings."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import requests
import yaml

spec = importlib.util.spec_from_file_location("record_vcr", Path(__file__).with_name("record-vcr.py"))
record_vcr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(record_vcr)


class RecordingTest(unittest.TestCase):
    """Exercise the public cassette sanitizer and subsequent offline replay."""

    def setUp(self):
        """Create a synthetic capture without accessing NetSuite."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.case = Path(temporary.name) / "synthetic_test"
        (self.case / "fixtures").mkdir(parents=True)
        self.config_path = self.case / "config.json"
        self.config_path.write_text(json.dumps({
            "ns_account": "7654321_SB2",
            "bill_attachments_restlet_url": (
                "https://7654321-sb2.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=44&deploy=1"
            ),
            "bill_attachments_suitelet_url": (
                "https://7654321-sb2.extforms.netsuite.com/app/site/hosting/scriptlet.nl?script=45&deploy=2"
            ),
        }))
        self.rows = [
            {"id": "18", "billingaddress": "12/10/2023 Main Street, Test City",
             "shippingaddress": "654 Example Lane, Test Town",
             "lastmodifieddate": "2023-10-12T13:14:15Z"},
            {"id": "19", "billingaddress": "246", "shippingaddress": 357},
            {"id": "20", "billingaddress": {
                "id": "468", "addr1": "2023-10-12 Private Street", "addr2": "2023-10-13",
            }},
        ]
        self.cassette_path = self.case / "fixtures" / "vcr.yaml"
        self.cassette_path.write_text(yaml.safe_dump({
            "version": 1,
            "interactions": [{
                "request": {
                    "uri": "https://7654321-sb2.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql",
                    "method": "POST", "body": json.dumps({"q": "SELECT id FROM transaction"}),
                    "headers": {},
                },
                "response": {
                    "body": {"string": json.dumps({"items": self.rows})},
                    "headers": {}, "status": {"code": 200, "message": "OK"},
                },
            }],
        }))
        self.runner = record_vcr.NetsuiteTestRunner("synthetic_test", temporary.name)
        self.runner.mode = "run"

    def test_addresses_are_scrubbed_but_reference_ids_survive(self):
        """Formatted and nested addresses are PII; numeric references are controls."""
        self.runner.sanitize_cassette()
        cassette = yaml.safe_load(self.cassette_path.read_text())
        body = cassette["interactions"][0]["response"]["body"]["string"]
        rows = json.loads(body)["items"]
        for value in (self.rows[0]["billingaddress"], self.rows[0]["shippingaddress"],
                      self.rows[2]["billingaddress"]["addr1"],
                      self.rows[2]["billingaddress"]["addr2"]):
            self.assertNotIn(value, body)
        self.assertEqual(rows[1]["billingaddress"], "246")
        self.assertEqual(rows[1]["shippingaddress"], 357)
        self.assertEqual(rows[2]["billingaddress"]["id"], "468")
        self.assertEqual(rows[0]["lastmodifieddate"], "2023-10-12T13:14:15Z")

    def test_normalized_config_can_replay_the_saved_capture(self):
        """The sanitized config and cassette must identify the same fake account."""
        self.runner.sanitize_cassette()
        config = json.loads(self.config_path.read_text())
        self.assertEqual(config["ns_account"], "1234567_SB1")
        self.assertEqual(config["bill_attachments_restlet_url"], (
            "https://1234567-sb1.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=44&deploy=1"
        ))
        self.assertEqual(config["bill_attachments_suitelet_url"], (
            "https://1234567-sb1.extforms.netsuite.com/app/site/hosting/scriptlet.nl?script=45&deploy=2"
        ))
        self.assertNotIn("7654321", self.config_path.read_text() + self.cassette_path.read_text())
        host = config["ns_account"].lower().replace("_", "-")
        with self.runner.vcr_use_cassette([]):
            response = requests.post(
                f"https://{host}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql",
                json={"q": "SELECT id FROM transaction"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual([row["id"] for row in response.json()["items"]], ["18", "19", "20"])


if __name__ == "__main__":
    unittest.main()
