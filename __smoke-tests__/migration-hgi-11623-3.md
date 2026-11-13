# HGI-11623: NetSuite sales and purchasing smoke migration (3/5)

14 of 68 legacy cases. Branch: `feature/hgi-11623_3`. Independent PR against `main`.

## Replay scope: legacy compatibility

The owner requested preservation of the legacy URL-only replay contract, not stronger SQL-query coverage. The experimental SuiteQL matcher and its normalization helpers have been removed. Replay retains VCR's default method/URL matchers and `record_mode="none"`; no live recording, invented responses, or legacy deletions were used.

The SQL-aware experiment produced 14 passes and 54 failures across all cases. Those results exposed inherited query/recording drift, not scrub-induced changes. They are not the acceptance criterion for this compatibility-only migration. URL-only replay can consume a response recorded for different SQL at the same endpoint, so passing replay establishes legacy parity, not current SQL correctness. Query drift and missing probe responses remain known limitations of the legacy coverage. Tenant-specific custom-field identifiers remain structural schema metadata. Workflow hardening, the harness pin, and scrubbing rules are retained.

## Provenance and validation

- Ticket: https://linear.app/hotglue/issue/HGI-11623
- Legacy source: `hotgluexyz/test-framework` at `8b74738e2483bbb9b1b145aad8ee7380bc2ccd82`.
- Tap base: `fe66ecd9c6e6a599b9c7a0ebd6a425ff0c3fdafd`.
- Python 3.10; hotglue-smoke-test 1.1.8; hotglue-singer-sdk 1.0.53; vcrpy 8.5.0.
- Migrated offline; no live recording, new API responses, or legacy deletions.
- Independently rerun on 2026-11-13 with legacy URL-only matching: `hotglue-smoke-test run`: **68 passed, 0 failed** combined; this batch: **14 passed, 0 failed**.
- `ruff check .` and `git diff --check` passed.
- Legacy stream sets, counts, schemas, and current-stack final STATE parity are retained under the original matching behavior. Neither this parity control nor green replay proves response/query correspondence.
- This batch checked 70 case-specific original secret/account/filter values against every committed case file (including decoded YAML bodies): **0 matches**; no non-example emails.

## Scrubbing and preservation

- Retain only tap-consumed configuration; replace credentials/account names/hosts and entity-name filters with fakes.
- Request and response headers were allowlisted to content-type, accept, SOAPAction and Prefer as a one-time offline migration step. Future recordings use the runner's inherited/extended `FILTER_HEADERS` denylist, not that allowlist; only listed headers are removed.
- Keep relational IDs used in child queries, pagination controls, dynamic schema identifiers/types, boolean controls and transaction/replication dates. Metadata schemas remain vendor protocol data.
- Names, addresses, emails, phone numbers, free text and financial values use the harness faker. Personal dates use synthetic values.
- SOAP tokenPassport authentication is removed; attachment URL auth tokens and payloads are synthetic. Attachment IDs/record counts remain covered.
- Replay explicitly uses VCR `record_mode="none"`. Legacy request matchers are not weakened. Existing test-config sort/ignore/include/freeze options and selected filters are retained.

## State compatibility

Sixteen of the 68 historical expected outputs differ from current-stack replay: child partition cleanup, corrected maximum bookmarks, or normalized date formatting. The untouched-input control replay reproduces these differences exactly; they are not scrub-induced. No tap/SDK behavior was changed.

Cases in this batch with historical state differences: `invoice_addresses_test`, `invoice_lines_test`, `invoice_payments_test`.

## Existing coverage limitations

Twelve of 68 cases attempt optional metadata/schema or permission-probe requests absent from their original recordings. VCR blocks those requests and the tap catches them and falls back; the raw-input control replay does the same. These optional paths are **not covered** by a successful API response. No interactions were invented or matching loosened.

Affected cases in this batch: none.

## Per-stream count parity

| Legacy case | Stream | Legacy records | Scrubbed records | Schema | Current-stack state |
| --- | --- | ---: | ---: | --- | --- |
| `change_order_line_test` | `change_order_line` | 3646 | 3646 | equal | equal |
| `invoice_addresses_fix_duplicates_test` | `invoice_addresses` | 211 | 211 | equal | equal |
| `invoice_addresses_test` | `invoice_addresses` | 165 | 165 | equal | equal |
| `invoice_lines_test` | `invoice_lines` | 557 | 557 | equal | equal |
| `invoice_payments_attributes_v2_test` | `invoice_payments` | 55 | 55 | equal | equal |
| `invoice_payments_test` | `invoice_payments` | 20 | 20 | equal | equal |
| `invoices_tax_lines_test` | `invoice_tax_lines` | 520 | 520 | equal | equal |
| `invoices_tax_lines_test` | `invoices` | 580 | 580 | equal | equal |
| `invoices_test` | `invoices` | 550 | 550 | equal | equal |
| `invoices_with_taxtotal_test` | `invoices` | 580 | 580 | equal | equal |
| `purchase_order_lines_test` | `purchase_order_lines` | 6 | 6 | equal | equal |
| `purchase_order_test` | `purchase_orders` | 3 | 3 | equal | equal |
| `sales_order_lines_test` | `sales_order_lines` | 897 | 897 | equal | equal |
| `sales_order_test` | `sales_orders` | 448 | 448 | equal | equal |
| `sales_rep_test` | `sales_rep` | 2 | 2 | equal | equal |
