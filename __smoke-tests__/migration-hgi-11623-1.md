# HGI-11623: NetSuite reference and customer smoke migration (1/5)

22 of 68 legacy cases. Branch: `feature/hgi-11623_1`. Independent PR against `main`.

## Merge blocker: query-aware replay

Post-review validation supersedes the initial green replay below. URL-only matching allowed unrelated SuiteQL responses to satisfy a request. The runner now compares SQL while ignoring whitespace, unquoted case, and top-level projection order; tables, joins, filters, literals, and dates must still match.

Full offline replay now reports **14 passed, 54 failed** across 68 cases; this batch reports **7 passed, 15 failed**. Do not merge while these failures remain. Examples of genuine recording drift include location INNER versus LEFT joins and changed general-ledger entity/currency joins and date windows. Existing responses cannot establish the result of those different queries; fresh compatible recordings are needed, with authorization before live access. No business responses were invented and no legacy tests were removed.

The currency expectation is corrected from 74 account records to the four recorded currency records; classification is corrected from 35 accounting-period records to the three recorded classification records. Their schemas are unchanged. Classification's bookmark now comes from its own response (`2025-02-25 18:28:35`), not the accounting-period response (`2/25/2025`). Both corrected smoke tests pass. Currency's unmatched schema probe still falls back and is not claimed as covered.

The project compatibility adaptation removes the already-excluded `refname` and `links` projections and their invalid-field messages. Other invalid-field errors, the retry query, and all business response data remain unchanged; its query-aware replay passes. Subsidiary and location query-drift comments remain blocked rather than relabeling non-equivalent recordings.

## Provenance and validation

- Ticket: https://linear.app/hotglue/issue/HGI-11623
- Legacy source: `hotgluexyz/test-framework` at `8b74738e2483bbb9b1b145aad8ee7380bc2ccd82`.
- Tap base: `fe66ecd9c6e6a599b9c7a0ebd6a425ff0c3fdafd`.
- Python 3.10; hotglue-smoke-test 1.1.8; hotglue-singer-sdk 1.0.53; vcrpy 8.5.0.
- Migrated offline; no live recording, new API responses, or legacy deletions.
- Initial URL-only validation before splitting: `hotglue-smoke-test run`: **68 passed, 0 failed** (superseded above).
- `ruff check .` and `git diff --check` passed.
- Initial migration preserved legacy stream sets, counts, and schemas under URL-only matching; this did not prove response/query correspondence.
- The original untouched-input parity control shared the URL-only matching weakness. Query-aware corrections intentionally change the two incorrect expectations described above.
- This batch checked 102 case-specific original secret/account/filter values against every committed case file (including decoded YAML bodies): **0 matches**; no non-example emails.

## Scrubbing and preservation

- Retain only tap-consumed configuration; replace credentials/account names/hosts and entity-name filters with fakes.
- Request and response headers were allowlisted to content-type, accept, SOAPAction and Prefer as a one-time offline migration step. Future recordings use the runner's inherited/extended `FILTER_HEADERS` denylist, not that allowlist; only listed headers are removed.
- Keep relational IDs used in child queries, pagination controls, dynamic schema identifiers/types, boolean controls and transaction/replication dates. Metadata schemas remain vendor protocol data.
- Names, addresses, emails, phone numbers, free text and financial values use the harness faker. Personal dates use synthetic values.
- SOAP tokenPassport authentication is removed; attachment URL auth tokens and payloads are synthetic. Attachment IDs/record counts remain covered.
- Replay explicitly uses VCR `record_mode="none"`. Legacy request matchers are not weakened. Existing test-config sort/ignore/include/freeze options and selected filters are retained.

## State compatibility

Sixteen of the 68 historical expected outputs differ from current-stack replay: child partition cleanup, corrected maximum bookmarks, or normalized date formatting. The untouched-input control replay reproduces these differences exactly; they are not scrub-induced. No tap/SDK behavior was changed.

Cases in this batch with historical state differences: `customer_addresses_test`, `location_return_address_test`, `location_test`, `subsidiary_main_address_test`, `subsidiary_return_address_test`, `subsidiary_shipping_address_test`.

## Existing coverage limitations

Twelve of 68 cases attempt optional metadata/schema or permission-probe requests absent from their original recordings. VCR blocks those requests and the tap catches them and falls back; the raw-input control replay does the same. These optional paths are **not covered** by a successful API response. No interactions were invented or matching loosened.

Affected cases in this batch: `customer_addresses_test`, `customer_category_test`, `customer_unified_script_test`.

`location_main_address_test` was already empty in the legacy expected output: no RECORD or SCHEMA messages. The migration preserves that empty case; it is not claimed as non-empty extraction coverage. All other 67 cases remain non-empty.

## Legacy and current replay counts

| Legacy case | Stream | Legacy records | Scrubbed records | Schema | Current-stack state |
| --- | --- | ---: | ---: | --- | --- |
| `account_test` | `accounts` | 75 | 75 | equal | equal |
| `accounting_books_test` | `accounting_books` | 2 | 2 | equal | equal |
| `classification_test` | `classification` | 35 (wrong response) | 3 | equal | corrected to classification response |
| `currencies_test` | `currencies` | 74 (wrong response) | 4 | equal | equal |
| `custom_segment_values_test` | `custom_segment_values` | 6 | 6 | equal | equal |
| `custom_segment_values_test` | `custom_segments` | 2 | 2 | equal | equal |
| `custom_segments_test` | `custom_segments` | 2 | 2 | equal | equal |
| `customer_addresses_test` | `customer_addresses` | 142 | 142 | equal | equal |
| `customer_category_test` | `customer_category` | 2 | 2 | equal | equal |
| `customer_test` | `customers` | 140 | 140 | equal | equal |
| `customer_unified_script_test` | `customers` | 140 | 140 | equal | equal |
| `department_test` | `departments` | 7 | 7 | equal | equal |
| `employees_test` | `employees` | 14 | 14 | equal | equal |
| `location_main_address_test` | — (originally empty) | 0 | 0 | none in either | equal |
| `location_return_address_test` | `location_return_address` | 1 | 1 | equal | equal |
| `location_test` | `locations` | 15 | 15 | equal | equal |
| `location_unified_script_test` | `locations` | 15 | 15 | equal | equal |
| `project_test` | `projects` | 3 | 3 | equal | equal |
| `subsidiary_main_address_test` | `subsidiary_main_address` | 3 | 3 | equal | equal |
| `subsidiary_return_address_test` | `subsidiary_return_address` | 1 | 1 | equal | equal |
| `subsidiary_shipping_address_test` | `subsidiary_shipping_address` | 2 | 2 | equal | equal |
| `subsidiary_test` | `subsidiaries` | 3 | 3 | equal | equal |
| `subsidiary_unified_script_test` | `subsidiaries` | 3 | 3 | equal | equal |
