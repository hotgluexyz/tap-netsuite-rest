# HGI-11623: NetSuite reference and customer smoke migration (1/5)

22 of 68 legacy cases. Branch: `feature/hgi-11623_1`. Independent PR against `main`.

## Replay scope: legacy compatibility

The owner requested preservation of the legacy URL-only replay contract, not stronger SQL-query coverage. The experimental SuiteQL matcher and its normalization helpers have been removed. Replay retains VCR's default method/URL matchers and `record_mode="none"`; no live recording or invented responses were used.

The SQL-aware experiment produced 14 passes and 54 failures across all cases. Those results exposed inherited query/recording drift, not scrub-induced changes. They are not the acceptance criterion for this compatibility-only migration. URL-only replay can consume a response recorded for different SQL at the same endpoint, so passing replay establishes legacy parity, not current SQL correctness.

The classification and currency expectations are restored byte-for-byte from the initial migration: 35 and 74 records, respectively, including their original migrated bookmarks. These retain the known legacy cross-query response behavior; their counts establish legacy replay parity, not correct classification or currency extraction. Location join changes, GL joins/date windows, and missing probe responses remain limitations of the legacy coverage, not reasons to re-record against a live account for this migration.

The project adaptation still removes the already-excluded `refname` and `links` projections and their invalid-field messages; no business response data was changed. Tenant-specific custom-field identifiers remain structural schema metadata. Workflow hardening, the harness pin, and scrubbing rules are retained.

## Provenance and validation

- Ticket: https://linear.app/hotglue/issue/HGI-11623
- Legacy source: `hotgluexyz/test-framework` at `8b74738e2483bbb9b1b145aad8ee7380bc2ccd82`.
- Tap base: `fe66ecd9c6e6a599b9c7a0ebd6a425ff0c3fdafd`.
- Python 3.10; hotglue-smoke-test 1.1.8; hotglue-singer-sdk 1.0.53; vcrpy 8.5.0.
- Migrated offline; no live recording, new API responses, or legacy deletions.
- Independently rerun on 2026-11-13 with legacy URL-only matching: `hotglue-smoke-test run`: **68 passed, 0 failed** combined; this batch: **22 passed, 0 failed**.
- `ruff check .` and `git diff --check` passed.
- Legacy stream sets, counts, schemas, and current-stack final STATE parity are retained under the original matching behavior. Neither this parity control nor green replay proves response/query correspondence.
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
| `classification_test` | `classification` | 35 | 35 | equal | equal |
| `currencies_test` | `currencies` | 74 | 74 | equal | equal |
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
