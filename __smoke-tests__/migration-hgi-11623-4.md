# HGI-11623: NetSuite general ledger smoke migration (4/5)

10 of 68 legacy cases. Branch: `feature/hgi-11623_4`. Independent PR against `main`.

## Merge blocker: query-aware replay

Post-review validation supersedes the initial green replay below. URL-only matching allowed unrelated SuiteQL responses to satisfy a request. The runner now compares SQL while ignoring whitespace, unquoted case, and top-level projection order; tables, joins, filters, literals, and dates must still match.

Full offline replay now reports **14 passed, 54 failed** across 68 cases; this batch reports **0 passed, 10 failed**. Do not merge while these failures remain. General-ledger recordings differ in entity/currency joins and date windows; existing responses cannot establish results for those different queries. Fresh compatible recordings are needed, with authorization before live access. The initial parity table below is historical preservation evidence, not proof of current query correctness. No business responses were invented and no legacy tests were removed.

## Provenance and validation

- Ticket: https://linear.app/hotglue/issue/HGI-11623
- Legacy source: `hotgluexyz/test-framework` at `8b74738e2483bbb9b1b145aad8ee7380bc2ccd82`.
- Tap base: `fe66ecd9c6e6a599b9c7a0ebd6a425ff0c3fdafd`.
- Python 3.10; hotglue-smoke-test 1.1.8; hotglue-singer-sdk 1.0.53; vcrpy 8.5.0.
- Migrated offline; no live recording, new API responses, or legacy deletions.
- Initial URL-only validation before splitting: `hotglue-smoke-test run`: **68 passed, 0 failed** (superseded above).
- `ruff check .` and `git diff --check` passed.
- Initial migration preserved legacy stream sets, counts, and schemas under URL-only matching; this did not prove response/query correspondence.
- The original untouched-input parity control shared the URL-only matching weakness.
- This batch checked 33 case-specific original secret/account/filter values against every committed case file (including decoded YAML bodies): **0 matches**; no non-example emails.

## Scrubbing and preservation

- Retain only tap-consumed configuration; replace credentials/account names/hosts and entity-name filters with fakes.
- Request and response headers were allowlisted to content-type, accept, SOAPAction and Prefer as a one-time offline migration step. Future recordings use the runner's inherited/extended `FILTER_HEADERS` denylist, not that allowlist; only listed headers are removed.
- Keep relational IDs used in child queries, pagination controls, dynamic schema identifiers/types, boolean controls and transaction/replication dates. Metadata schemas remain vendor protocol data.
- Names, addresses, emails, phone numbers, free text and financial values use the harness faker. Personal dates use synthetic values.
- SOAP tokenPassport authentication is removed; attachment URL auth tokens and payloads are synthetic. Attachment IDs/record counts remain covered.
- Replay explicitly uses VCR `record_mode="none"`. Legacy request matchers are not weakened. Existing test-config sort/ignore/include/freeze options and selected filters are retained.

## State compatibility

Sixteen of the 68 historical expected outputs differ from current-stack replay: child partition cleanup, corrected maximum bookmarks, or normalized date formatting. The untouched-input control replay reproduces these differences exactly; they are not scrub-induced. No tap/SDK behavior was changed.

Cases in this batch with historical state differences: none.

## Existing coverage limitations

Twelve of 68 cases attempt optional metadata/schema or permission-probe requests absent from their original recordings. VCR blocks those requests and the tap catches them and falls back; the raw-input control replay does the same. These optional paths are **not covered** by a successful API response. No interactions were invented or matching loosened.

Affected cases in this batch: `general_ledger_report_float_amounts_test`, `general_ledger_report_only_primary_accounting_books_off_test`, `general_ledger_report_only_primary_accounting_books_on_test`, `general_ledger_report_subsidiary_currency_test`, `general_ledger_report_test`, `general_ledger_report_unified_script_2_test`, `general_ledger_report_unified_script_3_test`, `general_ledger_report_unified_script_test`, `general_ledger_report_with_entity_data_from_lines_test`.

## Per-stream count parity

| Legacy case | Stream | Legacy records | Scrubbed records | Schema | Current-stack state |
| --- | --- | ---: | ---: | --- | --- |
| `general_ledger_report_float_amounts_test` | `general_ledger_report` | 547 | 547 | equal | equal |
| `general_ledger_report_only_primary_accounting_books_off_test` | `general_ledger_report` | 559 | 559 | equal | equal |
| `general_ledger_report_only_primary_accounting_books_on_test` | `general_ledger_report` | 557 | 557 | equal | equal |
| `general_ledger_report_subsidiary_currency_test` | `general_ledger_report` | 540 | 540 | equal | equal |
| `general_ledger_report_test` | `general_ledger_report` | 208 | 208 | equal | equal |
| `general_ledger_report_unified_script_2_test` | `general_ledger_report` | 363 | 363 | equal | equal |
| `general_ledger_report_unified_script_3_test` | `general_ledger_report` | 964 | 964 | equal | equal |
| `general_ledger_report_unified_script_test` | `general_ledger_report` | 208 | 208 | equal | equal |
| `general_ledger_report_with_entity_data_from_lines_test` | `general_ledger_report` | 52 | 52 | equal | equal |
| `gl_report_entities_fallback_test` | `general_ledger_report` | 7252 | 7252 | equal | equal |
