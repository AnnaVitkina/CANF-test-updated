# `matching_new.py` — CANF matching engine (detailed rules & checks)

**Path:** `test folder/matching_new.py`  
**Role:** After **`vocabular.py`** has written **`partly_df/vocabulary_mapping.xlsx`**, load the **rate card** and the **shipment** table (LC preferred, else ETOF), align on **common columns**, score every rate-card **lane** against each shipment row, apply **conditions** (from Excel comments) and **business rules** (geo zones), then build a multi-line **`comment`** per row and save **`Matched_Shipments_with.xlsx`**.

**Import note:** `result.py` uses `from matching import run_matching`. This repo often ships **`matching_new.py`** only — add a thin **`matching.py`** re-export or change the import. See [result.md](result.md).

---

## 1. Orchestrator: `run_matching(rate_card_file_path=None)`

| Step | What happens |
|------|----------------|
| 1 | Resolve rate card path under `input/` (or auto-pick from a small hardcoded list if `None`). |
| 2 | `process_rate_card` → dataframe, columns, **`rate_card_conditions`** dict (this is the live conditions source in the main flow; **`load_conditions()`** is a separate legacy path for `Filtered_Rate_Card_with_Conditions.xlsx`). |
| 3 | `load_business_rules_for_matching(rate_card_file_path)` → lookup used in matching. |
| 4 | Read `partly_df/vocabulary_mapping.xlsx` sheets **ETOF**, **LC**, **Origin** (optional). |
| 5 | **Shipment DF:** use **LC** if present and non-empty, else **ETOF**; need at least one. |
| 6 | Keep rows that have a populated **ETOF #** (several column name variants); else error. |
| 7 | `find_common_columns(shipment_df, rate_card_df)`. |
| 8 | `match_shipments_with_rate_card(..., conditions_dict=rate_card_conditions, ...)`. |
| 9 | Reorder columns (ids, carrier, ship date, …, **`comment`** last), write Excel + optional **`matching_debug.txt`** via stdout tee. |

---

## 2. Column alignment: `find_common_columns`

1. **Exact set intersection** of column names between shipment and rate card → `common_cols`.
2. If empty: **normalize** every name with `normalize_column_name` (lowercase, remove spaces and underscores), match normalized keys, and use the **shipment’s original** column names for the common list.

So matching always works on a list of **logical** attributes present in both sides (after rename by vocabular).

---

## 3. Value normalization: `normalize_value` / `normalize_column_name`

### `normalize_column_name(col_name)`

Lowercase string; remove spaces and underscores (so `Origin Country` and `origincountry` align).

### `normalize_value(value)`

Used for **comparing** shipment vs rate card cells:

- `NaN` / missing → `None`.
- Strings trimmed; then special case: if the string **starts with `0`** and the remainder is digits, **keep as string** (postal / leading zeros).
- Otherwise: try **numeric** parse (`float` → `int` if whole) so `7719.0` and `7719` align, then lowercased string with spaces and underscores removed.

This is the value space used for **exact match**, **postal prefix**, and **country set** membership unless a branch uses **raw** values for conditions.

---

## 4. Per-shipment-row pipeline inside `match_shipments_with_rate_card`

For **each** shipment row the code runs roughly this **ordered** pipeline.

### 4.1 PRE-STEP — Business rules on shipment (before countries)

For every column listed in **`business_rules_lookup['business_rule_columns']`**:

- Resolve the column on the shipment row (exact or normalized name).
- Call **`validate_business_rule(row, col, value, lookup)`** (or geo search when value empty — see §6).

**Outcomes:**

- **`business_rule_columns_passed`**: normalized column names where **at least one** rule passed.
- **`business_rule_columns_failed`**: failure messages per rule column when **no** rule passed for that column.
- **`columns_validated_by_business_rules`**: geo columns (e.g. Origin Country) that are **cleared** from later duplicate checks because a business rule already validated them.

These sets drive **which country checks are skipped** and **which discrepancy lines** appear later.

### 4.2 STEP 1 — Origin / destination countries (if rate card has country cols)

**Prerequisites:** Rate card has detected **Origin Country** / **Destination Country** columns (several naming variants + normalized fallback).

**Shipment:** same variants on the row.

**Logic:**

- If **both** origin and destination countries were validated by business rules → skip country validation.
- If only one side validated by BR → validate the **other** side only.
- Else full check:
  - Missing required origin/dest → comments like “Origin country is missing”.
  - Build sets of **normalized** countries appearing on the rate card (`unique_rc_orig_countries_norm`, `unique_rc_dest_countries_norm`).
  - Parse **conditions** on those country columns for lines like **`Singapore: equals SG,SGP`** → maps **`sg` → `singapore`** so shipment **codes** can match rate card **names**.
  - **`country_matches_rate_card(shipment_norm, rc_set, code_map)`**: direct membership in `rc_set`, or map code → name then membership.
  - Optional **(origin, dest) pair** check: normalized pair (after code→name) must exist in `unique_rc_orig_dest_combinations` built from rate card rows.

**If any comment was added here → row is finished** (`continue`): no lane scoring (countries must be plausible first).

### 4.3 STEP 3 — Lane scoring (best lane(s))

For the shipment row, build **`etofs_normalized_values`** keyed by **normalized common column** names.

**For each rate-card row (lane):**

- Build **`rate_card_normalized_values`** the same way.
- **`current_matches = 0`**. For each common column (in lockstep index `i`):

| Priority | Rule | Effect on score |
|----------|------|-----------------|
| 1 | **Wildcard** | If **rate card normalized value is `None`** (empty cell) → **+1** match (lane accepts any shipment value). |
| 2 | **Business rule cell** | If `rc_val_raw` equals a **rule name** in `rule_to_country` / `rule_to_postal`: compare shipment **country** (and **postal** if rule has postal prefixes) to that rule’s definition. If both pass → **+1**; else 0. (Separate from PRE-STEP; this is **lane-specific** rule name in the cell.) |
| 3 | **Column conditions** | If `conditions_dict` has rules for this column: **`check_value_against_conditions(shipment_raw, rc_raw, col, dict)`**. If satisfied → **+1**. If a condition line exists for this RC value but is **not** satisfied → **0** (explicit miss). |
| 4 | **Postal columns** | If normalized column name looks like postal (`post`, `ship_post`, `cust_post`, etc.) and both sides non-null: shipment must **`startswith`** rate card prefix → +1 or 0. |
| 5 | **Exact** | `normalize_value(shipment) == normalize_value(rate card)` → +1 or 0. |

**Best lane selection:**

- Track **`max_matches`** and list **`best_matching_rate_card_rows`** (dicts with `rate_card_row`, later `discrepancies`, lane-level BR fields).
- **Strict greater** replaces the list; **equal** score **appends** (ties).

**`too_many_matches`** flag if **more than 4** tied lanes (changes later comment strategy).

**`Rate Card` column on shipment:** if rate card has **Carrier agreement**, copy from **first** best lane (updated again after date filter).

### 4.4 Date filter on best lane(s)

If shipment has a **ship / loading date** (several column names) **and** rate card has **Valid from** / **Valid to**:

- For **each** best match, parse:
  - Shipment date: **`YYYYMMDD`** if 8 digits, else `pd.to_datetime`.
  - Valid from/to: **`DD.MM.YYYY`** if contains `.`, else **`DDMMYYYY`** if 8 digits, else `dayfirst` parse.
- If **`date_dt < valid_from` or `date_dt > valid_to`** → drop that lane from `best_matching_rate_card_rows`.

**If all lanes dropped:** comment *“Date '…' is outside valid date range for all matching rate card entries”* and **`continue`**.

### 4.5 STEP 4 — Discrepancies per remaining best lane

For **each** surviving best match:

1. **Per-lane business rule filter:** only BR messages whose rule name appears in **this lane’s** active business-rule cells are kept (`lane_br_messages`, `lane_validated_columns`).

2. **Date discrepancy** (again per lane): same parsing; if outside range → append structured discrepancy `type: 'date_range'`.

3. **For each common column** (same index mapping):

   - Skip if column normalized name is in **`lane_validated_columns`** (validated by BR for this lane).
   - **Wildcard:** empty rate card cell → **no discrepancy**.
   - **Postal:** if values differ normalized but shipment **starts with** RC prefix → **no discrepancy**.
   - Else if normalized values differ:
     - **`check_value_against_conditions`**: if condition **satisfies** shipment for this RC value → **no discrepancy** (legal alternate encoding).
     - Else if column is a **business rule column** and shipment value is **NaN** → skip (geo discrepancy handled elsewhere).
     - Else append discrepancy dict (column, shipment value, RC value, optional **`condition`** text from `find_condition_for_value` for user hints like “equals LGB”).

### 4.6 Comments assembly (high level)

- **Too many fields** on a lane (`>5` discrepancies) → *“Please recheck… Too many shipment details to update.”*
- **`too_many_matches`** → run **`analyze_discrepancy_patterns`** on pooled discrepancies; add summarized line; optional **“Also:”** lines for minor columns; append note about *N possible rate lanes*.
- Otherwise: per-lane **business rule messages**, then **“Discrepancies for Match k:”** lines with human-readable **needs to be changed** text; date lines formatted separately.
- **PASS 1 / additional passes:** merge **`business_rule_columns_failed`** messages for sides that had **no** passing BR (origin vs destination logic in later lines of the file — reduces noise when one side passed).

### 4.7 Success path

If no blocking comment was built, row may get **`No discrepancies found`** (see tail of `match_shipments_with_rate_card`).

---

## 5. Condition language: `check_value_against_conditions` & `value_satisfies_condition`

**`conditions_dict`** maps **column name** → **string** (often multiline from Excel comments) or list of lines.

### `check_value_against_conditions(shipment_val, rate_card_val, column_name, conditions_dict, debug)`

1. Find column in dict (**case-insensitive** key match).
2. Split condition blob into **lines** (if string).
3. Skip lines that are only “Conditional rules” headers without a value-specific colon.
4. For each line: if line matches pattern **`(optional number.) rate_card_value:`** in lowercase (regex with `re.escape(rate_card_val)`), then call **`value_satisfies_condition(shipment, rate_card_val, full_line)`**.
5. **First satisfied** condition → return **`(True, that_line)`**.
6. No match → **`(False, None)`**.

### `value_satisfies_condition` — supported **logic** fragments

After stripping optional **`N.`** prefix and **`VALUE:`** before the logic part, the **logic** string is scanned for keywords (order matters in code):

| Keyword / pattern | Meaning |
|-------------------|--------|
| **`is empty`** / **`is empty in any item`** | Shipment empty → True; if combined with **`and does not contain`**, empty still counts as not containing forbidden tokens → True. |
| **`does not contain`** | Empty shipment → True; else split forbidden list after phrase (comma-separated), lowercase substring check — **any** hit → False. |
| **`does not equal`** / **`does not equal to`** | Empty → True; else forbidden list must **not** equal shipment (exact lower string). |
| **`contains`** (and not “does not contain”) | Empty → **False**; else shipment must contain **at least one** required token after **`contains`**. |
| **`equals`** / **`equal to`** (and not “does not equal”) | Empty → **False**; else shipment must **equal** one of the listed tokens. |

If **no** clause matches → **False**.

**`find_condition_for_value`** returns the **raw condition line** for a given rate-card cell value (used in discrepancy text to suggest target codes).

---

## 6. Business rules: `validate_business_rule` & `find_matching_business_rule_by_geo`

### When `rule_value` is **non-empty**

1. Match `rule_value` string to a key in **`rule_to_country`** or **`rule_to_postal_codes`** (case-insensitive, substring allowed on rule names).
2. Load **expected country** (may be comma-separated **multiple** ISO-like tokens) and **expected postal prefixes** list.
3. Classify **column name**:
   - **Origin** if contains `origin`, `ship`, or `from`.
   - **Destination** if contains `destination`, `cust`, or `to`.
4. **Country region** vs **postal zone:** from column name: `country`+`region` → postal validation **skipped**; `postal` or `zone` → require postal checks.
5. Read **actual** country / postal from shipment row using long **variant lists** (`Origin Country`, …).
6. **Country:** actual upper string must be **in** the split expected list.
7. **Postal (non–country-region):** actual lower string must **`startswith`** any expected prefix **or** equal it.

Return **`(True, validated_column_names, message, {})`** or failure with **`failure_details`** (used for comments).

### When `rule_value` is **empty / NaN**

**`find_matching_business_rule_by_geo`** tries to infer which named rule applies by scanning **rules registered for that column** in `column_to_rules` and comparing shipment **country** / **postal** to each rule’s geography (postal zones vs country-only regions). Used to validate empty zone cells against actual shipment geo.

---

## 7. Pattern summarization: `analyze_discrepancy_patterns`

Input: list of discrepancy dicts `{column, etofs_value, rate_card_value, ...}`.

| Case | Output comment |
|------|----------------|
| All discrepancies same **column** | `"{column}: Shipment value needs to be changed"` |
| One column has **≥70%** of discrepancies | Same dominant column message + **minor** list for “Also” lines |
| Top **≤3** columns cover **≥80%** | `"Col1, Col2: Shipment values need to be changed"` |
| Else | `"Please recheck the shipment details"` |

Used when **many tied lanes** exist to avoid huge comment walls.

---

## 8. Legacy helper: `load_conditions`

Reads **`Filtered_Rate_Card_with_Conditions.xlsx`** next to the script (not used by default `run_matching`, which passes **`rate_card_conditions`** from **`process_rate_card`**). Kept for alternate workflows.

---

## 9. Debug output

- **`match_shipments_with_rate_card`** wraps **`sys.stdout`** in **`TeeOutput`** writing to **`matching_debug.txt`** (default name) while still printing to the console.
- **`debug_conditions`** toggles very verbose per-column traces inside discrepancy loops.

---

## 10. Integration

| Direction | Contract |
|-----------|----------|
| **Upstream** | `vocabular` → `partly_df/vocabulary_mapping.xlsx`; rate card under `input/`. |
| **Downstream** | `pivot_creation.update_canf_file` reads **`Matched Shipments`** + **`comment`**. |

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
