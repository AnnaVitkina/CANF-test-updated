# `part4_rate_card_processing.py` — Rate card lanes, conditions, business rules

**Path:** `test folder/part4_rate_card_processing.py`  
**Role:** Extract **mandatory rate attributes** (black-font columns), **per-column condition text** (from Excel comments / notes), and **business rules** (postal zones, country regions) from a standard rate card workbook—or load a **pre-merged** `rate_card_modified.xlsx` produced by **`multiple_rates.py`**.

---

## Dependencies

- `pandas`, `openpyxl`, `os`  
- `re` inside `process_business_rules`

---

## Two ingestion modes

### A. Native rate card Excel

Triggered when **`'rate_card_modified'`** is **not** in `file_path` (case-insensitive substring check fails).

1. **`pd.read_excel(..., sheet_name="Rate card", skiprows=2)`**.
2. Detect **first column index** where first row cell is not `"nan"` string → truncate sheet to “real” table width.
3. Drop rows with NaN in first column; set column names from **first remaining row**; data starts row after that.
4. **`openpyxl.load_workbook`** on same file, sheet **`Rate card`**:
   - Find row (within first ~10 rows) containing **`Currency`** → header row and **currency column index**.
   - **Truncated headers** = cells left of Currency on that row.
   - **Conditions:** for each header cell, prefer **cell.comment.text**; else optional row-2 cell value.
   - **Black font columns:** scan font RGB; treat **`000000`** as black; grey detection for near-equal RGB; keep only **black** columns (first occurrence per duplicate name).
5. Build **`conditions`** dict: `clean_condition_text` applied to each column’s note.

**Returns:** **`(df_filtered_rate_card, column_names, conditions)`**.

### B. Pre-combined file (`rate_card_modified`)

When **`'rate_card_modified' in file_path.lower()`**:

- **`_load_combined_rate_card`:** sheet **`Rate Card Data`**, sheet **`Conditions`** (`Has Condition` / `Condition Rule`).
- Business rules for lookups may be loaded via **`_load_combined_business_rules`** when other code reads the workbook (see `get_business_rules_lookup` path in source).

---

## Key functions (alphabetical by concern)

| Function | Purpose |
|----------|---------|
| `clean_condition_text` | Strip boilerplate (“Conditional rules:”), remove ALLCAPS column tokens before operators, normalize whitespace—helps **`matching`** parse rules. |
| `find_business_rule_columns` | Scan rate card **values** for names that match parsed rule keys; returns `rule_to_columns`, `column_to_rules`, `unique_columns`. |
| `format_business_rule_condition` | Human-readable summary for exports. |
| `get_business_rules_lookup` | Orchestrates `process_business_rules` + `transform_business_rules_to_conditions` + `process_rate_card` + `find_business_rule_columns` → lookup dict for **matching** (country/postal per rule, columns containing rules). |
| `get_required_geo_columns` | Fixed list: Origin/Destination Country + Postal Code—used by **vocabular** to extend mapping targets. |
| `process_business_rules` | Parses sheet **`Business rules`** (must exist): markers **Postal code zones**, **Country regions**, **No data added**; section header row; subsequent data rows into **`raw_rules`** and typed lists. |
| `process_rate_card` | Main entry: native vs combined (see above). |
| `save_rate_card_output` | Debug/artifact: writes **`partly_df/Filtered_Rate_Card_with_Conditions.xlsx`** with Rate Card Data, Conditions, Business Rules, Summary. |
| `transform_business_rules_to_conditions` | Converts each raw rule to `{section, country, postal_codes[], exclude, raw_postal_code}` keyed by **rule name**. |

---

## Sheet / naming assumptions (native mode)

- Sheet name **`Rate card`** (space, lowercase “card” as in code).
- Sheet **`Business rules`** for extended validation (optional for matching if empty).

---

## Integration

- **`vocabular`**, **`matching_new`**, **`multiple_rates`**, **`result.py`** all call `process_rate_card` and/or `get_business_rules_lookup` / `process_business_rules`.

---

## `__main__` block

Runs `save_rate_card_output(INPUT_FILE)` and prints conditions / business rules summary.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
