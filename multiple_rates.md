# `multiple_rates.py` — Merge multiple rate cards into `rate_card_modified.xlsx`

**Path:** `test folder/multiple_rates.py`  
**Role:** Validate that several native rate card Excel files share the same **mandatory (black-font) column set**, then **concatenate** lane rows, **merge** per-column **conditions** and **business rules**, attach **General info** metadata columns, and write **`input/rate_card_modified.xlsx`** in the format **`part4_rate_card_processing.process_rate_card`** expects when the filename contains **`rate_card_modified`**.

---

## Dependencies

- `pandas`, `openpyxl`, `os`, `shutil`, `sys`, `io`, `warnings`, `contextlib`
- **`part4_rate_card_processing`**: `process_rate_card`, `process_business_rules`, `transform_business_rules_to_conditions`, `clean_condition_text` (wrapped with stdout suppression for some calls)

---

## Design helpers

### `_suppress_output()`

Context manager: redirects stdout/stderr to `StringIO`, ignores warnings—keeps merge logs readable.

### `process_rate_card` / `process_business_rules`

Local wrappers that call part4 versions inside **`_suppress_output()`**.

---

## Public API (main building blocks)

| Function | Role |
|----------|------|
| `extract_general_info` | Reads **`General info`** sheet (up to ~50 rows): carrier agreement #, validity period split into **valid_from** / **valid_to**. |
| `get_mandatory_columns` | Mandatory columns = output of `process_rate_card` column list for one file. |
| `validate_mandatory_columns` | Compares all files to first file’s set; returns `(is_valid, reference_columns, differences_dict)`. |
| `combine_business_rules` | Concatenates all `process_business_rules` outputs with `source_file` on each rule. |
| `combine_conditions` | Per column: if same text → track multiple sources; if different → append `"\n[From file]: condition"`. |
| `process_multiple_rate_cards` | Validates (optional), combines rules/conditions, loops files adding **`Carrier agreement`**, **`Valid from`**, **`Valid to`**, **`Source file`**, `pd.concat` data. |
| **`save_combined_rate_cards`** | Writes Excel: **`Rate Card Data`**, **`Conditions`**, **`Business Rules`** (from transformed rules), **`Summary`**. Default path **`input/rate_card_modified.xlsx`**. |
| `process_rate_card_from_combined` | Read back combined file’s data + conditions only. |
| `process_rate_card_extended` | Single path / one-element list → native `process_rate_card`; multi → merged pipeline. |
| **`process_rate_cards`** | Alias for **`process_rate_card_extended`**. |

---

## Colab / convenience

- **`upload_and_merge_rate_cards()`** — `google.colab.files.upload()`, save under `input/`, merge or return single path.
- **`merge_rate_cards_from_folder(folder_path, pattern)`** — glob `*.xlsx` excluding `*rate_card_modified*`.

---

## Integration

- Place **`rate_card_modified.xlsx`** in **`input/`** before **`result.py`** / **`vocabular`** / **`matching`** when multiple carriers were merged.
- **`result.py`** warns if **multiple** uploads are detected without this pre-step.

---

## `__main__`

Tries Colab upload; else **`merge_rate_cards_from_folder("input")`**.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
