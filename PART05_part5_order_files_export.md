# `part5_order_files_export_processing.py` — Order files export (two columns)

**Path:** `test folder/part5_order_files_export_processing.py`  
**Role:** Read an **Order Files Export** Excel file from **`input/`** and return **only** the two columns required to link system order numbers to human-readable file names for **part7**.

---

## Dependencies

- `pandas`, `os`

---

## Public API

### `process_order_files_export(file_path)`

**Input:** `file_path` relative to **`input/`**.

1. `full_path = os.path.join("input", file_path)`.
2. `pd.read_excel(full_path)` — **first sheet** default.
3. Validates presence of **exact** column names:
   - **`Order file #`**
   - **`Order file name`**
4. If missing → **`ValueError`** with list of available columns for debugging.
5. Returns **`df[required_cols].copy()`**.

---

## Integration

- **`part7_optional_order_lc_etof_mapping`**: `process_order_files_export` → `map_order_file_to_lc` matches **`Order file name`** to LC **`ORIG_FILE_NAME`**.

---

## Output shape

| Column | Typical meaning |
|--------|------------------|
| `Order file #` | Internal order / LC key used on ETOF side as **`LC #`**. |
| `Order file name` | Filename string matched (fuzzily) to LC XML origin file name. |

---

## `__main__`

Commented example only.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
