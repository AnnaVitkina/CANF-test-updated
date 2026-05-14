# `part7_optional_order_lc_etof_mapping.py` — Link LC orders to ETOF lines

**Path:** `test folder/part7_optional_order_lc_etof_mapping.py`  
**Role:** Build an **LC-centric** DataFrame that includes **`ETOF #`** and **`LC #`**, using one of three strategies: **SHIPMENT_ID** match, **delivery number** match, or **order file # ↔ LC #** on ETOF (with optional order export for the first hop).

---

## Dependencies

- `pandas`, `os`, `difflib`, `pathlib`
- **`part5_order_files_export_processing`** → `process_order_files_export`
- **`part2_lc_processing`** → `process_lc_input`
- **`part1_etof_file_processing`** → `process_etof_file` (respects global enrichment if configured earlier)

---

## Public API

### `fuzzy_match_filename(filename, order_file_names)`

- Normalizes: basename, lower strip, **strip extension**.
- Exact match on normalized string; else **`difflib.get_close_matches(..., n=1, cutoff=0.7)`**.
- Returns matched **original** order file name from the list, or **`None`**.

### `map_order_file_to_lc(order_files_dataframe, lc_dataframe)`

**Requires:**

- Order DF: **`Order file #`**, **`Order file name`**
- LC DF: **`ORIG_FILE_NAME`**

Adds **`Order file #`** to LC by fuzzy-matching **`ORIG_FILE_NAME`** to **`Order file name`**.

### `map_etof_to_lc(etof_dataframe, lc_dataframe_updated)`

**Requires on ETOF:** **`ETOF #`** always.

**Branch selection (mutually exclusive priority):**

1. **`SHIPMENT_ID`** in **both** ETOF and LC → map **`ETOF #`** and optionally **`LC #`** from ETOF onto LC rows by shared `SHIPMENT_ID`. If no `shipment_to_lc` map but **`Order file #`** exists → rename to **`LC #`**; else **`LC #`** = None.

2. Else if **delivery** columns found:
   - LC: first match among  
     `DELIVERY_NUMBER`, `Delivery Number`, `delivery_number`, `DeliveryNumber`, `DELIVERY NUMBER`
   - ETOF: first match among  
     `DELIVERY NUMBER(s)`, `DELIVERY_NUMBER(s)`, `Delivery Number(s)`, `DELIVERY NUMBER`, `DELIVERY_NUMBER`, `Delivery Number`, `delivery_number`  
   - Builds **full-string** maps and **per-number** maps (split on `;` or `,`). LC row lookup tries **full string first**, then **single** delivery id.

3. Else **legacy path:** LC must have **`Order file #`**; ETOF must have **`LC #`**. Build **`lc_to_etof`** from ETOF rows; for each LC row, **`ETOF #` = lc_to_etof[Order file #]`**; rename **`Order file #` → `LC #`**.

**Returns:** **`(lc_dataframe_final, column_names)`**.

### `process_order_lc_mapping(order_files_path, lc_input_path, lc_recursive=False)`

Order export + LC only → **`map_order_file_to_lc`** → saves **`partly_df/order_lc_mapping.xlsx`**.

### `process_order_lc_etof_mapping(lc_input_path, etof_path, order_files_path=None, lc_recursive=False)`

Full chain:

1. `process_lc_input`
2. If `order_files_path`: `map_order_file_to_lc(process_order_files_export(...))` and output stub name **`order_lc_etof_mapping.xlsx`**; else **`lc_etof_mapping.xlsx`**
3. `process_etof_file(etof_path)`
4. `map_etof_to_lc`
5. Save to **`partly_df/`** with chosen filename.

**Returns:** **`(lc_dataframe_final, lc_column_names)`**.

---

## Integration

- **`vocabular.map_and_rename_columns`**: when both `lc_input_path` and `etof_file_path` are set, calls **`process_order_lc_etof_mapping`** (with optional order path).
- **`result.py`**: runs when LC list and ETOF filename exist.

---

## `__main__` block

Example paths `input_iff` / `etof_file_iff.xlsx` (commented `order_files_path`).

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
