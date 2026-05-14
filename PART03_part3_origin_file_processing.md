# `part3_origin_file_processing.py` — Origin CSV / Excel / EDI (XML)

**Path:** `test folder/part3_origin_file_processing.py`  
**Role:** Load **origin** or supplemental shipment files from **`input/`**: Excel/CSV with a configurable **header row** and optional **column slice**, or **`.edi`** files treated as **XML** with a dedicated flattening path.

---

## Dependencies

- `pandas`, `xml.etree.ElementTree`, `os`, `pathlib`, `typing`

---

## Public API

### `process_origin_file(file_path, header_row=None, end_column=None)`

**`file_path`:** relative to **`input/`**.

| Extension | `header_row` | `end_column` |
|-----------|--------------|--------------|
| **`.edi`** | Ignored | Optional: `df.iloc[:, :end_column]` if `end_column > 0` |
| **`.xlsx` / `.xls` / `.csv`** | **Required** (else `ValueError`) | Optional column slice (1-based count of columns from the left) |

**Excel/CSV logic:**

- `pandas_header = header_row - 1` (headers are **1-based** like Excel).
- Read with `header=pandas_header`.
- If `end_column` is set: **`df.iloc[:, :end_column]`** (note: uses the integer as slice end in iloc; aligns with “first N columns” style usage in the project).

**Returns:** **`(df, column_names)`**.

### `process_edi_file(file_path)`

Invoked internally for `.edi`:

1. Parse XML from `input/<file_path>`.
2. Prefer rows from **`.//InvoiceDetails`** — one row per element via `parse_edi_xml_to_dict(invoice_detail, "InvoiceDetails")`.
3. Else try **`.//Message`**, else whole **root**.
4. Merges **`Envelope`** and **`InvoiceHeader`** flattened dicts into **each** row (same header on all rows).
5. List values in cells → joined with **`"; "`**.

**Returns:** **`(df, column_names)`**.

### `parse_edi_xml_to_dict(element, parent_path="", data_dict=None)`

Recursive flatten:

- Leaf with text → store at `parent_path_tag` (nested tags joined with `_`).
- Duplicate keys → promote to **list** of values.
- Attributes → `current_path_attrname`.

### `save_dataframe_to_excel(df, output_filename, folder_name="partly_df")`

Writes under `test folder/partly_df/` by default.

---

## Integration

- **`vocabular.map_and_rename_columns`**: loads origin when `origin_file_path` is set; for shipper **`dairb`**, renames **`SHAI Reference` → `SHIPMENT_ID`** after load (that rename lives in **vocabular.py**, not part3).

---

## `__main__` block

Example: `process_origin_file("file_dairb.xlsx", header_row=16, end_column=33)`.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
