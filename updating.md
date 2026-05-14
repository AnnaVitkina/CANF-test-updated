# `updating.py` — Merge pivot metrics into `Data Collection.xlsx`

**Path:** `test folder/updating.py`  
**Role:** Read **`Pivot Data`** from a local **`Matched_Shipments_with.xlsx`** (default under **`output/`**), upsert rows into a long-running **`Data Collection.xlsx`** on **Google Drive** (or a local path that points to Drive sync), and apply **openpyxl** table styling.

---

## Dependencies

- `pandas`, `os`
- **Colab:** `google.colab.drive` for mount
- **`openpyxl`** for **`apply_excel_formatting`**

---

## Public API

### `update_data_collection(google_drive_path, local_output_folder=None, pivot_file_name="Matched_Shipments_with.xlsx", pivot_sheet_name="Pivot Data", collection_file_name="Data Collection.xlsx")`

1. Mount Drive if Colab; resolve **`collection_path`** from `google_drive_path` + `collection_file_name`.
2. Load pivot sheet; validate columns **`Shipper Value`**, **`Carrier`**, **`Cause of CANF`**, **`Amount`** (flexible name match via normalized compare).
3. Load existing collection or create empty frame with those columns.
4. For each pivot row: match on **(Shipper, Carrier, Cause)** — if exists, **add** `Amount`; else **append** row.
5. Save workbook then **`apply_excel_formatting(collection_path)`**.

**Returns:** **`bool`**.

### `apply_excel_formatting(file_path)`

Header style (dark blue / white text), alternating row fills, borders, numeric format on **Amount**, column widths, **freeze panes**, **auto_filter**.

### `update_from_colab(google_drive_folder_path)`

Thin wrapper calling **`update_data_collection`**.

---

## Notes

- Docstring references “run **matching.py** first”; in this repo the producing script is **`matching_new.py`** (output filename unchanged).
- **Local mode:** `drive_base` is empty; user must pass a **full** path that already includes Drive root if not using Colab.

---

## `__main__`

Prints usage instructions; example **`update_from_colab`** call is commented.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
