# `part2_lc_processing.py` — LC / ISD XML → tabular orders

**Path:** `test folder/part2_lc_processing.py`  
**Role:** Collect **LC** (and **ISD**) **XML** files from `input/`-relative paths, parse every **`<ORDER>`** subtree into one **pandas** row per order, and return a combined DataFrame.

---

## Dependencies

- `xml.etree.ElementTree`, `pandas`, `pathlib`, `os`, `typing`

---

## Public API

### `find_lc_xml_files(folder_path, recursive=False)`

- Validates `folder_path` exists and is a directory.
- Glob: `*.xml` or `**/*.xml` if `recursive=True`.
- Keeps files whose name **starts with `LC` or `ISD`** (case-insensitive).
- Returns **sorted list of absolute paths** (`str(xml_file.resolve())`).

**Note:** Docstring still says “start with LC” in places; implementation includes **ISD**.

### `create_dataframe_from_xml_files(file_paths: List[str])`

For each file:

1. `ET.parse` → `root`.
2. `root.findall('.//ORDER')` — **all** ORDER nodes at any depth.
3. For each ORDER: dict with **`filename`** = basename, plus each **direct child** tag → text (`''` if `None`).
4. Appends one dict per ORDER; builds `pd.DataFrame(all_data)` or empty DataFrame.

**Errors:** `ParseError` / generic exceptions → **print** and skip file.

### `process_lc_input(input_path, recursive=False)`

**`input_path`:** `str` (single file or folder) or **`List[str]`** of such paths.

For each path:

- If not absolute → **`os.path.join("input", path)`**.
- **Missing path:** warning, `continue`.
- **File:** included only if name **starts with `LC`** AND ends with **`.xml`** (case on extension via `.upper().endswith('.XML')`). **ISD** single files are **not** accepted here (only via folder scan).
- **Directory:** uses `find_lc_xml_files` (includes LC + ISD).

Deduplicates paths, sorts, then `create_dataframe_from_xml_files`.

**Returns:** **`(df, column_names)`** or **`(empty DataFrame, [])`** if nothing found.

### `save_dataframe_to_excel(df, output_filename, folder_name="partly_df")`

Same pattern as part1: `test folder/partly_df/<output_filename>.xlsx`.

---

## Integration

- **`part7_optional_order_lc_etof_mapping`**: primary consumer of `process_lc_input`.
- **`result.py`**: passes LC filename(s) under `input/` after upload.

---

## Column contract for downstream steps

- LC rows typically include **`ORIG_FILE_NAME`** (used in part7 for order export matching).
- Column set is **union of all tags** seen across ORDER children in all files (sparse columns possible).

---

## `__main__` block

Example: `process_lc_input("lc_densir_13.01.2026.xml")` — expects file under `input/`.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
