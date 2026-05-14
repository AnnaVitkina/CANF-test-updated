# `part1_etof_file_processing.py` — ETOF ingestion and optional enrichment

**Path:** `test folder/part1_etof_file_processing.py`  
**Role:** Load **ETOF** (Excel) shipment exports from the **`input/`** directory, normalize geography column names and values, drop UI/editor columns, and optionally **enrich** rows using **mismatch report** workbooks for specific shippers.

---

## Dependencies

- `pandas`, `openpyxl` (via `read_excel` / downstream saves)
- `os`, `pathlib.Path`

No imports from other project modules.

---

## Configuration (globals)

| Name | Default | Purpose |
|------|---------|---------|
| `SHIPPER_ID` | `None` | Used with mismatch reports to decide which enrichment runs. |
| `MISMATCH_REPORT_PATHS` | `None` | One filename or a **list** of filenames under `input/`. |

Set via **`configure_enrichment(shipper_id, mismatch_report_paths)`**, which assigns the globals. **`process_etof_file`** reads these on every call.

---

## Public API (order of typical use)

### 1. `configure_enrichment(shipper_id, mismatch_report_paths)`

Call **once** before `process_etof_file` when enrichment is needed.  
`mismatch_report_paths` may be a **string** or **list of strings**, each path **relative to `input/`**.

### 2. `process_etof_file(file_path)`

**Input:** `file_path` — path **relative to `input/`** (e.g. `etof_file.xlsx`).

**Steps:**

1. Build `full_path = os.path.join("input", file_path)`.
2. **`pd.read_excel(full_path, skiprows=1)`** — first Excel row is skipped (template / title row).
3. **Rename** duplicate column pairs to explicit Origin / Destination names:

   | Original | New |
   |----------|-----|
   | `Country code` | `Origin Country` |
   | `Postal code` | `Origin Postal Code` |
   | `Airport` | `Origin Airport` |
   | `City` | `Origin City` |
   | `Country code.1` | `Destination Country` |
   | `Postal code.1` | `Destination Postal Code` |
   | `Airport.1` | `Destination Airport` |
   | `City.1` | `Destination City` |
   | `Seaport` | `Origin Seaport` |
   | `Seaport.1` | `Destination Seaport` |

4. **Drop columns** (only if present):  
   `Match`, `Approve`, `Calculation`, `State`, `Issue`, `Carrier agreement #`,  
   `Currency`, `Value`, `Currency.1`, `Value.1`, `Currency.2`, `Value.2`.

5. **Normalize country cells:** if value is a string containing `' - '`, keep only the substring **before** `' - '` (intended to strip `"DE - Germany"` → `"DE"`).

6. **Enrichment** (only if both `SHIPPER_ID` and `MISMATCH_REPORT_PATHS` are set):

   - `enrich_etof_with_shipment_id(...)`
   - `enrich_etof_with_service(...)`

**Output:** **`(df_etofs, column_names)`** — DataFrame and `columns.tolist()`.

### 3. `load_mismatch_reports(mismatch_report_paths)`

Loads each path under `input/`, **`pd.concat`** with `ignore_index=True`.  
Used internally by enrichment functions.

### 4. `enrich_etof_with_shipment_id(df_etofs, shipper_id, mismatch_report_paths)`

- Runs only if **`shipper_id.lower() == 'iffdgf'`**.
- If **`SHIPMENT_ID`** already exists and has any non-empty value → returns `df_etofs` unchanged.
- Else builds mapping **`ETOF_NUMBER` → `SHIPMENT_ID`** from mismatch report(s).
- Sets **`df_etofs['SHIPMENT_ID'] = df_etofs['ETOF #'].astype(str).map(...)`**.

**Required mismatch columns:** `ETOF_NUMBER`, `SHIPMENT_ID`.  
**Required ETOF column:** `ETOF #`.

### 5. `enrich_etof_with_service(df_etofs, shipper_id, mismatch_report_paths)`

- Runs only if **`shipper_id.lower() == 'apple'`**.
- Requires ETOF column **`Service`** and mismatch columns **`SERVICE_ISD`**, **`ETOF_NUMBER`**.
- Maps **`ETOF #`** → **`SERVICE_ISD`**; **`fillna`** keeps original `Service` when no mapping.

Verbose **`print`** diagnostics.

### 6. `save_dataframe_to_excel(df, output_filename, folder_name="partly_df")`

Writes to **`Path(__file__).parent / folder_name / output_filename`** (default `test folder/partly_df/`).

---

## Integration in the wider pipeline

- **`result.py`**: copies uploaded ETOF to `input/`, optionally calls `configure_enrichment`, then `process_etof_file`.
- **`part7_optional_order_lc_etof_mapping.py`**: imports `process_etof_file` to align LC with ETOF.
- **`vocabular.py`**: imports `process_etof_file` to read ETOF columns for mapping.

---

## `__main__` block

Example: `configure_enrichment` with Apple + mismatch list, then `process_etof_file('etof_apple_test.xlsx')`, then `save_dataframe_to_excel`.

---

## Edge cases and constraints

- All paths assume a working directory where **`input/`** exists next to the process that resolves paths (Gradio flow `chdir`s to the script directory).
- Enrichment is **silent** (skipped) for shippers other than `iffdgf` / `apple` even if mismatch files are configured.
- Single-file ETOF format must match **skiprows=1** and expected column names after export.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
