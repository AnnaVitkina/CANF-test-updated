# `vocabular.py` — Column alignment and `vocabulary_mapping.xlsx`

**Path:** `test folder/vocabular.py`  
**Role:** Use the **rate card** column names as the **canonical vocabulary**, map columns from **ETOF**, **LC** (via part7 when LC+ETOF provided), and **Origin** onto that vocabulary using **direct dictionaries**, **fuzzy** string similarity, and optionally **sentence-transformers** embeddings; then **rename** source frames, write **`partly_df/vocabulary_mapping.xlsx`**, and return renamed DataFrames for downstream use.

---

## Dependencies

- **`part4_rate_card_processing`**: `process_rate_card`, `process_business_rules`, `transform_business_rules_to_conditions`, `find_business_rule_columns`, `get_business_rules_lookup`, `get_required_geo_columns`
- **`part1_etof_file_processing`**: `process_etof_file`
- **`part3_origin_file_processing`**: `process_origin_file`
- **`part7_optional_order_lc_etof_mapping`**: `process_order_lc_etof_mapping`
- **`pandas`**, **`difflib.SequenceMatcher`**
- **Optional:** `sentence_transformers.SentenceTransformer`, `sklearn.metrics.pairwise.cosine_similarity`, `numpy` — if import fails, **`SEMANTIC_AVAILABLE = False`** and fuzzy path is used.

---

## Global / configuration

- **`CURRENT_SHIPPER_ID`** via **`set_current_shipper(shipper_id)`** — affects **`normalize_for_semantics`** (e.g. **Apple**: port-of-loading/entry wording toward airport-style tokens).
- **`_semantic_model`**: lazy **`all-MiniLM-L6-v2`** in **`get_semantic_model()`**.

---

## Main entry point

### `map_and_rename_columns(...)`

**Key parameters:**

| Parameter | Role |
|-----------|------|
| `rate_card_file_path` | Under `input/`; may be **`rate_card_modified.xlsx`**. |
| `etof_file_path` | Optional. |
| `origin_file_path` | Optional; `.edi` skips default header row coercion in branch. |
| `origin_header_row` / `origin_end_column` | Passed to part3 for CSV/XLSX. |
| `order_files_path` | Passed to part7 when LC+ETOF processed. |
| `lc_input_path` | File, folder, or list — requires `etof_file_path` for LC branch. |
| `ignore_rate_card_columns` | List of rate card columns to **drop** before mapping. |
| `shipper_id` | Custom logic (e.g. **`dairb`**: rename **`SHAI Reference` → `SHIPMENT_ID`** on origin). |
| `output_txt_path` | Text log of mapping (written under partly_df in pipeline). |

**High-level steps:**

1. Validate rate card exists under `input/`, `process_rate_card`, apply **ignored** columns drop.
2. Derive **`rate_card_columns_to_map`**: exclude `is_excluded_column` and `RATE_CARD_EXCLUDED_COLUMNS` set in module.
3. Load business rules → **`find_business_rule_columns`** → remove those columns from semantic mapping targets (handled separately in matching).
4. Append **`get_required_geo_columns()`** to mapping targets if missing.
5. Load ETOF / Origin / LC (LC via **`process_order_lc_etof_mapping`**).
6. **`create_vocabulary_dataframe`** — per rate column, pick best source column and record **`Mapping_Method`**.
7. Rename columns on each dataframe; filter to relevant columns (rate card + ids + dates etc.).
8. **Excel output:** `partly_df/vocabulary_mapping.xlsx` with sheets **`ETOF`**, **`LC`**, **`Origin`** (if non-empty), **`Mapping`**.

**Returns:** **`(etof_renamed, lc_renamed, origin_renamed)`** — tuple of DataFrames (possibly empty).

On fatal rate card error returns **three empty DataFrames** (see code path).

---

## Other notable functions

| Function | Role |
|----------|------|
| `normalize_for_semantics` | String normalization before embedding/fuzzy. |
| `find_semantic_match_llm` | Name is legacy; implements rule-heavy fuzzy/semantic matching with **postal / country / flow / port** shortcut tables. |
| `calculate_string_similarity` | `SequenceMatcher` ratio. |
| `find_carrier_id_column` / `find_transport_mode_column` | Heuristics for **custom logic** branch. |
| `check_custom_logic` | Shipper + carrier + mode specific overrides when defined in **`custom_logic_dict`** (in-module configuration). |
| `is_date_column` / `is_shipment_id_column` / `is_excluded_column` | Column classification helpers. |
| **`create_vocabulary_dataframe`** | Core mapping table construction and console summary. |

---

## Contract with `matching_new.py`

**`run_matching`** expects **`partly_df/vocabulary_mapping.xlsx`** with at least **`ETOF`** and/or **`LC`** sheets populated after this module runs.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
