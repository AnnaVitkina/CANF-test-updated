# `result.py` — Gradio CANF Analyzer (end-to-end UI)

**Path:** `test folder/result.py`  
**Role:** **Single entry** for operators: copy uploaded files into **`input/`** next to the script, **`os.chdir`** to script directory, run **part1 → part2 → part3 → part4 → part7 → vocabular → matching → pivot**, then copy the matcher output to **`output/Result.xlsx`** and return paths + status text to **Gradio** widgets.

---

## Dependencies

- **`gradio`**
- Dynamic imports: **`part1`** … **`part7`**, **`vocabular`**, **`matching`**, **`pivot_creation`**

---

## Startup: `setup_python_path()`

Inserts **script directory** and optional **`test folder`** / Colab paths into **`sys.path`** so imports work from Colab or nested layouts.

---

## Core workflow: `run_full_workflow_gradio(...)`

**Parameters (conceptual):**

| Parameter | Role |
|-----------|------|
| `rate_card_file` | Gradio `File`; may be **multiple**; copied preserving names under `input/`. |
| `etof_file` | **Required** — copied to `etof_file.<ext>`. |
| `lc_file` | Optional list — multiple LC XML paths. |
| `origin_file` | Optional — `origin_file.<ext>`. |
| `order_files` | Optional — `order_files.<ext>`. |
| `shipper_id` | **Required** string — enrichment + vocabular customizations. |
| `mismatch_report_files` | Optional list → **`configure_enrichment`**. |
| `origin_header_row` / `origin_end_column` | Passed to part3 and vocabular. |
| `ignore_rate_card_columns` | Comma-separated string → list for **`map_and_rename_columns`**. |

**Order of operations** (see source for exact try/except granularity):

1. Validate **ETOF** + **shipper_id**.
2. Create **`input/`**, **`output/`** beside script.
3. Copy uploads into **`input/`**; if multiple rate cards → **warning** to pre-merge with **`multiple_rates`** (expects **`rate_card_modified.xlsx`**).
4. `chdir(script_dir)`.
5. **Part1** ETOF (+ optional `configure_enrichment`).
6. **Part2** LC if any.
7. **Part3** origin if any.
8. **Part4** rate card — prefers **`input/rate_card_modified.xlsx`** if present.
9. **Part7** if LC + ETOF present.
10. **Vocabular** `map_and_rename_columns` with resolved rate card path.
11. **`from matching import run_matching`** — **must resolve** to an installed `matching.py` or adjusted import (see below).
12. **`pivot_creation.update_canf_file`** if matcher output exists.
13. Copy **`Matched_Shipments_with.xlsx`** → **`output/Result.xlsx`**, or write a **status-only** workbook if matching failed.

**Returns:** **`(final_file_path, status_text)`** for Gradio File + Textbox.

---

## Gradio UI (`demo`)

- Accordion with instructions.
- File inputs for rate card(s), ETOF, LC (with **accumulator** `lc_files_state` — UI filter keeps only **`LC*.xml`** for accumulation; **ISD**-only uploads may be excluded).
- Origin toggles extra number inputs for header row / end column.
- **Ignore rate card columns** textbox.
- **Run** button → `launch_workflow` → `run_full_workflow_gradio`.

### `__main__`

Creates `input/` and `output/`, then **`demo.launch`** — Colab uses `0.0.0.0`, local uses `127.0.0.1`.

---

## Known integration gap

```python
from matching import run_matching
```

Repository includes **`matching_new.py`**, not necessarily **`matching.py`**. Fix options:

1. Add **`matching.py`** that re-exports: `from matching_new import run_matching` (and any other symbols), or  
2. Change import to **`matching_new`**.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
