# `pivot_creation.py` — Pivot Data sheet on matcher output

**Path:** `test folder/pivot_creation.py`  
**Role:** Post-process **`Matched_Shipments_with.xlsx`**: aggregate **Carrier × Cause of CANF** counts from the **`comment`** column (with line splitting and **pattern normalization**), add **Shipper Value**, and write a new sheet **`Pivot Data`** into the **same workbook**, preserving existing sheets. Optional **openpyxl** header styling.

---

## Dependencies

- `pandas`, `os`, `re`  
- Optional: **`openpyxl`** styles for formatting pass

---

## Functions

### `clean_comment_line(line)`

Transforms individual **comment lines** into **canonical “causes”** for pivoting:

- Drops lines starting with **`Discrepancies for Match`**.
- Skips “possible rate lanes” noise.
- Regex patterns collapse “value X → Y” into generic messages (shipment value needs change, rate card differs, date out of range, “Also:” alternate suggestions, etc.).
- Generic fallback strips quoted substrings.

Returns **`None`** to skip a line.

### `update_canf_file(matching_output_file=None, shipper_value=None)`

1. **Resolve input path** if `None`: script dir, parent `test folder`, or CWD **`Matched_Shipments_with.xlsx`**.
2. Read sheet **`Matched Shipments`** (fallback: first sheet).
3. Detect **carrier column:** best of `Carrier`, `CARRIER_NAME`, `Carier` by non-empty count.
4. Detect **comment column:** `comment` or `Comments`.
5. If both found:
   - Split comments by newline → **`clean_comment_line`** each.
   - Track carriers with no lines → add **`No comment`** row per carrier.
   - **`groupby(['Carrier','Cause of CANF']).size()`** → **`Amount`**.
   - Add **`Shipper Value`** (argument or `'Not provided'`).
   - Duplicate **`Carrier Name`** = **`Carrier`**.
   - Column order: **`Shipper Value`, `Carrier`, `Carrier Name`, `Cause of CANF`, `Amount`**.
6. Read **all existing sheets** into memory, rewrite file with **openpyxl** `ExcelWriter`: old sheets unchanged + **`Pivot Data`**. Optional formatting: header colors by sheet name, column widths, wrap on cause and comment columns, freeze panes.

**Returns:** **`True`/`False`**.

---

## Integration

- Called from **`result.py`** after **`run_matching`** when output file exists.

---

## `__main__`

Sets **`SHIPPER_VALUE`** and runs **`update_canf_file()`** with auto-detect path.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
