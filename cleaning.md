# `cleaning.py` — Wipe working folders

**Path:** `test folder/cleaning.py`  
**Role:** Delete **all files and subdirectories** inside **`input/`**, **`output/`**, and **`partly_df/`** next to the script (or under `getcwd()` when `__file__` is unavailable), without removing the folder nodes themselves.

---

## Dependencies

- `os`, `shutil`

---

## Public API

### `clean_folder(folder_path)`

Iterates directory entries:

- **File or symlink:** `os.unlink`
- **Subdirectory:** `shutil.rmtree`

Collects deleted paths into a list; prints failures. **Returns** list of deleted item paths.

### `clean_input_and_output_folders()`

Resolves **`script_dir`** (Colab-safe), then runs **`clean_folder`** on:

- `script_dir/input`
- `script_dir/output`
- `script_dir/partly_df`

Prints counts and first few deleted paths.

---

## When to use

Before a **clean re-run** of vocabular + matching to avoid stale **`vocabulary_mapping.xlsx`** or old **`Matched_Shipments_with.xlsx`** confusing debugging.

---

## `__main__`

Calls **`clean_input_and_output_folders()`**.

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
