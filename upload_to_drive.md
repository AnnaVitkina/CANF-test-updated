# `upload_to_drive.py` — Package `input` / `output` / `partly_df` to Google Drive

**Path:** `test folder/upload_to_drive.py`  
**Role:** Create a dated subfolder **`{Name} {Shipper} {dd.mm.yyyy}`** under a configured Drive base path, copy **`partly_df`**, **`input`**, and **`output`** from the script directory, optionally save **`comment.txt`**, supporting **Google Colab** mount or **local** Google Drive sync paths.

---

## Dependencies

- `os`, `shutil`, `datetime`
- **Colab:** `from google.colab import drive` when mounting

---

## Configuration constant

- **`GOOGLE_DRIVE_PATH`** — default **`"My Drive/CANF Reports"`**; edit for your team folder or Shared drive.

---

## Public API

### `get_user_input()`

Interactive CLI prompts: **Name** (required), **Shipper** (required), shows auto date, **multi-line comment** until **two consecutive empty lines** (or first-line empty skips comment).

### `upload_to_google_drive(google_drive_base_path, name=None, shipper_name=None, date_str=None, comment=None, local_base_folder=None)`

**Path logic:**

- **Colab:** mounts **`/content/drive`**, then resolves:
  - **`My Drive/...`** → under mount as given
  - **`Shared drives/...`** or **`Shareddrives/...`** → normalized to Colab **`Shareddrives`** layout
  - Else assumes subfolder of **`My Drive`**
- **Local:** `os.path.join(google_drive_base_path, folder_name)` — user must supply a real filesystem path to a synced Drive folder.

**Copies:** each of `partly_df`, `input`, `output` if present (files + subdirs with `copytree`). Errors on individual files are **silently skipped** (`except: pass`).

**Comment file:** `comment.txt` with header metadata + body.

**Returns:** final folder path string or **`None`**.

### `upload_from_colab(google_drive_folder_path=None)`

Uses **`GOOGLE_DRIVE_PATH`** when `None`, then calls **`upload_to_google_drive`** with prompts.

### `upload_with_params(...)`

Non-interactive variant.

---

## `__main__`

Runs **`upload_to_google_drive(google_drive_base_path=GOOGLE_DRIVE_PATH)`** (prompts if name/shipper not passed).

---

[Index](INDEX.md) · [Project overview](../../PROJECT_OVERVIEW.md)
