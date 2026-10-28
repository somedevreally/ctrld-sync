# Control D Sync

A tiny Python script that keeps your Control D Folders in sync with a set of
remote block-lists.

## What it does
1. Downloads the current JSON block-lists.
2. Deletes any existing folders with the same names.
3. Re-creates the folders and pushes all rules in batches.

## Quick start

### Obtain Control D API token

1. Log in to your Control D account.
2. Navigate to the "Preferences > API" section.
3. Click the "+" button to create a new API token.
4. Copy the token value.

### Obtain Control D profile ID

1. Log in to your Control D account.
2. Open the Profile you want to sync.
3. Copy the profile ID from the URL.
```
https://controld.com/dashboard/profiles/741861frakbm/filters
                                        ^^^^^^^^^^^^
```

### Configure the script

1. **Clone & install**
   ```bash
   git clone https://github.com/your-username/ctrld-sync.git
   cd ctrld-sync
   uv sync
   ```

2. **Configure secrets**  
   Create a `.env` file (or set GitHub secrets) with:
   ```py
   TOKEN=your_control_d_api_token
   PROFILE=your_profile_id  # or comma-separated list of profile ids (e.g. your_id_1,your_id_2)
   ```

3. **Configure Folders**  
   Edit the `FOLDER_URLS` list in `main.py` to include the lists you want to sync.

   Each entry can be:

   - **A string** — treated as a JSON block-list URL (original behavior).
   - **A dict** — for plain-text lists (one domain per line) or to override settings:

   ```python
   {
       "url": "https://raw.githubusercontent.com/ShadowWhisperer/BlockLists/master/Whitelists/Filter",
       "name": "ShadowWhisperer Filter",   # optional; derived from URL if omitted
       "do": 1,                            # 0 = Block, 1 = Bypass/Allow (required for plain-text)
       "status": 1,                        # optional, default 1 (enabled)
       "format": "text",                   # optional; auto-detected if omitted
   }
   ```

   Plain-text files skip blank lines and lines starting with `#`. Inline comments (`domain.com # reason`) are also stripped.

> [!NOTE]
> Currently only Folders with one action are supported.
> Either "Block" (`do: 0`) or "Allow/Bypass" (`do: 1`) actions are supported.

4. **Run locally**
   ```bash
   uv run python main.py
   ```

5. **Run in CI**  
   The included GitHub Actions workflow runs daily at 02:00 UTC and on demand.

### Configure GitHub Actions

1. Fork this repo.
2. Go to the "Actions" Tab and enable actions.
3. Go to the Repo Settings.
4. Under "Secrets and variables > Actions" create the following secrets like above, under "Repository secrets":
   - `TOKEN`: your Control D API token
   - `PROFILE`: your Control D profile ID(s) 

## Requirements
- Python 3.12+  
- `uv` (for dependency management)
