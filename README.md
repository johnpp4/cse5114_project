# Leftover to Makeover

Snowflake-backed recipe recommendation app with a FastAPI backend and static web UI.

## Quickstart (Snowflake-only)

1. Create and activate a virtual environment.
2. Install dependencies.
3. Ensure your `.env` includes all required Snowflake variables.
4. Start the API.
5. Open the UI in your browser.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn recommendation_api.main:app --reload
```

Open: [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Required Environment Variables

The app validates these at startup and fails fast if any are missing.

Required:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_WAREHOUSE`

Private key (set exactly one preferred source):
- `SNOWFLAKE_PRIVATE_KEY_B64` (recommended for portability)
- `SNOWFLAKE_PRIVATE_KEY`
- `SNOWFLAKE_PRIVATE_KEY_PATH` (supports absolute or repo-relative path)

Optional:
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`
- `SNOWFLAKE_ROLE`

## Smoke Test

1. Run the app with `uvicorn recommendation_api.main:app --reload`.
2. Visit `http://127.0.0.1:8000`.
3. Enter ingredients (one per line), for example:
   - chicken breast
   - olive oil
   - parmesan
4. Click **Find recipes**.
5. Confirm ranked results are returned from Snowflake without backend errors.
