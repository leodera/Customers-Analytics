"""Same idea as db_utils.py, but for local SQLite and DuckDB files.

    from local_db_utils import run_duckdb_query as dk, run_sqlite_query as sl, upload_file, copy_from_server

File locations (optional, put them in your .env file):
    SQLITE_PATH=brewline.sqlite
    DUCKDB_PATH=brewline.duckdb
"""
import datetime
import decimal
import glob
import os
import re
import sqlite3

import pandas as pd

try:  # optional: only used to read SQLITE_PATH / DUCKDB_PATH from .env
    from dotenv import load_dotenv

    load_dotenv(override=True)
except ImportError:
    pass

SUPPORTED_EXTENSIONS = (".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl")

DEFAULT_PATHS = {
    "sqlite": "local_data.sqlite",
    "duckdb": "local_data.duckdb",
}
PATH_ENV_VARS = {"sqlite": "SQLITE_PATH", "duckdb": "DUCKDB_PATH"}

# One open connection per database file, reused.
_connections = {}


# ---------------------------------------------------------------------------
# Connections and queries
# ---------------------------------------------------------------------------

def _check_db_type(db_type):
    if db_type not in ("sqlite", "duckdb"):
        raise ValueError("db_type must be 'sqlite' or 'duckdb'.")


def get_connection(db_type="duckdb", db_path=None):
    """Return a cached connection to a SQLite or DuckDB file (created if missing)."""
    _check_db_type(db_type)
    path = db_path or os.getenv(PATH_ENV_VARS[db_type]) or DEFAULT_PATHS[db_type]
    key = (db_type, os.path.abspath(path))

    if key not in _connections:
        if db_type == "sqlite":
            _connections[key] = sqlite3.connect(path)
        else:
            try:
                import duckdb
            except ImportError:
                raise ImportError("DuckDB is not installed. Run: pip install duckdb")
            _connections[key] = duckdb.connect(path)

    return _connections[key]


def close_connections():
    """Close every open file. Do this if another program needs the DuckDB file."""
    for conn in _connections.values():
        conn.close()
    _connections.clear()


def _is_select(sql_query):
    """True if the query returns rows (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, PRAGMA...)."""
    cleaned = re.sub(r"^(\s*(--[^\n]*\n|/\*.*?\*/))*", "", sql_query, flags=re.DOTALL)
    cleaned = cleaned.strip().lower()
    return cleaned.startswith(
        ("select", "with", "show", "describe", "desc ", "explain", "pragma", "summarize", "from")
    )


def execute_sql(sql_query, db_type="duckdb", params=None, db_path=None):
    """Run SQL on a local SQLite or DuckDB file.

    SELECT-type queries return a DataFrame. Everything else returns a short message.
    params: use ? placeholders with a list, e.g. execute_sql("SELECT * FROM t WHERE id = ?", params=[5])
    """
    conn = get_connection(db_type, db_path)

    if db_type == "sqlite":
        if _is_select(sql_query):
            return pd.read_sql_query(sql_query, conn, params=params)
        cur = conn.execute(sql_query, params or [])
        conn.commit()
        if cur.rowcount is not None and cur.rowcount >= 0:
            return f"Execution successful. Rows affected: {cur.rowcount}"
        return "Execution successful."

    # DuckDB
    result = conn.execute(sql_query, params) if params else conn.execute(sql_query)
    if _is_select(sql_query):
        return result.df()
    return "Execution successful."


def run_sqlite_query(sql_query, params=None, db_path=None):
    return execute_sql(sql_query, db_type="sqlite", params=params, db_path=db_path)


def run_duckdb_query(sql_query, params=None, db_path=None):
    return execute_sql(sql_query, db_type="duckdb", params=params, db_path=db_path)


# ---------------------------------------------------------------------------
# Helpers for loading data
# ---------------------------------------------------------------------------

def _clean_column_names(columns):
    """Lowercase snake_case names, e.g. 'Monthly Income' -> 'monthly_income'."""
    cleaned, seen = [], {}
    for i, col in enumerate(columns):
        name = re.sub(r"[^0-9a-zA-Z]+", "_", str(col).strip()).strip("_").lower()
        if not name:
            name = f"column_{i + 1}"
        elif name[0].isdigit():
            name = f"col_{name}"

        if name in seen:  # duplicate name -> add a number
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        cleaned.append(name)
    return cleaned


def _check_name(name, label):
    """Only allow plain table names (letters, numbers, underscores)."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(
            f"Invalid {label} '{name}'. Use only letters, numbers and "
            "underscores, and don't start with a number."
        )
    return name.lower()


def _find_files(path):
    """Accepts one file, a folder, or a pattern like 'data/*.csv'."""
    if os.path.isdir(path):
        files = [os.path.join(path, f) for f in os.listdir(path)]
        files = [f for f in files if os.path.isfile(f)]
    elif any(ch in path for ch in "*?["):
        files = glob.glob(path)
    elif os.path.isfile(path):
        files = [path]
    else:
        raise FileNotFoundError(f"Nothing found at '{path}'.")

    files = [
        f for f in files
        if f.lower().endswith(SUPPORTED_EXTENSIONS)
        and not os.path.basename(f).startswith("~$")  # Excel temp files
    ]
    return sorted(files)


def _read_chunks(file_path, chunksize, read_kwargs):
    """Yield DataFrames. CSV/TSV are streamed in chunks; other types load at once."""
    ext = os.path.splitext(file_path)[1].lower()
    kwargs = dict(read_kwargs or {})

    if ext in (".csv", ".tsv"):
        kwargs.setdefault("low_memory", False)
        if ext == ".tsv":
            kwargs.setdefault("sep", "\t")
        yield from pd.read_csv(file_path, chunksize=chunksize, **kwargs)
    elif ext in (".xlsx", ".xls"):
        yield pd.read_excel(file_path, **kwargs)
    elif ext == ".json":
        yield pd.read_json(file_path, **kwargs)
    elif ext == ".jsonl":
        kwargs.setdefault("lines", True)
        yield pd.read_json(file_path, **kwargs)


def _prepare_for_local(df, db_type):
    """Make values safe to store: Decimal -> float; for SQLite, dates -> 'YYYY-MM-DD' text."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype != object:
            continue
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        first = non_null.iloc[0]

        if isinstance(first, decimal.Decimal):
            df[col] = df[col].map(lambda v: float(v) if isinstance(v, decimal.Decimal) else v)
        elif db_type == "sqlite" and isinstance(first, (datetime.date, datetime.datetime)):
            df[col] = df[col].map(
                lambda v: v.isoformat() if isinstance(v, (datetime.date, datetime.datetime)) else v
            )
    return df


def _duckdb_table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table_name]
    ).fetchone()
    return row is not None


def _write_df(conn, db_type, table_name, df):
    """Append a DataFrame to a table, creating the table if it doesn't exist."""
    df = _prepare_for_local(df, db_type)

    if db_type == "sqlite":
        df.to_sql(table_name, conn, if_exists="append", index=False, chunksize=10000)
        conn.commit()
        return

    # DuckDB
    cols = ", ".join(f'"{c}"' for c in df.columns)
    conn.register("_incoming_df", df)
    try:
        if _duckdb_table_exists(conn, table_name):
            conn.execute(
                f'INSERT INTO "{table_name}" ({cols}) SELECT {cols} FROM _incoming_df'
            )
        else:
            conn.execute(f'CREATE TABLE "{table_name}" AS SELECT {cols} FROM _incoming_df')
    finally:
        conn.unregister("_incoming_df")


# ---------------------------------------------------------------------------
# Universal file upload
# ---------------------------------------------------------------------------

def upload_file(
    path,
    table_name=None,
    db_type="duckdb",
    drop_existing=False,
    chunksize=100000,
    clean_columns=True,
    read_kwargs=None,
    db_path=None,
):
    """Upload any CSV / TSV / Excel / JSON file (or a folder of them) to SQLite or DuckDB.

    The table is created automatically from the file's columns if it doesn't exist.

    path           A file, a folder, or a pattern like "data/*.csv".
                   Several files all go into the same table (they must have the same columns).
    table_name     Name of the table. If left out, it is taken from the file name
                   (only allowed when uploading a single file).
    db_type        "duckdb" (default) or "sqlite".
    drop_existing  False (default): add rows if the table already exists.
                   True: DELETE the table first and reload from scratch.
    chunksize      Rows read at a time from CSV/TSV files (keeps memory low).
    clean_columns  True (default): column names become lowercase_with_underscores.
    read_kwargs    Extra options for pandas, e.g. {"sheet_name": "Sales"} for Excel,
                   {"sep": ";"} or {"encoding": "latin-1"} for CSV,
                   {"parse_dates": ["order_date"]} to read dates properly.
    db_path        Database file. Default: SQLITE_PATH / DUCKDB_PATH from .env,
                   otherwise local_data.sqlite / local_data.duckdb.

    Returns the number of rows uploaded.

    Examples:
        upload_file("sales.csv", "sales")
        upload_file("exports/", "orders", db_type="sqlite", drop_existing=True)
        upload_file("brew/orders.csv", read_kwargs={"parse_dates": ["order_date"]})
    """
    _check_db_type(db_type)

    files = _find_files(path)
    if not files:
        print(f"No supported files found at '{path}'. Supported: {', '.join(SUPPORTED_EXTENSIONS)}")
        return 0

    if table_name is None:
        if len(files) > 1:
            raise ValueError("Please give a table_name when uploading more than one file.")
        stem = os.path.splitext(os.path.basename(files[0]))[0]
        table_name = _clean_column_names([stem])[0]

    table_name = _check_name(table_name, "table name")
    conn = get_connection(db_type, db_path)

    if drop_existing:
        print(f"drop_existing=True -> dropping {table_name}")
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        if db_type == "sqlite":
            conn.commit()

    print(f"Uploading {len(files)} file(s) to {table_name} ({db_type})...")

    expected_cols = None
    total_rows = 0

    for file_path in files:
        file_name = os.path.basename(file_path)
        print(f"\nUploading {file_name}...")
        file_rows = 0

        for df in _read_chunks(file_path, chunksize, read_kwargs):
            if df.empty:
                continue

            if clean_columns:
                df.columns = _clean_column_names(df.columns)

            cols = list(df.columns)
            if expected_cols is None:
                expected_cols = cols
            elif cols != expected_cols:
                raise ValueError(
                    f"Columns in '{file_name}' don't match the first file.\n"
                    f"Expected: {expected_cols}\nFound:    {cols}"
                )

            _write_df(conn, db_type, table_name, df)
            file_rows += len(df)
            total_rows += len(df)
            print(f"  -> {file_rows:,} rows uploaded")

        print(f"Finished {file_name}.")

    count = execute_sql(f'SELECT COUNT(*) AS total FROM "{table_name}"', db_type, db_path=db_path)
    print(f"\nDone. {table_name} now has {int(count['total'].iloc[0]):,} rows ({total_rows:,} uploaded just now).")
    return total_rows


# ---------------------------------------------------------------------------
# Copy tables from your MySQL / PostgreSQL server into a local file
# ---------------------------------------------------------------------------

def copy_from_server(
    source="mysql",
    tables=None,
    db_type="duckdb",
    schema=None,
    drop_existing=True,
    chunk_rows=100000,
    db_path=None,
):
    """Copy tables from your MySQL or PostgreSQL server into a local SQLite/DuckDB file.

    Needs db_utils.py in the same folder, with MYSQL_URL / POSTGRES_URL set in .env.

    source         "mysql" or "pg".
    tables         List of table names. Default: every table in the database
                   (MySQL: the one in your MYSQL_URL; PostgreSQL: the given schema).
    db_type        "duckdb" (default) or "sqlite".
    schema         PostgreSQL schema (default "public"). Not used for MySQL.
    drop_existing  True (default): replace the local copy of each table.
                   False: add rows to existing local tables.

    Returns a dict {table_name: rows_copied}.

    Examples:
        copy_from_server()                                     # MySQL -> DuckDB, all tables
        copy_from_server("mysql", db_type="sqlite")
        copy_from_server("pg", tables=["employees"], schema="hr_analytics")
    """
    from sqlalchemy import text

    from db_utils import execute_sql as server_sql
    from db_utils import get_engine

    _check_db_type(db_type)
    if source not in ("mysql", "pg"):
        raise ValueError("source must be 'mysql' or 'pg'.")

    server_schema = _check_name(schema, "schema name") if schema else ("public" if source == "pg" else None)

    if tables is None:
        if source == "mysql":
            listing = server_sql(
                "SELECT table_name AS name FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' ORDER BY 1",
                db_type="mysql",
            )
        else:
            listing = server_sql(
                "SELECT table_name AS name FROM information_schema.tables "
                "WHERE table_schema = :s AND table_type = 'BASE TABLE' ORDER BY 1",
                db_type="pg",
                params={"s": server_schema},
            )
        tables = listing["name"].tolist()

    if not tables:
        print("No tables found to copy.")
        return {}

    engine = get_engine(source)
    local = get_connection(db_type, db_path)
    results = {}

    for t in tables:
        if any(ch in t for ch in "`\"';\\"):
            raise ValueError(f"Invalid table name '{t}'.")
        qualified = f"{server_schema}.{t}" if (source == "pg" and server_schema) else t
        quote = "`" if source == "mysql" else '"'
        server_name = (
            f"{server_schema}.{quote}{t}{quote}" if (source == "pg" and server_schema)
            else f"{quote}{t}{quote}"
        )
        local_name = re.sub(r"[^0-9a-zA-Z_]+", "_", t).lower()

        if drop_existing:
            local.execute(f'DROP TABLE IF EXISTS "{local_name}"')
            if db_type == "sqlite":
                local.commit()

        print(f"Copying {qualified} -> {local_name} ({db_type})...")
        rows = 0
        with engine.connect() as conn:
            for df in pd.read_sql_query(text(f"SELECT * FROM {server_name}"), conn, chunksize=chunk_rows):
                if df.empty:
                    continue
                _write_df(local, db_type, local_name, df)
                rows += len(df)
                print(f"  -> {rows:,} rows")

        results[local_name] = rows

    print("\nDone. Copied:")
    for name, n in results.items():
        print(f"  {name}: {n:,} rows")
    return results

