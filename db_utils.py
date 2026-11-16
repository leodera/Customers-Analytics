import glob
import os
import re

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    MetaData,
    Table,
    Text,
    create_engine,
    inspect,
    text,
)

load_dotenv(override=True)

# One engine per database type, created once and reused.
_engines = {}

SUPPORTED_EXTENSIONS = (".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl")


# ---------------------------------------------------------------------------
# Connections and queries
# ---------------------------------------------------------------------------

def get_engine(db_type="pg"):
    """Return a cached SQLAlchemy engine for 'pg' (PostgreSQL) or 'mysql'."""
    if db_type not in _engines:
        url_key = "POSTGRES_URL" if db_type == "pg" else "MYSQL_URL"
        db_url = os.getenv(url_key)

        if not db_url:
            raise ValueError(f"Environment variable '{url_key}' is not set.")

        # pool_pre_ping checks the connection is alive before using it,
        # which helps with cloud databases that close idle connections.
        _engines[db_type] = create_engine(db_url, pool_pre_ping=True)

    return _engines[db_type]


def _is_select(sql_query):
    """True if the query returns rows (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN),
    ignoring leading comments."""
    cleaned = re.sub(r"^(\s*(--[^\n]*\n|/\*.*?\*/))*", "", sql_query, flags=re.DOTALL)
    cleaned = cleaned.strip().lower()
    return cleaned.startswith(("select", "with", "show", "describe", "desc ", "explain"))


def execute_sql(sql_query, db_type="pg", params=None):
    """Executes SQL statements against PostgreSQL or MySQL.

    Handles SELECT (returns DataFrame), DML (INSERT/UPDATE/DELETE), and DDL
    (CREATE/DROP).
    """
    engine = get_engine(db_type)

    with engine.connect() as conn:
        if _is_select(sql_query):
            return pd.read_sql_query(text(sql_query), conn, params=params)

        with conn.begin():
            result = conn.execute(text(sql_query), params or {})
            return f"Execution successful. Rows affected: {result.rowcount}"


def run_pg_query(sql_query, params=None):
    return execute_sql(sql_query, db_type="pg", params=params)


def run_mysql_query(sql_query, params=None):
    return execute_sql(sql_query, db_type="mysql", params=params)


# ---------------------------------------------------------------------------
# Universal file upload
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
    """Only allow plain table/schema names (letters, numbers, underscores)."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(
            f"Invalid {label} '{name}'. Use only letters, numbers and "
            "underscores, and don't start with a number."
        )
    return name.lower()


def _sqlalchemy_type(series):
    """Pick a database column type from a pandas column."""
    if pd.api.types.is_bool_dtype(series):
        return Boolean()
    if pd.api.types.is_integer_dtype(series):
        return BigInteger()
    if pd.api.types.is_float_dtype(series):
        return Float(53)
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime()
    return Text()


def _create_table_with_id(engine, df, table_name, schema, dtype):
    """Create the table with an auto-numbered primary key column (if it doesn't exist).

    Some MySQL servers (many cloud ones) refuse tables without a primary key.
    """
    if inspect(engine).has_table(table_name, schema=schema):
        return

    dtype = dtype or {}
    id_col = "id"
    while id_col in df.columns:  # file already has an "id" column -> use "_id"
        id_col = "_" + id_col

    columns = [Column(id_col, BigInteger, primary_key=True, autoincrement=True)]
    for col in df.columns:
        columns.append(Column(col, dtype.get(col) or _sqlalchemy_type(df[col])))

    Table(table_name, MetaData(), *columns, schema=schema).create(engine)


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


def upload_file(
    path,
    table_name=None,
    db_type="pg",
    schema=None,
    drop_existing=False,
    chunksize=100000,
    clean_columns=True,
    read_kwargs=None,
    dtype=None,
    add_id=None,
):
    """Upload any CSV / TSV / Excel / JSON file (or a folder of them) to PostgreSQL or MySQL.

    The table is created automatically from the file's columns if it doesn't exist.

    path           A file, a folder, or a pattern like "data/*.csv".
                   Several files all go into the same table (they must have the same columns).
    table_name     Name of the table. If left out, it is taken from the file name
                   (only allowed when uploading a single file).
    db_type        "pg" for PostgreSQL (default) or "mysql" for MySQL.
    schema         PostgreSQL schema (created if missing). For MySQL, leave empty
                   to use the database in your MYSQL_URL.
    drop_existing  False (default): add rows to the table if it already exists.
                   True: DELETE the table first and reload from scratch.
    chunksize      Rows read at a time from CSV/TSV files (keeps memory low).
    clean_columns  True (default): column names become lowercase_with_underscores,
                   so you never need to quote them in queries.
    read_kwargs    Extra options for pandas, e.g. {"sheet_name": "Sales"} for Excel,
                   {"sep": ";"} or {"encoding": "latin-1"} for CSV,
                   {"parse_dates": ["order_date"]} to read dates properly.
    dtype          Optional column types, e.g. {"zip_code": sqlalchemy.types.String(10)}.
    add_id         Adds an auto-numbered "id" primary key column to a NEW table.
                   Default (None): on for MySQL (many servers require a primary key),
                   off for PostgreSQL. Set True/False to force it.

    Returns the number of rows uploaded.

    Examples:
        upload_file("sales.csv", "sales", schema="retail")
        upload_file("exports/", "orders", db_type="mysql", drop_existing=True)
        upload_file("budget.xlsx", read_kwargs={"sheet_name": "2026"})
    """
    if db_type not in ("pg", "mysql"):
        raise ValueError("db_type must be 'pg' or 'mysql'.")

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
    if schema:
        schema = _check_name(schema, "schema name")
    full_name = f"{schema}.{table_name}" if schema else table_name

    engine = get_engine(db_type)

    # 1. Prepare schema / table
    if schema and db_type == "pg":
        execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};", db_type)

    if drop_existing:
        print(f"drop_existing=True -> dropping {full_name}")
        execute_sql(f"DROP TABLE IF EXISTS {full_name};", db_type)

    print(f"Uploading {len(files)} file(s) to {full_name} ({'PostgreSQL' if db_type == 'pg' else 'MySQL'})...")

    # 2. Upload
    expected_cols = None
    total_rows = 0
    use_id = (db_type == "mysql") if add_id is None else add_id
    table_checked = False

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

            # Create the table with a primary key first (once), if requested
            if use_id and not table_checked:
                _create_table_with_id(engine, df, table_name, schema, dtype)
                table_checked = True

            # to_sql creates the table itself if it still doesn't exist
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=2000,
                dtype=dtype,
            )
            file_rows += len(df)
            total_rows += len(df)
            print(f"  -> {file_rows:,} rows uploaded")

        print(f"Finished {file_name}.")

    # 3. Verify
    res = execute_sql(f"SELECT COUNT(*) AS total FROM {full_name};", db_type)
    print(f"\nDone. {full_name} now has {int(res['total'].iloc[0]):,} rows ({total_rows:,} uploaded just now).")
    return total_rows

