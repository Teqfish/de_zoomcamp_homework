import duckdb
import requests
from pathlib import Path
from typing import List, Tuple

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
RELEASE = "fhv"  # folder name under releases/download


def month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    """Yield (year, month) tuples from start (inclusive) to end (inclusive)."""
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def download_file(url: str, dest: Path, chunk_size: int = 8192) -> None:
    """Download a URL to a destination file."""
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)


def convert_csv_gz_to_parquet(csv_gz_path: Path, parquet_path: Path) -> None:
    """
    Convert a gzipped CSV to Parquet using DuckDB.
    Uses tolerant CSV parsing to handle non-UTF8 bytes / malformed rows.
    If conversion fails, deletes any partial Parquet file.
    """
    try:
        con = duckdb.connect()
        con.execute(
            f"""
            COPY (
              SELECT * FROM read_csv_auto(
                '{csv_gz_path}',
                strict_mode=false,
                ignore_errors=true,
                encoding='latin-1'
              )
            )
            TO '{parquet_path}' (FORMAT PARQUET);
            """
        )
        con.close()
    except Exception:
        # If a partial parquet file was created, remove it so future runs don't "skip" it.
        if parquet_path.exists():
            try:
                parquet_path.unlink()
            except Exception:
                pass
        raise


def download_and_convert_fhv_files(
    start_year: int = 2019,
    start_month: int = 1,
    end_year: int = 2021,
    end_month: int = 7,
) -> Tuple[List[Path], List[Tuple[str, str]]]:
    """
    Download NYC TLC FHV tripdata CSV.gz files and convert them to Parquet.
    Continues month-by-month even if some months fail.
    Returns (successful_parquets, failures[(filename, error)]).
    """
    data_dir = Path("data") / "fhv"
    data_dir.mkdir(exist_ok=True, parents=True)

    successes: List[Path] = []
    failures: List[Tuple[str, str]] = []

    for year, month in month_range(start_year, start_month, end_year, end_month):
        csv_gz_filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
        csv_gz_filepath = data_dir / csv_gz_filename

        parquet_filename = f"fhv_tripdata_{year}-{month:02d}.parquet"
        parquet_filepath = data_dir / parquet_filename

        # Skip if already converted AND looks non-empty
        if parquet_filepath.exists() and parquet_filepath.stat().st_size > 1024:
            print(f"Skipping {parquet_filename} (already exists)")
            successes.append(parquet_filepath)
            continue

        # If a broken tiny parquet exists from a previous failed run, remove it
        if parquet_filepath.exists() and parquet_filepath.stat().st_size <= 1024:
            print(f"[WARN] Removing tiny/broken parquet: {parquet_filename}")
            try:
                parquet_filepath.unlink()
            except Exception:
                pass

        url = f"{BASE_URL}/{RELEASE}/{csv_gz_filename}"
        print(f"Downloading {url}")

        try:
            download_file(url, csv_gz_filepath)
        except Exception as e:
            msg = f"download failed: {e}"
            print(f"[WARN] {csv_gz_filename}: {msg}")
            failures.append((csv_gz_filename, msg))
            # If a partial download exists, remove it
            if csv_gz_filepath.exists():
                try:
                    csv_gz_filepath.unlink()
                except Exception:
                    pass
            continue

        print(f"Converting {csv_gz_filename} to Parquet...")
        try:
            convert_csv_gz_to_parquet(csv_gz_filepath, parquet_filepath)
            successes.append(parquet_filepath)
            print(f"Completed {parquet_filename}")
        except Exception as e:
            msg = f"convert failed: {e}"
            print(f"[WARN] {csv_gz_filename}: {msg}")
            failures.append((csv_gz_filename, msg))
        finally:
            # Remove the CSV.gz to save space (even on conversion failure)
            if csv_gz_filepath.exists():
                try:
                    csv_gz_filepath.unlink()
                except Exception:
                    pass

    return successes, failures


def update_gitignore():
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""
    if "data/" not in content:
        with open(gitignore_path, "a") as f:
            f.write("\n# Data directory\ndata/\n" if content else "# Data directory\ndata/\n")


def load_parquets_into_duckdb(db_path: str = "taxi_rides_ny.duckdb") -> None:
    """
    Load all valid FHV Parquets into DuckDB as prod.fhv_tripdata.
    Skips tiny/broken parquet files.
    """
    fhv_dir = Path("data") / "fhv"
    parquet_files = sorted(fhv_dir.glob("*.parquet"))

    # Heuristic: skip tiny files that are likely partial/broken
    good_files = [p for p in parquet_files if p.stat().st_size > 1024]

    print(f"Found {len(parquet_files)} parquet files; loading {len(good_files)} (skipping tiny/broken).")

    if not good_files:
        raise RuntimeError("No valid parquet files found to load. Conversion likely failed for all months.")

    file_list_sql = ", ".join([f"'{str(p)}'" for p in good_files])

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT * FROM read_parquet([{file_list_sql}], union_by_name=true);
        """
    )
    con.close()
    print("✅ Loaded prod.fhv_tripdata successfully.")


if __name__ == "__main__":
    update_gitignore()

    successes, failures = download_and_convert_fhv_files()

    print(f"\n=== Summary ===")
    print(f"Parquets created/available: {len(successes)}")
    print(f"Failures: {len(failures)}")
    if failures:
        for fname, err in failures[:10]:
            print(f" - {fname}: {err}")
        if len(failures) > 10:
            print(f" ... and {len(failures) - 10} more")

    # Load whatever valid Parquets exist into DuckDB
    load_parquets_into_duckdb("taxi_rides_ny.duckdb")
