"""
Milestone 2: Data Model — Delhi Air Quality Dataset
=====================================================
Database Choice: SQLite (Relational / RDBMS)
Schema Style:  Star Schema (Dimensional Model)

WHY RDBMS (SQLite)?
--------------------
Our dataset has clear, well-defined relationships:
  - Stations are fixed entities (15 stations, stable attributes: name, city, state)
  - Pollutants are a fixed controlled vocabulary (13 types)
  - Every measurement row is a FACT that references a station, a pollutant, and a time

The data is:
  ✓ Structured with consistent column types (no nested / variable schema)
  ✓ Relatively narrow (22 cols) — not a wide-column use case
  ✓ Fully relational — station info repeats millions of times → normalise it out
  ✓ Needs SQL queries (GROUP BY station, pollutant, time → classic OLAP pattern)
  ✓ ~8.8M rows is manageable in SQLite for analytics; no distributed infra needed

WHY NOT NoSQL?
  ✗ Document stores (MongoDB) shine when schema varies per record — ours is fixed
  ✗ Key-Value stores do not suit multi-dimensional time-series analytics
  ✗ Wide-column (Cassandra/HBase) fit IoT with thousands of sensors & write-heavy
    workloads — we have only 15 sensors and a read-heavy analytics workload

WHY NOT flat / wide denormalised table?
  ✗ Repeating station metadata across 8.8M rows wastes space & causes update anomalies
  ✗ 13 pollutant types already stored as rows — normalise into dim_pollutant

STAR SCHEMA DESIGN
------------------
  dim_station   (station_id PK, station_name, city, state)
  dim_pollutant (pollutant_id PK AUTOINCREMENT, pollutant_name UNIQUE)
  dim_time      (time_id PK AUTOINCREMENT, dt_str UNIQUE, year, month, day, hour)
  fact_measurements (
      measurement_id PK,
      station_id  FK → dim_station,
      pollutant_id FK → dim_pollutant,
      time_id      FK → dim_time,
      value,
      at_c, rh_percent, ws_m_s, wd_deg, rf_mm, tot_rf_mm, sr_w_mt2, bp_mmhg, vws_m_s
  )

USAGE
-----
    python scripts/data_model.py

Reads partitioned Parquet files one at a time (memory-safe) and inserts into SQLite.
Set MAX_FILES = 8 for a quick January-2024-only test run.
"""

import sqlite3, pandas as pd, glob, re, logging
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "data" / "ingestion_layer" / "ingestion_layer" / "ingestion_layer" / "partitioned_data"
DB_PATH     = BASE_DIR / "data" / "database" / "air_quality.db"
LOG_PATH    = BASE_DIR / "logs" / "data_model_logs.txt"
MAX_FILES   = 8   # set to None to load all 192 partitions (~8.8M rows)

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=OFF;
CREATE TABLE IF NOT EXISTS dim_station (
    station_id TEXT PRIMARY KEY, station_name TEXT NOT NULL,
    city TEXT NOT NULL, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dim_pollutant (
    pollutant_id INTEGER PRIMARY KEY AUTOINCREMENT, pollutant_name TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY AUTOINCREMENT, dt_str TEXT NOT NULL UNIQUE,
    year INTEGER NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL, hour INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS fact_measurements (
    measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT REFERENCES dim_station(station_id),
    pollutant_id INTEGER REFERENCES dim_pollutant(pollutant_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    value REAL, at_c REAL, rh_percent REAL, ws_m_s REAL, wd_deg REAL,
    rf_mm REAL, tot_rf_mm REAL, sr_w_mt2 REAL, bp_mmhg REAL, vws_m_s REAL);
CREATE INDEX IF NOT EXISTS idx_fact_station   ON fact_measurements(station_id);
CREATE INDEX IF NOT EXISTS idx_fact_pollutant ON fact_measurements(pollutant_id);
CREATE INDEX IF NOT EXISTS idx_fact_time      ON fact_measurements(time_id);
CREATE INDEX IF NOT EXISTS idx_time_ym        ON dim_time(year, month);
"""


def get_files():
    files = sorted([f for f in glob.glob(str(PARQUET_DIR / "**" / "*.parquet"), recursive=True)
                    if not f.endswith(".crc")])
    return files[:MAX_FILES] if MAX_FILES else files


def process(conn, filepath):
    m = re.search(r"year=(\d+)/month=(\d+)", filepath)
    yr, mo = int(m.group(1)), int(m.group(2))
    df = pd.read_parquet(filepath)
    df["year"] = yr; df["month"] = mo
    df["dt_str"] = df.apply(lambda r: f"{yr:04d}-{mo:02d}-{int(r.day):02d}T{int(r.hour):02d}:00:00Z", axis=1)

    for _, r in df[["station_id","station_name","city","state"]].drop_duplicates("station_id").iterrows():
        conn.execute("INSERT OR IGNORE INTO dim_station VALUES(?,?,?,?)",
                     (r.station_id, r.station_name, r.city, r.state))
    for p in df["pollutant"].unique():
        conn.execute("INSERT OR IGNORE INTO dim_pollutant(pollutant_name) VALUES(?)", (p,))
    for _, r in df[["dt_str","year","month","day","hour"]].drop_duplicates("dt_str").iterrows():
        conn.execute("INSERT OR IGNORE INTO dim_time(dt_str,year,month,day,hour) VALUES(?,?,?,?,?)",
                     (r.dt_str, yr, mo, int(r.day), int(r.hour)))
    conn.commit()

    poll_map = {r[1]:r[0] for r in conn.execute("SELECT pollutant_id,pollutant_name FROM dim_pollutant")}
    time_map = {r[1]:r[0] for r in conn.execute("SELECT time_id,dt_str FROM dim_time")}

    def _f(v): return None if pd.isna(v) else float(v)
    rows = [(r.station_id, poll_map.get(r.pollutant), time_map.get(r.dt_str), r.value,
             _f(r.at_c), _f(r.rh_percent), _f(r.ws_m_s), _f(r.wd_deg),
             _f(r.rf_mm), _f(r.tot_rf_mm), _f(r.sr_w_mt2), _f(r.bp_mmhg), _f(r.vws_m_s))
            for _, r in df.iterrows()]
    conn.executemany("""INSERT INTO fact_measurements
        (station_id,pollutant_id,time_id,value,at_c,rh_percent,ws_m_s,wd_deg,
         rf_mm,tot_rf_mm,sr_w_mt2,bp_mmhg,vws_m_s) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    return len(rows)


def verify(conn):
    log.info("── Verification ─────────────────────────────────────────────")
    for label, q in [("dim_station","SELECT COUNT(*) FROM dim_station"),
                     ("dim_pollutant","SELECT COUNT(*) FROM dim_pollutant"),
                     ("dim_time","SELECT COUNT(*) FROM dim_time"),
                     ("fact_measurements","SELECT COUNT(*) FROM fact_measurements")]:
        log.info(f"  {label:<22}: {conn.execute(q).fetchone()[0]:>10,}")
    log.info("\nAvg PM2.5 by station:")
    for r in conn.execute("""SELECT s.station_name, ROUND(AVG(f.value),2)
        FROM fact_measurements f JOIN dim_station s ON s.station_id=f.station_id
        JOIN dim_pollutant p ON p.pollutant_id=f.pollutant_id
        WHERE p.pollutant_name='pm25' GROUP BY s.station_name ORDER BY 2 DESC""").fetchall():
        log.info(f"  {r[0]:<50} {r[1]}")


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    files = get_files()
    log.info(f"Processing {len(files)} partition files → {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(DDL); conn.commit()

    total = 0
    for i, f in enumerate(files, 1):
        n = process(conn, f)
        total += n
        log.info(f"  [{i:>3}/{len(files)}] {Path(f).parent.parent.name}/{Path(f).parent.name}  +{n:,}  (total {total:,})")

    verify(conn); conn.close()
    log.info(f"\nDB: {DB_PATH}  ({DB_PATH.stat().st_size/1e6:.1f} MB)  |  Rows: {total:,}")


if __name__ == "__main__":
    main()
