
import os
import re
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import trino
from trino.exceptions import TrinoUserError
from dotenv import load_dotenv

load_dotenv()

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "api")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "nessie")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "telematics")
TIME_ZONE = os.getenv("TIME_ZONE", "America/Mexico_City")
API_TOKENS_RAW = os.getenv("API_TOKENS", "change-me")
API_TOKENS = {t.strip() for t in API_TOKENS_RAW.split(",") if t.strip()}
PORT = int(os.getenv("PORT", "9009"))

TZ_PATTERN = re.compile(r"^[A-Za-z_]+(?:/[A-Za-z_]+)+$")

app = FastAPI(title="Telematics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def require_bearer(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token not in API_TOKENS:
        raise HTTPException(status_code=401, detail="Invalid token")
    return True

def trino_conn():
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
    )
    # Fija TZ de sesión si es válido
    if TZ_PATTERN.match(TIME_ZONE):
        cur = conn.cursor()
        cur.execute(f"SET TIME ZONE '{TIME_ZONE}'")
        cur.fetchall()
    return conn

@app.get("/health")
def health():
    return {"status": "ok", "catalog": TRINO_CATALOG, "schema": TRINO_SCHEMA, "tz": TIME_ZONE}

@app.get("/latest/by-device")
def latest_by_device(
    device_ids: Optional[str] = Query(None, description="CSV de device_ids"),
    limit: int = Query(100, ge=1, le=5000),
    _: bool = Depends(require_bearer),
):
    where = []
    if device_ids:
        ids = ",".join(f"'{x.strip()}'" for x in device_ids.split(",") if x.strip())
        where.append(f"device_id IN ({ids})")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    # Convierte a hora local
    tz = TIME_ZONE if TZ_PATTERN.match(TIME_ZONE) else "UTC"
    sql = f"""
    SELECT
      device_id,
      report_type,
      latitude, longitude, gps_fixed,
      CAST(gps_epoch AT TIME ZONE '{tz}' AS timestamp) AS gps_epoch,
      CAST(received_epoch AT TIME ZONE '{tz}' AS timestamp) AS received_epoch,
      speed_kmh, heading, odometer_meters, engine_on,
      vehicle_battery_voltage, backup_battery_voltage, correlation_id
    FROM latest_gps_by_device
    {where_clause}
    ORDER BY device_id
    LIMIT {limit}
    """
    try:
        with trino_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            return rows
    except TrinoUserError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/gps/history")
def gps_history(
    device_id: str = Query(...),
    date_str: Optional[str] = Query(None, description="YYYY-MM-DD (local)"),
    limit: int = Query(1000, ge=1, le=10000),
    _: bool = Depends(require_bearer),
):
    tz = TIME_ZONE if TZ_PATTERN.match(TIME_ZONE) else "UTC"
    where = [f"device_id = '{device_id}'"]
    if date_str:
        where.append(f"CAST(gps_epoch AT TIME ZONE '{tz}' AS date) = DATE '{date_str}'")
    where_clause = " AND ".join(where)

    sql = f"""
    SELECT
      device_id, report_type, latitude, longitude, gps_fixed,
      CAST(gps_epoch AT TIME ZONE '{tz}' AS timestamp) AS gps_epoch,
      CAST(received_epoch AT TIME ZONE '{tz}' AS timestamp) AS received_epoch,
      speed_kmh, heading, odometer_meters, engine_on,
      vehicle_battery_voltage, backup_battery_voltage, correlation_id
    FROM gps_reports
    WHERE {where_clause}
    ORDER BY gps_epoch DESC
    LIMIT {limit}
    """
    try:
        with trino_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            return rows
    except TrinoUserError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/risk/score/daily")
def risk_score_daily(
    score_date: Optional[str] = Query(None, description="YYYY-MM-DD (local)"),
    limit: int = Query(1000, ge=1, le=10000),
    _: bool = Depends(require_bearer),
):
    tz = TIME_ZONE if TZ_PATTERN.match(TIME_ZONE) else "UTC"
    where = []
    if score_date:
        where.append(f"score_date = DATE '{score_date}'")
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    sql = f"""
    SELECT
      device_id, score_date, total_reports, speed_hi_reports, night_reports,
      rs, rn, fs, fn, fint, risk_raw, CAST(ROUND(score) AS integer) AS score, level,
      CAST(last_update AT TIME ZONE '{tz}' AS timestamp) AS last_update
    FROM risk_score_daily
    {where_clause}
    ORDER BY device_id
    LIMIT {limit}
    """
    try:
        with trino_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            return rows
    except TrinoUserError as e:
        raise HTTPException(status_code=400, detail=str(e))
