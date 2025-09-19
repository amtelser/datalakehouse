import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Depends, Header, HTTPException, Query
import trino
from trino.auth import BasicAuthentication


# =========================
#  Auth por Bearer Tokens
# =========================
def get_allowed_tokens() -> List[str]:
    raw = os.getenv("API_TOKENS", "")
    return [t.strip() for t in raw.split(",") if t.strip()]

def auth_dependency(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token not in get_allowed_tokens():
        raise HTTPException(status_code=403, detail="Invalid token")
    return token


# =========================
#  Conexión Trino
# =========================
TRINO_HOST     = os.getenv("TRINO_HOST", "trino")
TRINO_PORT     = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER     = os.getenv("TRINO_USER", "analyst")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "analyst#2025")
TRINO_CATALOG  = os.getenv("TRINO_CATALOG", "nessie")
TRINO_SCHEMA   = os.getenv("TRINO_SCHEMA", "telematics")
TRINO_VERIFY   = os.getenv("TRINO_VERIFY_SSL", "false").lower() == "true"

def trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="https",
        auth=BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
        verify=TRINO_VERIFY,   # False si usas certificado autofirmado
    )


# =========================
#  Utils
# =========================
# Zona horaria (para construir SQL seguro)
TIME_ZONE = os.getenv("TIME_ZONE", "America/Mexico_City")
TZ_SQL = TIME_ZONE.replace("'", "''")  # escape por seguridad

def rows_to_objs(cursor, rows) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cursor.description]
    out: List[Dict[str, Any]] = []
    for r in rows:
        obj: Dict[str, Any] = {}
        for k, v in zip(cols, r):
            if hasattr(v, "isoformat"):
                obj[k] = v.isoformat()
            else:
                obj[k] = v
        out.append(obj)
    return out

def pagination(limit: int = Query(100, ge=1, le=1000),
               offset: int = Query(0, ge=0)):
    return {"limit": limit, "offset": offset}


# =========================
#  FastAPI app
# =========================
app = FastAPI(
    title="Telematics API (Nessie/Trino)",
    version="1.2.0",
    description=(
        "API tipo PostgREST para exponer tablas Iceberg en Nessie vía Trino, "
        "con seguridad por Bearer token y timestamps en la zona horaria de TIME_ZONE."
    ),
)

@app.get("/health")
def health():
    return {"status": "ok", "time_zone": TIME_ZONE}


# =========================
#  Endpoints
# =========================

# 1) gps_reports
@app.get("/gps_reports", dependencies=[Depends(auth_dependency)])
def get_gps_reports(
    pagination: dict = Depends(pagination),
    device_id: Optional[str] = Query(None),
    day: Optional[str] = Query(None, description="YYYY-MM-DD (aplicado sobre gps_epoch en TIME_ZONE)"),
    from_ts: Optional[str] = Query(None, description="ISO-8601 inicio; compara contra received_epoch (con TZ absoluta)"),
    to_ts: Optional[str] = Query(None, description="ISO-8601 fin; compara contra received_epoch (con TZ absoluta)"),
    report_type: Optional[List[str]] = Query(None),
):
    """
    Listado de gps_reports.
    - Todas las columnas timestamp (gps_epoch, received_epoch, decoded_epoch) se devuelven en TIME_ZONE.
    - El filtro 'day' se aplica sobre gps_epoch convertido a TIME_ZONE.
    - Paginación: limit/offset (offset emulado con row_number()).
    """
    where = []
    params: List[Any] = []

    if device_id:
        where.append("device_id = ?")
        params.append(device_id)

    if report_type:
        placeholders = ", ".join(["?"] * len(report_type))
        where.append(f"report_type IN ({placeholders})")
        params.extend(report_type)

    if day:
        # fecha local derivada de gps_epoch en TZ
        where.append(f"CAST(CAST((gps_epoch AT TIME ZONE '{TZ_SQL}') AS timestamp) AS date) = CAST(? AS DATE)")
        params.append(day)

    if from_ts:
        # Comparación directa contra TIMESTAMP WITH TIME ZONE (absoluta)
        where.append("received_epoch >= from_iso8601_timestamp(?)")
        params.append(from_ts)

    if to_ts:
        where.append("received_epoch <= from_iso8601_timestamp(?)")
        params.append(to_ts)

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    # Expresiones de tiempo en TZ para SELECT/ORDER BY
    gps_epoch_tz     = f"CAST((gps_epoch     AT TIME ZONE '{TZ_SQL}') AS timestamp)"
    received_epoch_tz= f"CAST((received_epoch AT TIME ZONE '{TZ_SQL}') AS timestamp)"
    decoded_epoch_tz = f"CAST((decoded_epoch AT TIME ZONE '{TZ_SQL}') AS timestamp)"

    order_clause = f"ORDER BY {received_epoch_tz} DESC"

    if pagination["offset"] == 0:
        sql = f"""
        SELECT
          report_type, tenant, provider, "model", firmware, device_id, alert_type,
          CAST(latitude AS double) AS latitude, CAST(longitude AS double) AS longitude,
          gps_fixed,
          {gps_epoch_tz}      AS gps_epoch,
          satellites,
          CAST(speed_kmh AS double) AS speed_kmh,
          heading,
          CAST(odometer_meters AS bigint) AS odometer_meters,
          engine_on,
          vehicle_battery_voltage,
          backup_battery_voltage,
          {received_epoch_tz} AS received_epoch,
          {decoded_epoch_tz}  AS decoded_epoch,
          correlation_id,
          device_id_bucket, received_day, received_hour, gps_day
        FROM gps_reports
        {where_sql}
        {order_clause}
        LIMIT ?
        """
        params_q = params + [pagination["limit"]]
    else:
        # Emulación OFFSET con row_number() usando el orden por received_epoch en TZ
        sql = f"""
        WITH t AS (
          SELECT
            report_type, tenant, provider, "model", firmware, device_id, alert_type,
            CAST(latitude AS double) AS latitude, CAST(longitude AS double) AS longitude,
            gps_fixed,
            {gps_epoch_tz}      AS gps_epoch,
            satellites,
            CAST(speed_kmh AS double) AS speed_kmh,
            heading,
            CAST(odometer_meters AS bigint) AS odometer_meters,
            engine_on,
            vehicle_battery_voltage,
            backup_battery_voltage,
            {received_epoch_tz} AS received_epoch,
            {decoded_epoch_tz}  AS decoded_epoch,
            correlation_id,
            device_id_bucket, received_day, received_hour, gps_day,
            row_number() OVER ({order_clause}) AS rn
          FROM gps_reports
          {where_sql}
        )
        SELECT
          report_type, tenant, provider, "model", firmware, device_id, alert_type,
          latitude, longitude, gps_fixed, gps_epoch, satellites, speed_kmh, heading,
          odometer_meters, engine_on, vehicle_battery_voltage, backup_battery_voltage,
          received_epoch, decoded_epoch, correlation_id,
          device_id_bucket, received_day, received_hour, gps_day
        FROM t
        WHERE rn > ?
        ORDER BY rn
        LIMIT ?
        """
        params_q = params + [pagination["offset"], pagination["limit"]]

    with trino_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params_q)
        rows = cur.fetchall()
        return rows_to_objs(cur, rows)

# 2) risk_score_daily
@app.get("/risk_score_daily", dependencies=[Depends(auth_dependency)])
def get_risk_score_daily(
    pagination: dict = Depends(pagination),
    device_id: Optional[str] = Query(None),
    day: Optional[str] = Query(None, description="YYYY-MM-DD"),
    dfrom: Optional[str] = Query(None, description="YYYY-MM-DD"),
    dto: Optional[str] = Query(None, description="YYYY-MM-DD"),
):
    """
    Scores diarios por (device_id, score_date).
    - No se exponen campos rs, rn, fs, fn, fint.
    - last_update se devuelve en TIME_ZONE.
    """
    where = []
    params: List[Any] = []

    if device_id:
        where.append("device_id = ?")
        params.append(device_id)

    if day:
        where.append("score_date = CAST(? AS DATE)")
        params.append(day)

    if dfrom:
        where.append("score_date >= CAST(? AS DATE)")
        params.append(dfrom)

    if dto:
        where.append("score_date <= CAST(? AS DATE)")
        params.append(dto)

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    order_clause   = "ORDER BY score_date DESC, device_id"

    select_cols = f"""
      device_id,
      score_date,
      total_reports,
      speed_hi_reports,
      night_reports,
      risk_raw,
      score,
      level
    """

    if pagination["offset"] == 0:
        sql = f"""
        SELECT
          {select_cols}
        FROM risk_score_daily
        {where_sql}
        {order_clause}
        LIMIT ?
        """
        params_q = params + [pagination["limit"]]
    else:
        sql = f"""
        WITH t AS (
          SELECT
            {select_cols},
            row_number() OVER ({order_clause}) AS rn
          FROM risk_score_daily
          {where_sql}
        )
        SELECT
          device_id, score_date, total_reports, speed_hi_reports, night_reports,
          risk_raw, score, level, last_update
        FROM t
        WHERE rn > ?
        ORDER BY rn
        LIMIT ?
        """
        params_q = params + [pagination["offset"], pagination["limit"]]

    with trino_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params_q)
        rows = cur.fetchall()
        return rows_to_objs(cur, rows)