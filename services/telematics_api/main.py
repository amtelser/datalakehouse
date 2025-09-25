import os
import requests
from typing import List, Optional, Any, Dict
from datetime import datetime, date

import pytz
import trino
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from fastapi.openapi.utils import get_openapi
from fastapi.responses import PlainTextResponse
from trino.auth import BasicAuthentication
import yaml

# =========================
# Configuración
# =========================
TIME_ZONE = os.getenv("TIME_ZONE", "America/Mexico_City")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "analyst")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "analyst2025")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "nessie")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "telematics")
API_TOKENS = [t.strip() for t in os.getenv("API_TOKENS", "token1,token2,token3").split(",") if t.strip()]

# CORS: soporta ALLOW_ORIGINS="*" o lista separada por comas
ALLOW_ORIGINS_ENV = os.getenv("ALLOW_ORIGINS", "").strip()
ANY_ORIGIN = (ALLOW_ORIGINS_ENV == "*")
ALLOW_ORIGINS = [] if ANY_ORIGIN else [o.strip() for o in ALLOW_ORIGINS_ENV.split(",") if o.strip()]

app = FastAPI(
    title="Telematics API",
    version="1.0.0",
    description="API para exponer datos de Nessie/Iceberg vía Trino.\n\n"
                "• /telematics_real_time: device_id + rango gps_epoch (requeridos)\n"
                "• /risk_score_daily: filtrar por device_id y/o report_date\n"
                "Auth: Bearer token (ver `components.securitySchemes.bearerAuth`)",
)

# CORS middleware
if ANY_ORIGIN:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,   # necesario con "*"
        allow_methods=["*"],
        allow_headers=["*"],
        max_age=3600,
    )
elif ALLOW_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOW_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
        max_age=3600,
    )

# Seguridad Bearer
auth_scheme = HTTPBearer(auto_error=True)

def require_token(credentials: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    token = credentials.credentials
    if token not in API_TOKENS:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token

# Conexión Trino (HTTPS + BasicAuth + Skip TLS Verify)
def trino_conn():
    session = requests.Session()
    session.verify = False  # ⚠️ Skip TLS verify (dev con cert autofirmado)

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        auth=BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="https",
        http_session=session,
    )
    cur = conn.cursor()
    # La sesión de Trino interpreta/retorna timestamps en esta zona
    cur.execute(f"SET TIME ZONE '{TIME_ZONE}'")
    return conn

# =========================
# Helpers (TZ local y formato exacto)
# =========================
LOCAL_TZ = pytz.timezone(TIME_ZONE)
TIMESTAMP_FIELDS = {"gps_epoch", "received_epoch", "decoded_epoch"}

def to_local_naive(dt: datetime) -> datetime:
    """Convierte un datetime con tz a la TZ local y lo deja naive (sin tzinfo).
       Si ya es naive, se asume que está en hora local."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(LOCAL_TZ).replace(tzinfo=None)

def parse_local_dt_str(s: Optional[str]) -> Optional[str]:
    """
    Devuelve 'YYYY-MM-DD HH:MM:SS' en TIME_ZONE (sin offset).
    Acepta ISO con o sin zona. Si solo hay fecha, asume 00:00:00.
    """
    if not s:
        return None
    txt = s.strip()
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    dt: Optional[datetime] = None
    try:
        dt = datetime.fromisoformat(txt)
    except Exception:
        try:
            d = date.fromisoformat(txt)
            dt = datetime(d.year, d.month, d.day, 0, 0, 0)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid datetime format: {s}")
    dt_local_naive = to_local_naive(dt)
    return dt_local_naive.strftime("%Y-%m-%d %H:%M:%S")

def format_local_offset(dt_value: Any) -> Optional[str]:
    """
    Formatea a 'YYYY-MM-DD HH:MM:SS.mmm -0600' en TIME_ZONE.
    - Acepta datetime (con o sin tz) o string ISO.
    - Si es None, retorna None.
    """
    if dt_value is None:
        return None

    # Convertir a datetime
    if isinstance(dt_value, datetime):
        dt = dt_value
    elif isinstance(dt_value, str):
        txt = dt_value.strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(txt)
        except Exception:
            try:
                dt = datetime.strptime(txt, "%Y-%m-%d %H:%M:%S.%f")
            except Exception:
                try:
                    dt = datetime.strptime(txt, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    return dt_value
    else:
        return str(dt_value)

    # A TZ local con offset y milisegundos
    if dt.tzinfo is None:
        dt_local = LOCAL_TZ.localize(dt)
    else:
        dt_local = dt.astimezone(LOCAL_TZ)

    msec = dt_local.microsecond // 1000
    return f"{dt_local.strftime('%Y-%m-%d %H:%M:%S')}.{msec:03d} {dt_local.strftime('%z')}"

class PageMeta(BaseModel):
    limit: int = 100
    offset: int = 0
    total: Optional[int] = None

    @validator("limit")
    def v_limit(cls, v):
        if v < 1 or v > 10000:  # ⬆ límite máx 10k
            raise ValueError("limit must be 1..10000")
        return v

    @validator("offset")
    def v_offset(cls, v):
        if v < 0:
            raise ValueError("offset must be >= 0")
        return v

def pagination_clause(offset: int, limit: int):
    """Paginación robusta (evita edge-case con offset=0)."""
    if offset == 0:
        return "FETCH NEXT ? ROWS ONLY", [limit]
    else:
        return "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", [offset, limit]

def postprocess_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Formatea campos de tiempo a 'YYYY-MM-DD HH:MM:SS.mmm -0600'."""
    out = {}
    for k, v in row.items():
        if k in TIMESTAMP_FIELDS and v is not None:
            out[k] = format_local_offset(v)
        else:
            out[k] = v
    return out

# =========================
# OpenAPI personalizado (bearer global)
# =========================
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {})
    openapi_schema["components"]["securitySchemes"]["bearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
    }
    openapi_schema["security"] = [{"bearerAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

@app.get("/openapi.yaml", response_class=PlainTextResponse, include_in_schema=False)
def openapi_yaml():
    return yaml.safe_dump(app.openapi(), sort_keys=False, allow_unicode=True)

# =========================
# Endpoints
# =========================
@app.get("/health", tags=["system"], include_in_schema=True)
def health():
    try:
        conn = trino_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"trino error: {e}")

@app.get("/telematics_real_time", tags=["telematics"])
def telematics_real_time(
    token: str = Depends(require_token),
    device_id: str = Query(..., description="Exact device_id (requerido)"),
    gps_epoch_start: str = Query(..., description="Inicio local, ej. 2025-09-25T00:00:00"),
    gps_epoch_end: str = Query(..., description="Fin local (inclusive)"),
    limit: int = Query(100, ge=1, le=10000),   # ⬆ 10k
    offset: int = Query(0, ge=0),
    columns: Optional[str] = Query(None, description="Proyección separada por comas"),
):
    # Parseo a string local sin tz
    start_ts_local = parse_local_dt_str(gps_epoch_start)  # 'YYYY-MM-DD HH:MM:SS'
    end_ts_local   = parse_local_dt_str(gps_epoch_end)    # 'YYYY-MM-DD HH:MM:SS'
    if start_ts_local > end_ts_local:
        raise HTTPException(status_code=400, detail="gps_epoch_start must be <= gps_epoch_end")

    # Días locales para pruning
    day_start_local = start_ts_local.split(" ")[0]
    day_end_local   = end_ts_local.split(" ")[0]

    base_cols = [
        "report_type","tenant","provider","model","firmware","device_id",
        "alert_type","latitude","longitude","gps_fixed","gps_epoch",
        "satellites","speed_kmh","heading","odometer_meters","engine_on",
        "vehicle_battery_voltage","backup_battery_voltage",
        "received_epoch","decoded_epoch","correlation_id"
    ]
    proj_cols = [c.strip() for c in (columns.split(",") if columns else base_cols) if c.strip()]
    sel = ", ".join(proj_cols)

    # Casts para tipos correctos
    where = [
        "device_id = ?",
        f"gps_epoch BETWEEN with_timezone(CAST(? AS timestamp), '{TIME_ZONE}') AND with_timezone(CAST(? AS timestamp), '{TIME_ZONE}')",
        "received_day BETWEEN CAST(? AS date) AND CAST(? AS date)",
    ]
    params: List[Any] = [device_id, start_ts_local, end_ts_local, day_start_local, day_end_local]

    where_sql = "WHERE " + " AND ".join(where)

    count_sql = f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.telematics_real_time {where_sql}"

    # Paginación robusta
    pag_sql, pag_params = pagination_clause(offset, limit)

    data_sql = f"""
        SELECT {sel}
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.telematics_real_time
        {where_sql}
        ORDER BY device_id, gps_epoch DESC
        {pag_sql}
    """

    try:
        conn = trino_conn()
        cur = conn.cursor()

        cur.execute(count_sql, params)
        total = cur.fetchone()[0]

        cur.execute(data_sql, [*params, *pag_params])
        rows = cur.fetchall()
        raw = [dict(zip(proj_cols, r)) for r in rows]
        # Formateo final de timestamps como en la BD: "YYYY-MM-DD HH:MM:SS.mmm -0600"
        result = [postprocess_row(item) for item in raw]
        return {"items": result, "page": {"limit": limit, "offset": offset, "total": total}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"trino query error: {e}")


@app.get("/risk_score_daily", tags=["risk"])
def risk_score_daily(
    token: str = Depends(require_token),
    device_id: Optional[str] = Query(None),
    report_date_start: Optional[date] = Query(None),
    report_date_end: Optional[date] = Query(None),
    limit: int = Query(100, ge=1, le=10000),   # ⬆ 10k
    offset: int = Query(0, ge=0),
    columns: Optional[str] = Query(None),
):
    if not device_id and not (report_date_start or report_date_end):
        raise HTTPException(status_code=400, detail="Provide device_id or report_date range")

    if report_date_start and report_date_end and report_date_start > report_date_end:
        raise HTTPException(status_code=400, detail="report_date_start must be <= report_date_end")

    base_cols = ["device_id", "report_date", "score", "level", "total_reports", "overspeed_reports", "night_reports"]
    proj_cols = [c.strip() for c in (columns.split(",") if columns else base_cols) if c.strip()]
    sel = ", ".join(proj_cols)

    where = []
    params: List[Any] = []

    if device_id:
        where.append("device_id = ?")
        params.append(device_id)
    if report_date_start and report_date_end:
        where.append("report_date BETWEEN CAST(? AS date) AND CAST(? AS date)")
        params.extend([report_date_start.isoformat(), report_date_end.isoformat()])
    elif report_date_start:
        where.append("report_date >= CAST(? AS date)")
        params.append(report_date_start.isoformat())
    elif report_date_end:
        where.append("report_date <= CAST(? AS date)")
        params.append(report_date_end.isoformat())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.risk_score_daily {where_sql}"

    pag_sql, pag_params = pagination_clause(offset, limit)

    data_sql = f"""
        SELECT {sel}
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.risk_score_daily
        {where_sql}
        ORDER BY device_id, report_date DESC
        {pag_sql}
    """

    try:
        conn = trino_conn()
        cur = conn.cursor()

        cur.execute(count_sql, params)
        total = cur.fetchone()[0]

        cur.execute(data_sql, [*params, *pag_params])
        rows = cur.fetchall()
        raw = [dict(zip(proj_cols, r)) for r in rows]
        result = raw
        return {"items": result, "page": {"limit": limit, "offset": offset, "total": total}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"trino query error: {e}")