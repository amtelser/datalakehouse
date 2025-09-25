import os
import requests
from typing import List, Optional, Any
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
                "• /telematics_real_time: filtrar por device_id y/o gps_epoch\n"
                "• /risk_score_daily: filtrar por device_id y/o report_date\n"
                "Auth: Bearer token (ver `components.securitySchemes.bearerAuth`)",
)

# CORS middleware (maneja wildcard *)
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
    session.verify = False  # ⚠️ Skip TLS verify (útil en dev con cert autofirmado)

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        auth=BasicAuthentication(TRINO_USER, TRINO_PASSWORD),
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="https",
        http_session=session,   # pasa la sesión con verify=False
    )
    cur = conn.cursor()
    cur.execute(f"SET TIME ZONE '{TIME_ZONE}'")
    return conn

# =========================
# Helpers
# =========================
MX_TZ = pytz.timezone(TIME_ZONE)

def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # ISO 8601; si viene naive, interpreta en TZ local y convierte a UTC
    if "Z" in s or "+" in s:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = MX_TZ.localize(dt)
    return dt.astimezone(pytz.UTC)

def to_date_utc_floor(dts: Optional[datetime]) -> Optional[date]:
    return dts.date() if dts else None

class PageMeta(BaseModel):
    limit: int = 100
    offset: int = 0
    total: Optional[int] = None

    @validator("limit")
    def v_limit(cls, v):
        if v < 1 or v > 1000:
            raise ValueError("limit must be 1..1000")
        return v

    @validator("offset")
    def v_offset(cls, v):
        if v < 0:
            raise ValueError("offset must be >= 0")
        return v

def pagination_clause(offset: int, limit: int):
    """
    Devuelve el fragmento SQL de paginación y el orden correcto de parámetros.
    - offset == 0 -> solo FETCH (evita edge-cases y asegura row-count positivo)
    - offset > 0  -> OFFSET ... FETCH ...
    """
    if offset == 0:
        return "FETCH NEXT ? ROWS ONLY", [limit]
    else:
        return "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", [offset, limit]

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
    # Security scheme
    openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {})
    openapi_schema["components"]["securitySchemes"]["bearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",  # o "opaque" si usas tokens opacos
    }
    # Seguridad global: todas las rutas requieren bearer (las públicas se pueden excluir manualmente)
    openapi_schema["security"] = [{"bearerAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

@app.get("/openapi.yaml", response_class=PlainTextResponse, include_in_schema=False)
def openapi_yaml():
    """Descarga el OpenAPI en YAML."""
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
    device_id: Optional[str] = Query(None, description="Exact device_id"),
    gps_epoch_start: Optional[str] = Query(None, description="ISO-8601 start, e.g. 2025-09-01T00:00:00"),
    gps_epoch_end: Optional[str] = Query(None, description="ISO-8601 end (inclusive)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    columns: Optional[str] = Query(None, description="Comma-separated projection"),
):
    if not device_id and not (gps_epoch_start or gps_epoch_end):
        raise HTTPException(status_code=400, detail="Provide device_id or gps_epoch range")

    dt_start = parse_dt(gps_epoch_start)
    dt_end = parse_dt(gps_epoch_end)
    if dt_start and dt_end and dt_start > dt_end:
        raise HTTPException(status_code=400, detail="gps_epoch_start must be <= gps_epoch_end")

    base_cols = [
        "report_type","tenant","provider","model","firmware","device_id",
        "alert_type","latitude","longitude","gps_fixed","gps_epoch",
        "satellites","speed_kmh","heading","odometer_meters","engine_on",
        "vehicle_battery_voltage","backup_battery_voltage",
        "received_epoch","decoded_epoch","correlation_id"
    ]
    proj_cols = [c.strip() for c in (columns.split(",") if columns else base_cols) if c.strip()]
    sel = ", ".join(proj_cols)

    where = []
    params: List[Any] = []

    if device_id:
        where.append("device_id = ?")
        params.append(device_id)

    if dt_start and dt_end:
        where.append("gps_epoch BETWEEN ? AND ?")
        params.extend([dt_start.isoformat(), dt_end.isoformat()])
        where.append("received_day BETWEEN DATE(?) AND DATE(?)")
        params.extend([dt_start.isoformat(), dt_end.isoformat()])
    elif dt_start:
        where.append("gps_epoch >= ?")
        params.append(dt_start.isoformat())
        where.append("received_day >= DATE(?)")
        params.append(dt_start.isoformat())
    elif dt_end:
        where.append("gps_epoch <= ?")
        params.append(dt_end.isoformat())
        where.append("received_day <= DATE(?)")
        params.append(dt_end.isoformat())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.telematics_real_time {where_sql}"

    # Paginación robusta
    pag_sql, pag_params = pagination_clause(offset, limit)

    data_sql = f"""
        SELECT {sel}
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.telematics_real_time
        {where_sql}
        ORDER BY device_id, gps_epoch
        {pag_sql}
    """

    try:
        conn = trino_conn()
        cur = conn.cursor()

        cur.execute(count_sql, params)
        total = cur.fetchone()[0]

        cur.execute(data_sql, [*params, *pag_params])  # orden correcto de params
        rows = cur.fetchall()
        result = [dict(zip(proj_cols, r)) for r in rows]
        return {"items": result, "page": {"limit": limit, "offset": offset, "total": total}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"trino query error: {e}")

@app.get("/risk_score_daily", tags=["risk"])
def risk_score_daily(
    token: str = Depends(require_token),
    device_id: Optional[str] = Query(None),
    report_date_start: Optional[date] = Query(None),
    report_date_end: Optional[date] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
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
        where.append("report_date BETWEEN ? AND ?")
        params.extend([report_date_start.isoformat(), report_date_end.isoformat()])
    elif report_date_start:
        where.append("report_date >= ?")
        params.append(report_date_start.isoformat())
    elif report_date_end:
        where.append("report_date <= ?")
        params.append(report_date_end.isoformat())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.risk_score_daily {where_sql}"

    # Paginación robusta
    pag_sql, pag_params = pagination_clause(offset, limit)

    data_sql = f"""
        SELECT {sel}
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.risk_score_daily
        {where_sql}
        ORDER BY device_id, report_date
        {pag_sql}
    """

    try:
        conn = trino_conn()
        cur = conn.cursor()

        cur.execute(count_sql, params)
        total = cur.fetchone()[0]

        cur.execute(data_sql, [*params, *pag_params])  # orden correcto de params
        rows = cur.fetchall()
        result = [dict(zip(proj_cols, r)) for r in rows]
        return {"items": result, "page": {"limit": limit, "offset": offset, "total": total}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"trino query error: {e}")