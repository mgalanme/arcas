"""
ARCAS - src/arcas_api/routers/alerts.py
Alert management endpoints.
"""
import logging
from typing import Literal, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import os

router = APIRouter()
log    = logging.getLogger(__name__)

PG_DSN = (
    f"host={os.getenv('POSTGRES_HOST','localhost')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','arcas')} "
    f"user={os.getenv('POSTGRES_USER','arcas_app')} "
    f"password={os.getenv('POSTGRES_PASSWORD','')}"
)


def get_conn():
    return psycopg2.connect(PG_DSN)


@router.get("/")
async def list_alerts(
    status:   Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit:    int           = Query(50, le=200),
    offset:   int           = Query(0),
):
    """List alerts with optional filters."""
    where_clauses = []
    params        = []
    if status:
        where_clauses.append("status = %s")
        params.append(status)
    if category:
        where_clauses.append("category = %s")
        params.append(category)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM alerts {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                params + [limit, offset]
            )
            return {"alerts": cur.fetchall(), "limit": limit, "offset": offset}
    finally:
        conn.close()


@router.get("/{alert_id}")
async def get_alert(alert_id: str):
    """Get a specific alert by ID."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM alerts WHERE alert_id = %s", (alert_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Alert not found")
            return dict(row)
    finally:
        conn.close()
