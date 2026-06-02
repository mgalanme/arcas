"""
ARCAS - src/arcas_api/main.py

FastAPI internal REST API.
Provides endpoints for the Streamlit dashboard to interact
with the backend services.
"""
import logging, os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from .routers import alerts, hitl

load_dotenv()
log = logging.getLogger(__name__)

OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")


def setup_telemetry():
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT)))
    trace.set_tracer_provider(provider)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_telemetry()
    log.info("ARCAS API started")
    yield
    log.info("ARCAS API shutting down")


app = FastAPI(
    title="ARCAS Internal API",
    description="Anti-Corruption & Accountability System - Internal REST API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],  # Streamlit dashboard only
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["*"],
)

app.include_router(alerts.router, prefix="/api/v1/alerts",  tags=["Alerts"])
app.include_router(hitl.router,   prefix="/api/v1/hitl",    tags=["HITL"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "arcas-api"}


@app.get("/metrics")
async def metrics():
    return {"status": "ok"}
