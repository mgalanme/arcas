"""
ARCAS - scripts/maintenance/backup.py

Backs up PostgreSQL and Neo4j to MinIO (arcas-backups bucket).
Backups are timestamped and compressed.
MinIO lifecycle policy auto-deletes backups older than 30 days.

Run with: PYTHONPATH=. python scripts/maintenance/backup.py
"""

import os
import sys
import subprocess
import gzip
import shutil
import tempfile
import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

TIMESTAMP   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
BUCKET      = "arcas-backups"

# PostgreSQL config
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "arcas")
PG_USER     = os.getenv("POSTGRES_USER", "arcas_app")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# MinIO config
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Neo4j config (dump via docker exec)
NEO4J_CONTAINER  = "arcas-neo4j"
NEO4J_PASSWORD   = os.getenv("NEO4J_PASSWORD", "")


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def backup_postgres(tmp_dir: Path) -> Path | None:
    """Dump PostgreSQL to a compressed SQL file."""
    log.info("Backing up PostgreSQL...")
    dump_file  = tmp_dir / f"postgres_{PG_DB}_{TIMESTAMP}.sql"
    gzip_file  = Path(str(dump_file) + ".gz")

    env = os.environ.copy()
    env["PGPASSWORD"] = PG_PASSWORD

    result = subprocess.run(
        [
            "pg_dump",
            "-h", PG_HOST,
            "-p", PG_PORT,
            "-U", PG_USER,
            "-d", PG_DB,
            "--format=plain",
            "--no-password",
            "-f", str(dump_file),
        ],
        env=env,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        log.error(f"pg_dump failed: {result.stderr}")
        return None

    # Compress
    with open(dump_file, "rb") as f_in, gzip.open(gzip_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    dump_file.unlink()

    size_mb = gzip_file.stat().st_size / (1024 * 1024)
    log.info(f"  PostgreSQL dump: {gzip_file.name} ({size_mb:.1f} MB)")
    return gzip_file


def backup_neo4j(tmp_dir: Path) -> Path | None:
    """
    Dump Neo4j using neo4j-admin backup via docker exec.
    Community Edition supports online backup via neo4j-admin dump.
    """
    log.info("Backing up Neo4j...")
    dump_dir  = tmp_dir / f"neo4j_{TIMESTAMP}"
    dump_dir.mkdir()
    archive   = tmp_dir / f"neo4j_{TIMESTAMP}.tar.gz"

    # neo4j-admin dump requires the database to be stopped in Community Edition.
    # We use cypher-shell to export a Cypher script instead (online, no downtime).
    export_file = tmp_dir / f"neo4j_export_{TIMESTAMP}.cypher"

    result = subprocess.run(
        [
            "docker", "exec", NEO4J_CONTAINER,
            "cypher-shell",
            "-u", "neo4j",
            "-p", NEO4J_PASSWORD,
            "--format", "plain",
            "CALL apoc.export.cypher.all('/tmp/arcas_export.cypher', {format: 'cypher-shell'})"
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )

    if result.returncode != 0:
        log.warning(f"Neo4j cypher export returned non-zero: {result.stderr}")
        # Non-fatal: APOC may not support this in all versions
        # Fall back to node count verification
        log.info("  Falling back to statistics-only Neo4j backup check...")
        with open(export_file, "w") as f:
            f.write(f"# Neo4j backup check - {TIMESTAMP}\n")
            f.write(f"# Note: Full Cypher export requires APOC apoc.export.cypher.all\n")
        return None

    # Copy the export file from container
    subprocess.run(
        ["docker", "cp", f"{NEO4J_CONTAINER}:/tmp/arcas_export.cypher", str(export_file)],
        check=True,
    )

    # Compress
    with open(export_file, "rb") as f_in, gzip.open(archive, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    export_file.unlink()

    size_mb = archive.stat().st_size / (1024 * 1024)
    log.info(f"  Neo4j export: {archive.name} ({size_mb:.1f} MB)")
    return archive


def upload_to_minio(client: Minio, local_path: Path, object_prefix: str) -> bool:
    """Upload a file to MinIO."""
    object_name = f"{object_prefix}/{local_path.name}"
    try:
        client.fput_object(
            BUCKET,
            object_name,
            str(local_path),
        )
        log.info(f"  Uploaded: s3://{BUCKET}/{object_name}")
        return True
    except S3Error as e:
        log.error(f"MinIO upload failed: {e}")
        return False


def main() -> None:
    log.info("=" * 60)
    log.info(f"ARCAS - Backup started at {TIMESTAMP}")
    log.info("=" * 60)

    client = get_minio_client()

    # Verify bucket exists
    if not client.bucket_exists(BUCKET):
        log.error(f"Backup bucket '{BUCKET}' does not exist. Run init_minio_buckets.sh first.")
        sys.exit(1)

    results = {}

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # PostgreSQL backup
        pg_file = backup_postgres(tmp_path)
        if pg_file:
            results["postgres"] = upload_to_minio(client, pg_file, f"postgres/{TIMESTAMP[:8]}")
        else:
            results["postgres"] = False

        # Neo4j backup
        neo4j_file = backup_neo4j(tmp_path)
        if neo4j_file:
            results["neo4j"] = upload_to_minio(client, neo4j_file, f"neo4j/{TIMESTAMP[:8]}")
        else:
            results["neo4j"] = False
            log.warning("  Neo4j backup skipped (see above)")

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("Backup Summary:")
    for service, success in results.items():
        status = "OK" if success else "FAILED/SKIPPED"
        log.info(f"  {service}: {status}")

    # List recent backups
    log.info("")
    log.info("Recent backups in MinIO:")
    objects = client.list_objects(BUCKET, recursive=True)
    backup_list = sorted(
        [(obj.object_name, obj.size) for obj in objects],
        key=lambda x: x[0],
        reverse=True
    )[:10]
    for name, size in backup_list:
        log.info(f"  {name} ({size / 1024:.0f} KB)")

    log.info("=" * 60)

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
