"""
ARCAS - scripts/implementation/run_ingestion.py

Runs the full ingestion pipeline: BOE + scraper (media + fact-checkers).

Usage:
  PYTHONPATH=. python scripts/implementation/run_ingestion.py
  PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources boe
  PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources scraper
  PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources scraper --mode media
  PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources scraper --mode factcheck
  PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources boe,scraper --days-back 3
"""
import argparse, logging, os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def run_boe(days_back: int = 1) -> int:
    from src.arcas_ingest.connectors.gazette.boe_connector import BOEConnector
    connector = BOEConnector()
    try:
        end   = date.today()
        start = end - timedelta(days=days_back - 1)
        count = connector.fetch_range(start, end)
        log.info(f"BOE: {count} records produced")
        return count
    finally:
        connector.close()


def run_scraper(mode: str = "all") -> int:
    from src.arcas_ingest.scraper.scraper_engine import ScraperEngine
    engine = ScraperEngine()
    try:
        if mode == "media":
            results = engine.scrape_media_only()
        elif mode == "factcheck":
            results = engine.scrape_factcheckers_only()
        else:
            results = engine.scrape_all()
            if isinstance(results, dict):
                log.info(f"Scraper: {results['media']} media + {results['factcheck']} fact-check records")
                return results["total"]
        log.info(f"Scraper ({mode}): {results} records produced")
        return results if isinstance(results, int) else results.get("total", 0)
    finally:
        engine.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARCAS ingestion pipeline runner")
    parser.add_argument(
        "--sources", default="boe,scraper",
        help="Comma-separated: boe, scraper (default: boe,scraper)"
    )
    parser.add_argument(
        "--mode", choices=["all", "media", "factcheck"], default="all",
        help="Scraper mode: all (default), media only, factcheck only"
    )
    parser.add_argument(
        "--days-back", type=int, default=1,
        help="Days back from today for BOE (default: 1)"
    )
    args    = parser.parse_args()
    sources = [s.strip() for s in args.sources.split(",")]

    total = 0
    if "boe"     in sources: total += run_boe(args.days_back)
    if "scraper" in sources: total += run_scraper(args.mode)

    log.info(f"Ingestion complete. Total records produced: {total}")
