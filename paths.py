from pathlib import Path

ROOT = Path(__file__).resolve().parent

DATA = ROOT / "data"
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"

CURRENCY_SUMMARY = GOLD / "currency_summary"
STATUS_SUMMARY = GOLD / "status_summary"
DAILY_SUMMARY = GOLD / "daily_summary"

MERCHANT_DIM = DATA / "merchant_dim.json"
