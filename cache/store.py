"""
cache/store.py
──────────────
SQLite-backed cache. Fresh-start version — no JSON migration code.

Persisted in .cache/sync.db:
    video_pages     video_id  → Notion page_id
    video_etags     video_id  → YouTube item-level etag
    video_last_sync video_id  → ISO datetime of last successful write
    video_props     video_id  → JSON blob of raw video data dict
    channel_yt      channel_id → JSON blob {Custom URL, Logo URL}
    categories      category_id → name / NULL

In-memory only (re-populated each run):
    _channel_notion     channel_id → Notion page_id / None
    _notion_last_edited video_id   → Notion last_edited_time
    _existing_video_ids set of all video_ids known to Notion

_notion_last_edited is NOT persisted — it must reflect current Notion
state, not what it was on a previous run.

Thread / async safety
──────────────────────
All DB writes go through _db_write() which holds a threading.Lock.
Reads use a separate connection with WAL mode so reads never block writes.
"""

import json
import logging
import sqlite3
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH   = CACHE_DIR / "sync.db"

# ── DB connections ─────────────────────────────────────────────────────────────
_write_lock = threading.Lock()
_wcon: sqlite3.Connection | None = None
_rcon: sqlite3.Connection | None = None


def _get_write_con() -> sqlite3.Connection:
    global _wcon
    if _wcon is None:
        _wcon = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _wcon.execute("PRAGMA journal_mode=WAL")
        _wcon.execute("PRAGMA synchronous=NORMAL")
        _wcon.execute("PRAGMA busy_timeout=10000")
    return _wcon


def _get_read_con() -> sqlite3.Connection:
    global _rcon
    if _rcon is None:
        _rcon = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _rcon.execute("PRAGMA journal_mode=WAL")
        _rcon.execute("PRAGMA query_only=ON")
    return _rcon


def _db_write(sql: str, params=()):
    with _write_lock:
        con = _get_write_con()
        con.execute(sql, params)
        con.commit()


def _db_write_many(sql: str, param_list: list):
    if not param_list:
        return
    with _write_lock:
        con = _get_write_con()
        con.executemany(sql, param_list)
        con.commit()


def _db_read_one(sql: str, params=()) -> tuple | None:
    con = _get_read_con()
    return con.execute(sql, params).fetchone()


def _db_read_all(sql: str, params=()) -> list[tuple]:
    con = _get_read_con()
    return con.execute(sql, params).fetchall()


# ── Schema ─────────────────────────────────────────────────────────────────────

def _init_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS video_pages (
        video_id TEXT PRIMARY KEY,
        page_id  TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS video_etags (
        video_id TEXT PRIMARY KEY,
        etag     TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS video_last_sync (
        video_id     TEXT PRIMARY KEY,
        last_sync_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS video_props (
        video_id TEXT PRIMARY KEY,
        props    TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS channel_yt (
        channel_id TEXT PRIMARY KEY,
        data       TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS categories (
        category_id TEXT PRIMARY KEY,
        name        TEXT
    );
    """
    with _write_lock:
        con = _get_write_con()
        con.executescript(ddl)
        con.commit()
    logger.info(f"[Cache] SQLite schema ready at {DB_PATH}")


# ── In-memory stores ───────────────────────────────────────────────────────────
_channel_notion: dict[str, str | None] = {}
_notion_last_edited: dict[str, str]    = {}
_existing_video_ids: set[str] | None   = None


# ── YouTube channel cache ──────────────────────────────────────────────────────

def get_yt_channel(channel_id: str) -> dict | None:
    row = _db_read_one("SELECT data FROM channel_yt WHERE channel_id=?", (channel_id,))
    return json.loads(row[0]) if row else None


def set_yt_channel(channel_id: str, data: dict):
    _db_write(
        "INSERT OR REPLACE INTO channel_yt(channel_id, data) VALUES(?,?)",
        (channel_id, json.dumps(data)),
    )


# ── YouTube category cache ─────────────────────────────────────────────────────

def get_category(category_id: str) -> str | None | bool:
    """Returns False if not cached, None if cached-but-missing, str if found."""
    row = _db_read_one("SELECT name FROM categories WHERE category_id=?", (category_id,))
    if row is None:
        return False
    return row[0]


def set_category(category_id: str, name: str | None):
    _db_write(
        "INSERT OR REPLACE INTO categories(category_id, name) VALUES(?,?)",
        (category_id, name),
    )


# ── Notion channel cache (in-memory) ──────────────────────────────────────────

def get_notion_channel(channel_id: str) -> str | None | bool:
    """Returns False if not cached, None if not in Notion, str page_id if found."""
    if channel_id not in _channel_notion:
        return False
    return _channel_notion[channel_id]


def set_notion_channel(channel_id: str, page_id: str | None):
    _channel_notion[channel_id] = page_id


# ── video_id → page_id ────────────────────────────────────────────────────────

def get_video_page_id(video_id: str) -> str | None:
    row = _db_read_one("SELECT page_id FROM video_pages WHERE video_id=?", (video_id,))
    return row[0] if row else None


def set_video_page_id(video_id: str, page_id: str):
    _db_write(
        "INSERT OR REPLACE INTO video_pages(video_id, page_id) VALUES(?,?)",
        (video_id, page_id),
    )
    if _existing_video_ids is not None:
        _existing_video_ids.add(video_id)


def bulk_set_video_page_ids(mapping: dict[str, str]):
    """
    Batch-upsert {video_id: page_id} in one transaction.
    Called per Notion page during prefetch (100 videos at a time).
    """
    if not mapping:
        return
    _db_write_many(
        "INSERT OR IGNORE INTO video_pages(video_id, page_id) VALUES(?,?)",
        list(mapping.items()),
    )
    if _existing_video_ids is not None:
        _existing_video_ids.update(mapping.keys())


# ── ETag cache ─────────────────────────────────────────────────────────────────

def get_video_etag(video_id: str) -> str | None:
    row = _db_read_one("SELECT etag FROM video_etags WHERE video_id=?", (video_id,))
    return row[0] if row else None


def set_video_etag(video_id: str, etag: str):
    _db_write(
        "INSERT OR REPLACE INTO video_etags(video_id, etag) VALUES(?,?)",
        (video_id, etag),
    )


# ── Last sync time ─────────────────────────────────────────────────────────────

def get_last_sync_time(video_id: str) -> str | None:
    row = _db_read_one(
        "SELECT last_sync_at FROM video_last_sync WHERE video_id=?", (video_id,)
    )
    return row[0] if row else None


def set_last_sync_time(video_id: str, iso_datetime: str):
    _db_write(
        "INSERT OR REPLACE INTO video_last_sync(video_id, last_sync_at) VALUES(?,?)",
        (video_id, iso_datetime),
    )


# ── Video properties snapshot ──────────────────────────────────────────────────

def get_video_props(video_id: str) -> dict | None:
    row = _db_read_one("SELECT props FROM video_props WHERE video_id=?", (video_id,))
    return json.loads(row[0]) if row else None


def set_video_props(video_id: str, data: dict):
    _db_write(
        "INSERT OR REPLACE INTO video_props(video_id, props) VALUES(?,?)",
        (video_id, json.dumps(data)),
    )


# ── Notion last_edited_time (in-memory) ───────────────────────────────────────

def get_notion_last_edited(video_id: str) -> str | None:
    return _notion_last_edited.get(video_id)


def set_notion_last_edited(video_id: str, iso_datetime: str):
    _notion_last_edited[video_id] = iso_datetime


def bulk_set_notion_last_edited(mapping: dict[str, str]):
    """Batch-set from prefetch loop — single dict update, no per-call overhead."""
    _notion_last_edited.update(mapping)


# ── Bulk video ID set ──────────────────────────────────────────────────────────

def video_ids_loaded() -> bool:
    return _existing_video_ids is not None


def set_existing_video_ids(ids: set[str]):
    """
    Called once after prefetch. Merges prefetch results with whatever
    is already in the DB (from a prior GitHub Actions cache restore).
    """
    global _existing_video_ids
    db_ids = {row[0] for row in _db_read_all("SELECT video_id FROM video_pages")}
    _existing_video_ids = ids | db_ids
    logger.info(
        f"[Cache] Existing video set: {len(ids)} from prefetch + "
        f"{len(db_ids)} from DB = {len(_existing_video_ids)} total"
    )


def video_exists(video_id: str) -> bool:
    if _existing_video_ids is None:
        raise RuntimeError("video ID cache not loaded yet")
    return video_id in _existing_video_ids


def mark_video_exists(video_id: str):
    if _existing_video_ids is not None:
        _existing_video_ids.add(video_id)


# ── Persistence ────────────────────────────────────────────────────────────────

def load_from_disk():
    """
    Open (or create) sync.db, create schema, pre-load video IDs.
    On a fresh start the DB is empty and _existing_video_ids stays None
    until prefetch runs and calls set_existing_video_ids().
    """
    global _existing_video_ids
    _init_schema()
    db_ids = {row[0] for row in _db_read_all("SELECT video_id FROM video_pages")}
    if db_ids:
        _existing_video_ids = db_ids
        logger.info(f"[Cache] {len(db_ids)} video page_id(s) pre-loaded from DB.")
    else:
        logger.info("[Cache] DB is empty — fresh start.")


def save_to_disk():
    """
    SQLite writes are already durable. This flushes the WAL and logs stats.
    Kept so existing call-sites in main.py / run_sync.py don't need changes.
    """
    with _write_lock:
        con = _get_write_con()
        con.execute("PRAGMA wal_checkpoint(PASSIVE)")
    _log_stats()


def checkpoint(label: str = ""):
    """Mid-run WAL flush — called every 500 videos."""
    tag = f" [{label}]" if label else ""
    logger.info(f"[Checkpoint{tag}] Flushing WAL…")
    save_to_disk()


def _log_stats():
    try:
        n_pages = _db_read_one("SELECT COUNT(*) FROM video_pages")[0]
        n_etags = _db_read_one("SELECT COUNT(*) FROM video_etags")[0]
        n_syncs = _db_read_one("SELECT COUNT(*) FROM video_last_sync")[0]
        n_props = _db_read_one("SELECT COUNT(*) FROM video_props")[0]
        n_chans = _db_read_one("SELECT COUNT(*) FROM channel_yt")[0]
        n_cats  = _db_read_one("SELECT COUNT(*) FROM categories")[0]
        logger.info(
            f"[Cache] DB stats — pages:{n_pages} etags:{n_etags} "
            f"syncs:{n_syncs} props:{n_props} channels:{n_chans} cats:{n_cats}"
        )
    except Exception as e:
        logger.warning(f"[Cache] Could not read stats: {e}")
