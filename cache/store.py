"""
cache/store.py
──────────────
All in-memory caches with optional disk persistence.

Caches in this module
─────────────────────
  _channel_yt          YouTube channel_id  → {Custom URL, Logo URL}        (persisted)
  _category            YouTube category_id → "Music" / None                (persisted)
  _channel_notion      Notion  channel_id  → Notion page id / None         (in-memory)
  _video_page_map      video_id → Notion page_id                           (persisted)
  _video_etag          video_id → YouTube item-level etag                  (persisted)
  _video_last_sync     video_id → ISO datetime of last successful write     (persisted)
  _video_props         video_id → last written raw video data dict          (persisted)
  _notion_last_edited  video_id → Notion last_edited_time from prefetch    (in-memory)
  _existing_video_ids  set of all video_ids already in Notion              (in-memory)

Decision matrix (evaluated in notion/client.py → add_or_update_video):
────────────────────────────────────────────────────────────────────────
  ETag missing                       → FULL UPDATE  (first run, no baseline)
  ETag changed                       → FULL UPDATE  (YouTube data changed)
  ETag unchanged + Notion untouched  → TRUE SKIP    (zero API calls)
  ETag unchanged + Notion touched    → RESTORE      (PATCH from _video_props,
                                                      no YouTube call needed)

"Notion touched" means: notion_last_edited_time > our last_sync_time,
which indicates a manual edit or deletion happened in Notion since we
last wrote to this page.  last_edited_time is captured free-of-charge
during the prefetch scan and held in _notion_last_edited.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

# ── In-memory stores ──────────────────────────────────────────────────────────
_channel_yt: dict[str, dict]           = {}
_category:   dict[str, str | None]     = {}
_channel_notion: dict[str, str | None] = {}
_video_page_map: dict[str, str]        = {}
_video_etag: dict[str, str]            = {}
_video_last_sync: dict[str, str]       = {}
_video_props: dict[str, dict]          = {}
_notion_last_edited: dict[str, str]    = {}   # in-memory only, re-populated each run
_existing_video_ids: set[str] | None   = None


# ── YouTube channel cache ─────────────────────────────────────────────────────

def get_yt_channel(channel_id: str) -> dict | None:
    return _channel_yt.get(channel_id)

def set_yt_channel(channel_id: str, data: dict):
    _channel_yt[channel_id] = data


# ── YouTube category cache ────────────────────────────────────────────────────

def get_category(category_id: str) -> str | None | bool:
    """Returns False if not cached, None if cached-but-missing, str if found."""
    if category_id not in _category:
        return False
    return _category[category_id]

def set_category(category_id: str, name: str | None):
    _category[category_id] = name


# ── Notion channel cache ──────────────────────────────────────────────────────

def get_notion_channel(channel_id: str) -> str | None | bool:
    """Returns False if not cached, None if not in Notion, str page_id if found."""
    if channel_id not in _channel_notion:
        return False
    return _channel_notion[channel_id]

def set_notion_channel(channel_id: str, page_id: str | None):
    _channel_notion[channel_id] = page_id


# ── video_id → page_id map ────────────────────────────────────────────────────

def get_video_page_id(video_id: str) -> str | None:
    return _video_page_map.get(video_id)

def set_video_page_id(video_id: str, page_id: str):
    _video_page_map[video_id] = page_id
    if _existing_video_ids is not None:
        _existing_video_ids.add(video_id)


# ── ETag cache ────────────────────────────────────────────────────────────────

def get_video_etag(video_id: str) -> str | None:
    """Return cached YouTube item-level etag, or None if not known."""
    return _video_etag.get(video_id)

def set_video_etag(video_id: str, etag: str):
    """Store YouTube item-level etag after a successful fetch."""
    _video_etag[video_id] = etag


# ── Last sync time ────────────────────────────────────────────────────────────

def get_last_sync_time(video_id: str) -> str | None:
    """Return ISO datetime of last successful Notion write, or None."""
    return _video_last_sync.get(video_id)

def set_last_sync_time(video_id: str, iso_datetime: str):
    """Record datetime of a successful Notion create or PATCH."""
    _video_last_sync[video_id] = iso_datetime


# ── Video properties snapshot ─────────────────────────────────────────────────

def get_video_props(video_id: str) -> dict | None:
    """
    Return the last raw video data dict written to Notion.

    WHY raw data dict instead of Notion properties payload:
      The raw dict (Name, Video Id, Duration, etc.) is smaller and more
      portable than the full Notion payload. It gets passed back through
      _video_properties() on the RESTORE path, which rebuilds the Notion
      payload fresh — so channel relation and cover are always included.
    """
    return _video_props.get(video_id)

def set_video_props(video_id: str, data: dict):
    """
    Snapshot the raw video data dict after every successful write.
    Excludes channel_page_id (resolved fresh on restore) so we don't
    store stale Notion page references.
    """
    _video_props[video_id] = data


# ── Notion last_edited_time (in-memory, populated from prefetch) ──────────────

def get_notion_last_edited(video_id: str) -> str | None:
    """
    Return Notion last_edited_time captured during prefetch.

    WHY in-memory only:
      This value is always re-fetched fresh from Notion at the start of
      each run (during prefetch). Persisting it to disk would give us a
      stale value from the previous run, defeating the purpose — we need
      the *current* Notion state, not what it was last time we ran.
    """
    return _notion_last_edited.get(video_id)

def set_notion_last_edited(video_id: str, iso_datetime: str):
    _notion_last_edited[video_id] = iso_datetime


# ── Bulk video ID cache ───────────────────────────────────────────────────────

def video_ids_loaded() -> bool:
    return _existing_video_ids is not None

def set_existing_video_ids(ids: set[str]):
    """Called once after prefetch; merges with disk-loaded IDs."""
    global _existing_video_ids
    _existing_video_ids = ids | set(_video_page_map.keys())

def video_exists(video_id: str) -> bool:
    if _existing_video_ids is None:
        raise RuntimeError("video ID cache not loaded yet")
    return video_id in _existing_video_ids

def mark_video_exists(video_id: str):
    if _existing_video_ids is not None:
        _existing_video_ids.add(video_id)


# ── Disk persistence ──────────────────────────────────────────────────────────

def save_to_disk():
    """Persist all caches to disk."""
    files = {
        "channel_yt.json":      _channel_yt,
        "category.json":        _category,
        "video_page_map.json":  _video_page_map,
        "video_etag.json":      _video_etag,
        "video_last_sync.json": _video_last_sync,
        "video_props.json":     _video_props,
    }
    errors: list[str] = []
    for filename, obj in files.items():
        try:
            (CACHE_DIR / filename).write_text(json.dumps(obj, indent=2))
        except Exception as e:
            errors.append(filename)
            logger.warning(f"Failed to save {filename}: {e}")

    logger.info(
        f"Cache saved — "
        f"{len(_channel_yt)} channels, "
        f"{len(_category)} categories, "
        f"{len(_video_page_map)} page_ids, "
        f"{len(_video_etag)} etags, "
        f"{len(_video_last_sync)} sync-times, "
        f"{len(_video_props)} prop-snapshots."
    )
    if errors:
        logger.warning(f"Failed to save: {errors}")


def load_from_disk():
    """Load persisted caches at startup. Missing files are silently skipped."""
    global _channel_yt, _category, _video_page_map
    global _video_etag, _video_last_sync, _video_props

    loaders = [
        ("channel_yt.json",      "_channel_yt"),
        ("category.json",        "_category"),
        ("video_page_map.json",  "_video_page_map"),
        ("video_etag.json",      "_video_etag"),
        ("video_last_sync.json", "_video_last_sync"),
        ("video_props.json",     "_video_props"),
    ]
    targets = {
        "_channel_yt":      _channel_yt,
        "_category":        _category,
        "_video_page_map":  _video_page_map,
        "_video_etag":      _video_etag,
        "_video_last_sync": _video_last_sync,
        "_video_props":     _video_props,
    }

    for filename, varname in loaders:
        path = CACHE_DIR / filename
        if not path.exists():
            continue
        try:
            loaded = json.loads(path.read_text())
            targets[varname].update(loaded)
            logger.info(f"Loaded {len(loaded)} entries from {filename}.")
        except Exception as e:
            logger.warning(f"Could not load {filename}: {e}")

    if _video_page_map:
        global _existing_video_ids
        _existing_video_ids = set(_video_page_map.keys())
        logger.info(f"{len(_existing_video_ids)} video page_id(s) pre-loaded from disk.")


def checkpoint(label: str = ""):
    """
    Mid-run cache save — called every 500 videos so a crash or Ctrl-C
    doesn't lose the entire run's etag/page_id/props data.
    """
    tag = f" [{label}]" if label else ""
    logger.info(f"[Checkpoint{tag}] Saving caches mid-run...")
    save_to_disk()
