"""
notion/client.py
────────────────
Async Notion API client with:
  • Exponential-backoff retry on transient errors (429, 5xx)
  • Bulk video-ID prefetch (paginated, stable sort, captures last_edited_time)
  • Three-way skip/restore/update decision on every video
  • Semaphore-limited concurrency
  • Periodic checkpoint saves every 500 videos

Three-way decision matrix in add_or_update_video
──────────────────────────────────────────────────
  ETag missing                       → FULL UPDATE
    First run for this video; no baseline to compare against.

  ETag changed                       → FULL UPDATE
    YouTube data changed; fetch fresh props and PATCH Notion.

  ETag unchanged + Notion untouched  → TRUE SKIP
    Nothing changed anywhere. Zero API calls.
    "Untouched" = notion_last_edited_time <= our last_sync_time.

  ETag unchanged + Notion touched    → RESTORE
    Someone edited or deleted properties in Notion manually.
    Re-PATCH using the cached props snapshot — no YouTube call needed.
    "Touched" = notion_last_edited_time > our last_sync_time.

last_edited_time is captured free-of-charge during the prefetch scan
(it's already in every Notion page result) and stored in memory via
store.set_notion_last_edited(). No extra API calls at sync time.
"""

import asyncio
import logging
from datetime import datetime, timezone
from aiohttp import ClientSession, ClientTimeout

from cache import store

logger = logging.getLogger(__name__)

NOTION_API_BASE    = "https://api.notion.com/v1"
NOTION_API_VERSION = "2022-06-28"
NOTION_CONCURRENCY = 10
MAX_RETRIES        = 3
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
CHECKPOINT_EVERY   = 500   # save caches to disk every N videos processed

_PREFETCH_TIMEOUT = ClientTimeout(connect=10, total=600)


def _headers(api_key: str) -> dict:
    return {
        'Authorization':  f'Bearer {api_key}',
        'Content-Type':   'application/json',
        'Notion-Version': NOTION_API_VERSION,
    }


def _now_iso() -> str:
    """Current UTC time as ISO 8601 string — used as last_sync_time."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _parse_iso(iso: str) -> datetime:
    """Parse an ISO 8601 string to a timezone-aware datetime."""
    # Notion uses format: 2024-01-15T10:30:00.000Z
    return datetime.fromisoformat(iso.replace('Z', '+00:00'))


# ── Low-level HTTP with retry ─────────────────────────────────────────────────

async def _request(session: ClientSession, method: str, url: str,
                   api_key: str, **kwargs) -> dict | None:
    hdrs = _headers(api_key)
    for attempt in range(MAX_RETRIES):
        try:
            async with session.request(method, url, headers=hdrs, **kwargs) as resp:
                data = await resp.json()
                if resp.status == 200:
                    return data
                if resp.status in RETRYABLE_STATUSES:
                    wait = 2 ** attempt
                    logger.warning(
                        f"[Notion] {method} {url} → {resp.status} "
                        f"(attempt {attempt+1}/{MAX_RETRIES}, retrying in {wait}s)"
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.error(f"[Notion] {method} {url} → {resp.status}: {data}")
                return None
        except Exception as e:
            wait = 2 ** attempt
            if attempt == MAX_RETRIES - 1:
                logger.error(f"[Notion] {method} {url} — final failure: {e}")
                return None
            logger.warning(f"[Notion] {method} {url} — exception: {e}, retrying in {wait}s")
            await asyncio.sleep(wait)
    return None


async def _get(session, url, api_key):
    return await _request(session, "GET", url, api_key)

async def _post(session, url, api_key, payload):
    return await _request(session, "POST", url, api_key, json=payload)

async def _patch(session, url, api_key, payload):
    return await _request(session, "PATCH", url, api_key, json=payload)


# ── Bulk video-ID prefetch ────────────────────────────────────────────────────

async def prefetch_existing_video_ids(session: ClientSession, api_key: str,
                                       video_db_id: str):
    """
    Paginated scan of the entire Notion video database.

    Captures per page:
      • page_id        → stored in _video_page_map
      • last_edited_time → stored in _notion_last_edited (in-memory)

    last_edited_time is used at sync time to detect manual Notion edits
    without any extra API calls — it's already present in every result.

    Pagination fixes:
      • Explicit ascending sort by created_time — prevents non-deterministic
        early termination (was the root cause of the 100-page / 9,936-video
        truncation in production).
      • Dedicated session with 600s total timeout (vs main session's 20s).
      • Per-page logging for immediate visibility of any truncation.

    VIDEO_ID_PROPERTY_ID:
      Set to your actual Notion property ID to slim responses ~10×.
      Find it: GET /v1/databases/{video_db_id} → properties → "Video Id" → id
      Leave as None to fetch full payloads (slower but always correct).
    """
    VIDEO_ID_PROPERTY_ID: str | None = None  # ← replace with your property ID

    url = f"{NOTION_API_BASE}/databases/{video_db_id}/query"
    new_ids: set[str] = set()
    cursor   = None
    page_num = 0

    async with ClientSession(timeout=_PREFETCH_TIMEOUT) as prefetch_session:
        while True:
            payload: dict = {
                "page_size": 100,
                "sorts": [{"timestamp": "created_time", "direction": "ascending"}],
            }
            if cursor:
                payload["start_cursor"] = cursor
            if VIDEO_ID_PROPERTY_ID:
                payload["filter_properties"] = [VIDEO_ID_PROPERTY_ID]

            page_num += 1
            logger.info(
                f"[Prefetch] Page {page_num} | collected={len(new_ids)} | "
                f"cursor={'set' if cursor else 'none'}"
            )

            data = await _post(prefetch_session, url, api_key, payload)

            if not data:
                logger.warning(
                    f"[Prefetch] Page {page_num} returned no data — "
                    f"collected {len(new_ids)} IDs so far. "
                    f"Remaining pages skipped; affected videos will be "
                    f"re-created instead of updated this run."
                )
                break

            for result in data.get("results", []):
                page_id      = result.get("id")
                last_edited  = result.get("last_edited_time")  # free — already in response
                rt = result.get("properties", {}).get("Video Id", {}).get("rich_text", [])
                if rt and page_id:
                    video_id = rt[0]["text"]["content"]
                    if not store.get_video_page_id(video_id):
                        store.set_video_page_id(video_id, page_id)
                    new_ids.add(video_id)
                    # Store last_edited_time in memory for skip/restore decisions
                    if last_edited:
                        store.set_notion_last_edited(video_id, last_edited)

            has_more    = data.get("has_more", False)
            next_cursor = data.get("next_cursor")

            if not has_more:
                logger.info(
                    f"[Prefetch] has_more=False at page {page_num} — "
                    f"scan complete. Total collected: {len(new_ids)}"
                )
                break

            if not next_cursor:
                logger.warning(
                    f"[Prefetch] has_more=True but next_cursor missing at "
                    f"page {page_num}. Cannot continue. Collected {len(new_ids)}."
                )
                break

            cursor = next_cursor

    store.set_existing_video_ids(new_ids)
    logger.info(
        f"[Notion] Prefetch done — {len(new_ids)} video(s) in {page_num} page(s). "
        f"Total known (incl. disk cache): {len(store._video_page_map)}."
    )


# ── Channel operations ────────────────────────────────────────────────────────

async def _check_channel_in_notion(session, api_key, channel_id, channel_db_id) -> str | None:
    cached = store.get_notion_channel(channel_id)
    if cached is not False:
        return cached

    url  = f"{NOTION_API_BASE}/databases/{channel_db_id}/query"
    data = await _post(session, url, api_key,
                       {"filter": {"property": "Channel Id",
                                   "rich_text": {"equals": channel_id}}})
    page_id = None
    if data and data.get("results"):
        page_id = data["results"][0]["id"]

    store.set_notion_channel(channel_id, page_id)
    return page_id


async def _create_channel(session, api_key, channel_db_id,
                           name, channel_id, logo_url, custom_url) -> str | None:
    props: dict = {
        "Name":       {"title":     [{"text": {"content": name[:2000]}}]},
        "Channel Id": {"rich_text": [{"text": {"content": channel_id}}]},
    }
    if custom_url:
        props["URL"] = {"url": custom_url}

    payload: dict = {"parent": {"database_id": channel_db_id}, "properties": props}
    if logo_url:
        payload["icon"] = {"type": "external", "external": {"url": logo_url}}

    data = await _post(session, f"{NOTION_API_BASE}/pages", api_key, payload)
    if data:
        page_id = data["id"]
        store.set_notion_channel(channel_id, page_id)
        logger.info(f'[Notion] Channel created: "{name}"')
        return page_id
    return None


async def _update_channel(session, api_key, page_id,
                           name, channel_id, logo_url, custom_url):
    existing = await _get(session, f"{NOTION_API_BASE}/pages/{page_id}", api_key)
    if not existing:
        return

    ep     = existing.get("properties", {})
    e_name = (ep.get("Name", {}).get("title") or [{}])[0].get("text", {}).get("content", "")
    e_url  = (ep.get("URL") or {}).get("url", "")

    if e_name == name and e_url == custom_url:
        logger.info(f'[Notion] Channel up to date: "{name}"')
        return

    props: dict = {
        "Name":       {"title":     [{"text": {"content": name[:2000]}}]},
        "Channel Id": {"rich_text": [{"text": {"content": channel_id}}]},
    }
    if custom_url:
        props["URL"] = {"url": custom_url}

    payload: dict = {"properties": props}
    if logo_url:
        payload["icon"] = {"type": "external", "external": {"url": logo_url}}

    await _patch(session, f"{NOTION_API_BASE}/pages/{page_id}", api_key, payload)
    logger.info(f'[Notion] Channel updated: "{name}"')


async def get_or_create_channel(session, api_key, channel_db_id,
                                 name, channel_id, logo_url, custom_url) -> str | None:
    existing_id = await _check_channel_in_notion(session, api_key, channel_id, channel_db_id)
    if existing_id:
        await _update_channel(session, api_key, existing_id,
                              name, channel_id, logo_url, custom_url)
        return existing_id
    return await _create_channel(session, api_key, channel_db_id,
                                  name, channel_id, logo_url, custom_url)


# ── Video properties builder ──────────────────────────────────────────────────

def _video_properties(data: dict, channel_page_id: str | None) -> dict:
    props: dict = {
        "Name":          {"title":     [{"text": {"content": data["Name"][:2000]}}]},
        "Video Id":      {"rich_text": [{"text": {"content": data["Video Id"]}}]},
        "Date":          {"date":      {"start": data["Date"]}},
        "Duration":      {"rich_text": [{"text": {"content": data.get("Duration", "")}}]},
        "Category Id":   {"select":    {"name": data.get("Category Id", "")}},
        "Category Name": {"select":    {"name": data.get("Category Name", "")}},
    }
    if data.get("Thumbnail"):
        props["Thumbnail"] = {"url": data["Thumbnail"]}
    if data.get("URL"):
        props["URL"] = {"url": data["URL"]}
    if channel_page_id:
        props["Channel"] = {"relation": [{"id": channel_page_id}]}
    return props


def _cover(data: dict) -> dict | None:
    if data.get("Thumbnail"):
        return {"type": "external", "external": {"url": data["Thumbnail"]}}
    return None


# ── Three-way decision helpers ────────────────────────────────────────────────

def _etag_changed(video_id: str, current_etag: str | None) -> bool:
    """
    True if the YouTube item-level etag differs from our cached value,
    OR if we have no cached etag (first run for this video).
    """
    if current_etag is None:
        return True   # YouTube didn't return an etag — treat as changed
    cached = store.get_video_etag(video_id)
    if cached is None:
        return True   # No baseline — must do full update
    return cached != current_etag


def _notion_touched(video_id: str) -> bool:
    """
    True if Notion's last_edited_time is strictly after our last_sync_time.

    This catches manual property deletions or edits made directly in Notion
    between sync runs. last_edited_time is captured during prefetch (free),
    last_sync_time is written after every successful create/PATCH.

    Returns False (not touched) if either timestamp is missing — we err on
    the side of skipping rather than unnecessary PATCHes when data is absent.
    """
    last_edited = store.get_notion_last_edited(video_id)
    last_sync   = store.get_last_sync_time(video_id)

    if not last_edited or not last_sync:
        # Missing data: if no last_sync we've never written → not "touched by
        # someone else". If no last_edited something odd happened → skip safely.
        return False

    try:
        return _parse_iso(last_edited) > _parse_iso(last_sync)
    except Exception:
        return False   # malformed timestamp — don't crash, just skip


# ── Video write operation ─────────────────────────────────────────────────────

async def add_or_update_video(session: ClientSession, api_key: str,
                               video_db_id: str,
                               data: dict,
                               current_etag: str | None,
                               channel_page_id: str | None,
                               semaphore: asyncio.Semaphore,
                               progress: dict,
                               progress_lock: asyncio.Lock,
                               sync_start_time: float):
    """
    Create or update a single video in Notion using the three-way decision:

    FULL UPDATE  — ETag missing or changed
      Write all properties to Notion (create or PATCH).
      Update etag, last_sync, props snapshot in cache.

    TRUE SKIP    — ETag unchanged AND Notion untouched
      Do nothing. Zero API calls. Log the skip.

    RESTORE      — ETag unchanged BUT Notion touched
      Re-PATCH Notion using the cached props snapshot.
      No YouTube call needed — we already have the right data.
      Update last_sync (etag unchanged so don't update it).

    progress_lock guards the shared counter against concurrent mutation.
    sync_start_time is captured after prefetch so throughput reflects
    actual video processing speed, not prefetch overhead.
    """
    import time as _time

    async with semaphore:
        video_id   = data["Video Id"] if data else None
        short_name = (data["Name"][:50] if data else video_id) or video_id

        # ── Determine action ──────────────────────────────────────────────────
        exists         = store.video_exists(video_id)
        etag_is_new    = _etag_changed(video_id, current_etag)
        notion_changed = _notion_touched(video_id) if exists else False

        if exists and not etag_is_new and not notion_changed:
            # ── TRUE SKIP ─────────────────────────────────────────────────────
            logger.debug(f'[Notion] Skip (unchanged): "{short_name}"')
            action = "skipped"

        elif exists and not etag_is_new and notion_changed:
            # ── RESTORE ───────────────────────────────────────────────────────
            # Notion was manually edited/deleted; re-PATCH from cached snapshot.
            cached_data = store.get_video_props(video_id)
            if not cached_data:
                # No snapshot available — can't restore without YouTube data.
                # This only happens if the video was added outside this script.
                logger.warning(
                    f'[Notion] Notion touched but no prop snapshot for '
                    f'"{short_name}" — skipping restore. Will fix on next '
                    f'full update (when ETag changes).'
                )
                action = "skipped"
            else:
                page_id = store.get_video_page_id(video_id)
                props   = _video_properties(cached_data, channel_page_id)
                cover   = _cover(cached_data)
                payload: dict = {"properties": props}
                if cover:
                    payload["cover"] = cover
                await _patch(session,
                             f"{NOTION_API_BASE}/pages/{page_id}",
                             api_key, payload)
                store.set_last_sync_time(video_id, _now_iso())
                logger.info(f'[Notion] Restored: "{short_name}"')
                action = "restored"

        else:
            # ── FULL UPDATE (create or PATCH) ─────────────────────────────────
            props = _video_properties(data, channel_page_id)
            cover = _cover(data)

            if exists:
                page_id = store.get_video_page_id(video_id)
                if not page_id:
                    logger.warning(
                        f'[Notion] No cached page_id for "{short_name}", '
                        f'skipping update.'
                    )
                    action = "skipped"
                else:
                    payload = {"properties": props}
                    if cover:
                        payload["cover"] = cover
                    await _patch(session,
                                 f"{NOTION_API_BASE}/pages/{page_id}",
                                 api_key, payload)
                    logger.info(f'[Notion] Updated: "{short_name}"')
                    action = "updated"
            else:
                payload = {"parent": {"database_id": video_db_id}, "properties": props}
                if cover:
                    payload["cover"] = cover
                resp = await _post(session,
                                   f"{NOTION_API_BASE}/pages",
                                   api_key, payload)
                if resp:
                    store.set_video_page_id(video_id, resp["id"])
                    logger.info(f'[Notion] Created: "{short_name}"')
                    action = "created"
                else:
                    action = "skipped"

            # Update caches after any successful full update
            if action in ("created", "updated"):
                if current_etag:
                    store.set_video_etag(video_id, current_etag)
                store.set_last_sync_time(video_id, _now_iso())
                store.set_video_props(video_id, data)

        # ── Progress tracking ─────────────────────────────────────────────────
        async with progress_lock:
            progress["done"] += 1
            if action == "skipped":
                progress["skipped"] = progress.get("skipped", 0) + 1
            elif action == "restored":
                progress["restored"] = progress.get("restored", 0) + 1
            done  = progress["done"]
            total = progress["total"]

        if done % 10 == 0 or done == total:
            elapsed    = _time.monotonic() - sync_start_time
            throughput = done / elapsed if elapsed > 0 else 0
            skipped    = progress.get("skipped", 0)
            restored   = progress.get("restored", 0)
            logger.info(
                f"[Progress] {done}/{total} ({int(done/total*100)}%) "
                f"— {throughput:.1f} videos/sec "
                f"| skipped={skipped} restored={restored}"
            )

        # ── Periodic checkpoint ───────────────────────────────────────────────
        if done % CHECKPOINT_EVERY == 0:
            store.checkpoint(f"{done}/{total}")
