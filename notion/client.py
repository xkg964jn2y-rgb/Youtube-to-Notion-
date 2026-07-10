"""
notion/client.py
────────────────
Async Notion API client.

Three-way decision matrix in add_or_update_video
──────────────────────────────────────────────────
  ETag missing                       → FULL UPDATE  (first run, no baseline)
  ETag changed                       → FULL UPDATE  (YouTube data changed)
  ETag unchanged + Notion untouched  → TRUE SKIP    (zero API calls)
  ETag unchanged + Notion touched    → RESTORE      (PATCH from cached props,
                                                      no YouTube call needed)

Prefetch
─────────
  Paginates the entire Notion video DB using a stable ascending sort.
  Accumulates {video_id: page_id} and {video_id: last_edited_time} per
  Notion page (100 results), then flushes both in a single SQLite
  transaction via bulk_set_video_page_ids() / bulk_set_notion_last_edited().
  A crash mid-prefetch leaves a consistent partial state — no partial rows,
  no duplicate creates on the next run.
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
CHECKPOINT_EVERY   = 500

_PREFETCH_TIMEOUT = ClientTimeout(connect=10, total=600)


def _headers(api_key: str) -> dict:
    return {
        'Authorization':  f'Bearer {api_key}',
        'Content-Type':   'application/json',
        'Notion-Version': NOTION_API_VERSION,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _parse_iso(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace('Z', '+00:00'))


# ── Low-level HTTP with retry ──────────────────────────────────────────────────

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
            logger.warning(
                f"[Notion] {method} {url} — exception: {e}, retrying in {wait}s"
            )
            await asyncio.sleep(wait)
    return None


async def _get(session, url, api_key):
    return await _request(session, "GET", url, api_key)

async def _post(session, url, api_key, payload):
    return await _request(session, "POST", url, api_key, json=payload)

async def _patch(session, url, api_key, payload):
    return await _request(session, "PATCH", url, api_key, json=payload)


# ── Bulk video-ID prefetch ─────────────────────────────────────────────────────

# Monthly time windows used to split the prefetch into chunks that each
# stay well under Notion's undocumented 100-page / ~10k-row cursor limit.
# The limit only triggers when a `sorts` clause is present, but removing
# sorts makes pagination non-deterministic.  The correct fix is to use
# `filter` on created_time with NO sorts — each window is small enough
# that pagination completes naturally before hitting the cap.
#
# Windows start from 2005-01-01 (YouTube's founding) and run monthly up
# to "now + 1 month" so any recently-added pages are always covered.
# A 32k-video database spread over ~20 years = ~130 videos/month on
# average, so each window typically needs only 1–2 API pages.

from datetime import timezone as _tz, timedelta as _td

def _monthly_windows(start_year: int = 2005) -> list[tuple[str, str]]:
    """
    Return a list of (after, before) ISO strings covering every calendar
    month from start_year-01-01 to now + 1 month.
    Each pair maps to a Notion filter:
        created_time >= after  AND  created_time < before
    """
    windows = []
    now     = datetime.now(_tz.utc)
    # end sentinel: first day of next month
    end_year  = now.year + (1 if now.month == 12 else 0)
    end_month = 1 if now.month == 12 else now.month + 1
    end_dt    = datetime(end_year, end_month, 1, tzinfo=_tz.utc)

    year, month = start_year, 1
    while True:
        after_dt  = datetime(year, month, 1, tzinfo=_tz.utc)
        # advance one month
        if month == 12:
            year += 1; month = 1
        else:
            month += 1
        before_dt = datetime(year, month, 1, tzinfo=_tz.utc)

        windows.append((
            after_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            before_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        ))

        if after_dt >= end_dt:
            break
    return windows


async def _prefetch_window(prefetch_session: ClientSession, api_key: str,
                            url: str, after: str, before: str,
                            video_id_prop_id: str | None,
                            window_num: int, total_windows: int) -> int:
    """
    Paginate one monthly time window without a sorts clause.
    Returns the number of NEW page_id mappings written for this window.
    """
    cursor    = 0          # use int as local page counter
    api_cursor = None      # actual Notion cursor token
    new_in_window = 0

    while True:
        cursor += 1
        payload: dict = {
            "page_size": 100,
            "filter": {
                "and": [
                    {"timestamp": "created_time", "created_time": {"on_or_after":  after}},
                    {"timestamp": "created_time", "created_time": {"before":        before}},
                ]
            },
        }
        if api_cursor:
            payload["start_cursor"] = api_cursor
        if video_id_prop_id:
            payload["filter_properties"] = [video_id_prop_id]

        logger.info(
            f"[Prefetch] Window {window_num}/{total_windows} "
            f"({after[:7]}) page {cursor} | new_so_far={new_in_window}"
        )

        data = await _post(prefetch_session, url, api_key, payload)

        if not data:
            logger.warning(
                f"[Prefetch] Window {window_num} page {cursor} "
                f"returned no data — stopping this window."
            )
            break

        # Accumulate then bulk-write (one SQLite tx per 100 results)
        page_map:    dict[str, str] = {}
        last_edited: dict[str, str] = {}

        for result in data.get("results", []):
            page_id = result.get("id")
            ts      = result.get("last_edited_time")
            rt = (
                result.get("properties", {})
                      .get("Video Id", {})
                      .get("rich_text", [])
            )
            if rt and page_id:
                video_id = rt[0]["text"]["content"]
                if not store.get_video_page_id(video_id):
                    page_map[video_id] = page_id
                if ts:
                    last_edited[video_id] = ts

        if page_map:
            store.bulk_set_video_page_ids(page_map)
        if last_edited:
            store.bulk_set_notion_last_edited(last_edited)

        new_in_window += len(page_map)

        has_more    = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

        if not has_more:
            break

        if not next_cursor:
            logger.warning(
                f"[Prefetch] Window {window_num} page {cursor}: "
                f"has_more=True but next_cursor missing — stopping window."
            )
            break

        api_cursor = next_cursor

    return new_in_window


async def _lookup_video_in_notion(prefetch_session: ClientSession, api_key: str,
                                   url: str, video_id: str) -> tuple[str, str | None, str | None]:
    """
    Single targeted query: find one video_id in Notion and return
    (video_id, page_id, last_edited_time).  Used only for video IDs
    that are missing from the local DB cache.
    """
    data = await _post(prefetch_session, url, api_key, {
        "page_size": 1,
        "filter": {
            "property": "Video Id",
            "rich_text": {"equals": video_id},
        },
    })
    if data and data.get("results"):
        result  = data["results"][0]
        page_id = result.get("id")
        ts      = result.get("last_edited_time")
        return video_id, page_id, ts
    return video_id, None, None


async def prefetch_existing_video_ids(session: ClientSession, api_key: str,
                                       video_db_id: str,
                                       video_ids: list[str],
                                       full_scan: bool = False):
    """
    Smart prefetch — only contacts Notion for video IDs genuinely missing
    from the local SQLite cache.

    Decision logic
    ──────────────
    CACHED   — page_id already in DB → skip Notion entirely.
               last_edited_time is fetched lazily only when needed
               (etag unchanged + exists = potential restore).

    MISSING  — page_id not in DB → targeted single-video Notion query.
               Handles videos created outside this tool or from runs
               whose DB was not persisted.

    Warm-cache example (your 353s → ~4s case):
      19,445 cached + 7 new = 7 targeted queries, not 259 page scans.

    Full windowed scan (full_scan=True or FULL_PREFETCH=1 env var)
    ───────────────────────────────────────────────────────────────
    Forces a complete re-scan using monthly created_time filter windows
    (no sorts clause — avoids Notion's undocumented 100-page/10k-row cap).
    Use when: DB was deleted, videos added outside this tool, or you want
    to rebuild the cache from scratch.
    """
    VIDEO_ID_PROPERTY_ID: str | None = None  # ← replace with your property ID

    url = f"{NOTION_API_BASE}/databases/{video_db_id}/query"

    # ── Determine which IDs need a Notion lookup ───────────────────────────────
    if full_scan:
        missing_ids = list(video_ids)
    else:
        missing_ids  = [vid for vid in video_ids if not store.get_video_page_id(vid)]
        cached_count = len(video_ids) - len(missing_ids)
        logger.info(
            f"[Prefetch] {cached_count}/{len(video_ids)} video IDs already cached "
            f"— {len(missing_ids)} need Notion lookup"
        )

    if not missing_ids and not full_scan:
        db_ids = {r[0] for r in store._db_read_all("SELECT video_id FROM video_pages")}
        store.set_existing_video_ids(db_ids)
        logger.info(
            f"[Prefetch] All {len(video_ids)} video IDs cached — "
            f"skipping Notion scan entirely. DB total: {len(db_ids)}."
        )
        return

    # ── Full windowed scan when forced or DB is completely empty ───────────────
    if full_scan or not store._db_read_one("SELECT 1 FROM video_pages LIMIT 1"):
        windows     = _monthly_windows(start_year=2005)
        total_w     = len(windows)
        grand_total = 0
        logger.info(f"[Prefetch] Full windowed scan — {total_w} monthly windows")

        async with ClientSession(timeout=_PREFETCH_TIMEOUT) as prefetch_session:
            for idx, (after, before) in enumerate(windows, 1):
                new = await _prefetch_window(
                    prefetch_session, api_key, url,
                    after, before,
                    VIDEO_ID_PROPERTY_ID,
                    window_num=idx, total_windows=total_w,
                )
                grand_total += new
                if new:
                    logger.info(
                        f"[Prefetch] Window {idx}/{total_w} ({after[:7]}) "
                        f"— {new} new | running total={grand_total}"
                    )

        db_ids = {r[0] for r in store._db_read_all("SELECT video_id FROM video_pages")}
        store.set_existing_video_ids(db_ids)
        logger.info(
            f"[Notion] Full prefetch done — {grand_total} new mapping(s) "
            f"across {total_w} windows. DB total: {len(db_ids)}."
        )
        return

    # ── Targeted lookup for only the missing IDs ───────────────────────────────
    LOOKUP_CONCURRENCY = 20
    sem      = asyncio.Semaphore(LOOKUP_CONCURRENCY)
    found    = 0
    notfound = 0

    async def _bounded_lookup(vid: str):
        async with sem:
            return await _lookup_video_in_notion(prefetch_session, api_key, url, vid)

    logger.info(
        f"[Prefetch] Targeted lookup for {len(missing_ids)} missing video ID(s) "
        f"(concurrency={LOOKUP_CONCURRENCY})"
    )

    async with ClientSession(timeout=_PREFETCH_TIMEOUT) as prefetch_session:
        tasks   = [asyncio.create_task(_bounded_lookup(vid)) for vid in missing_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    page_map:    dict[str, str] = {}
    last_edited_map: dict[str, str] = {}

    for res in results:
        if isinstance(res, Exception):
            logger.error(f"[Prefetch] Lookup error: {res}")
            continue
        vid, page_id, ts = res
        if page_id:
            page_map[vid] = page_id
            if ts:
                last_edited_map[vid] = ts
            found += 1
        else:
            notfound += 1

    if page_map:
        store.bulk_set_video_page_ids(page_map)
    if last_edited_map:
        store.bulk_set_notion_last_edited(last_edited_map)

    db_ids = {r[0] for r in store._db_read_all("SELECT video_id FROM video_pages")}
    store.set_existing_video_ids(db_ids)

    logger.info(
        f"[Prefetch] Targeted lookup done — "
        f"{found} found in Notion, {notfound} genuinely new. "
        f"DB total: {len(db_ids)}."
    )


async def fetch_notion_last_edited(session: ClientSession, api_key: str,
                                    video_db_id: str, video_id: str) -> str | None:
    """
    Lazily fetch last_edited_time for a single video from Notion.
    Called only on the RESTORE check path (etag unchanged + video exists).
    Result is cached in-memory so repeated calls are free.
    """
    cached = store.get_notion_last_edited(video_id)
    if cached:
        return cached

    url  = f"{NOTION_API_BASE}/databases/{video_db_id}/query"
    data = await _post(session, url, api_key, {
        "page_size": 1,
        "filter": {"property": "Video Id", "rich_text": {"equals": video_id}},
    })
    if data and data.get("results"):
        ts = data["results"][0].get("last_edited_time")
        if ts:
            store.set_notion_last_edited(video_id, ts)
            return ts
    return None


# ── Channel operations ─────────────────────────────────────────────────────────

async def _check_channel_in_notion(session, api_key,
                                    channel_id, channel_db_id) -> str | None:
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
    existing_id = await _check_channel_in_notion(
        session, api_key, channel_id, channel_db_id
    )
    if existing_id:
        await _update_channel(session, api_key, existing_id,
                              name, channel_id, logo_url, custom_url)
        return existing_id
    return await _create_channel(session, api_key, channel_db_id,
                                  name, channel_id, logo_url, custom_url)


# ── Video properties builder ───────────────────────────────────────────────────

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


# ── Three-way decision helpers ─────────────────────────────────────────────────

def _etag_changed(video_id: str, current_etag: str | None) -> bool:
    if current_etag is None:
        return True
    cached = store.get_video_etag(video_id)
    if cached is None:
        return True
    return cached != current_etag


async def _notion_touched(session: ClientSession, api_key: str,
                           video_db_id: str, video_id: str) -> bool:
    """
    True if Notion last_edited_time > our last_sync_time.

    last_edited_time is populated by prefetch for missing IDs, but for
    cached IDs (the common case) we never fetched it — we don't need it
    unless the etag is unchanged AND the video already exists (potential
    restore).  So we fetch it lazily here, cache the result in-memory,
    and return False if last_sync is missing (video never written by us).
    """
    last_sync = store.get_last_sync_time(video_id)
    if not last_sync:
        return False   # never written by this tool — can't have been "touched"

    last_edited = store.get_notion_last_edited(video_id)
    if not last_edited:
        # Not populated by prefetch (cached video) — fetch it now
        last_edited = await fetch_notion_last_edited(
            session, api_key, video_db_id, video_id
        )
    if not last_edited:
        return False

    try:
        return _parse_iso(last_edited) > _parse_iso(last_sync)
    except Exception:
        return False


# ── Video write operation ──────────────────────────────────────────────────────

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
    Three-way decision:

    FULL UPDATE  — ETag missing or changed
      Write all properties to Notion (create or PATCH).
      Update etag, last_sync, props snapshot in DB.

    TRUE SKIP    — ETag unchanged AND Notion untouched
      Zero API calls.

    RESTORE      — ETag unchanged BUT Notion touched
      Re-PATCH from cached props snapshot (no YouTube call needed).
      Update last_sync only (etag is still current).
      last_edited_time fetched lazily — only when etag is unchanged
      and the video already exists (the only case where it matters).
    """
    import time as _time

    async with semaphore:
        video_id   = data["Video Id"]
        short_name = (data["Name"][:50] or video_id)

        exists      = store.video_exists(video_id)
        etag_is_new = _etag_changed(video_id, current_etag)

        # Lazy: only call Notion for last_edited if etag is unchanged on an
        # existing video.  New videos and changed-etag videos never need it.
        notion_changed = (
            await _notion_touched(session, api_key, video_db_id, video_id)
            if exists and not etag_is_new
            else False
        )

        if exists and not etag_is_new and not notion_changed:
            # ── TRUE SKIP ──────────────────────────────────────────────────────
            logger.debug(f'[Notion] Skip (unchanged): "{short_name}"')
            action = "skipped"

        elif exists and not etag_is_new and notion_changed:
            # ── RESTORE ────────────────────────────────────────────────────────
            cached_data = store.get_video_props(video_id)
            if not cached_data:
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
            # ── FULL UPDATE (create or PATCH) ──────────────────────────────────
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

            if action in ("created", "updated"):
                if current_etag:
                    store.set_video_etag(video_id, current_etag)
                store.set_last_sync_time(video_id, _now_iso())
                store.set_video_props(video_id, data)

        # ── Progress tracking ──────────────────────────────────────────────────
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

        if done % CHECKPOINT_EVERY == 0:
            store.checkpoint(f"{done}/{total}")
