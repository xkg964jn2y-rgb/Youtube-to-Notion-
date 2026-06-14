"""
youtube/client.py
─────────────────
Fully async YouTube Data API v3 client using aiohttp.

ETag handling
─────────────
YouTube returns an etag at two levels:
  • Response-level etag  — changes if ANY item in the batch changes.
  • Item-level etag      — changes only when THAT specific video changes.

We use the item-level etag (item['etag']) for per-video change detection.
If a video's item etag matches our cached value, we still include it in the
batch result but flag it as etag_unchanged=True so notion/client.py can
apply the three-way skip/restore/update decision without us pre-filtering.

WHY we don't skip the YouTube fetch entirely when ETags are cached:
  YouTube's videos.list endpoint is billed per-batch (50 videos = 1 unit),
  not per-video. Skipping individual videos inside a batch saves nothing on
  quota. We still fetch the whole batch, extract item etags, and let Notion
  client decide what to do per video.

  The real quota saving is that videos with unchanged ETags never trigger
  a Notion PATCH — which is the expensive, rate-limited side.
"""

import asyncio
import logging
import pytz
import isodate

from datetime import datetime, timedelta
from typing import AsyncGenerator
from aiohttp import ClientSession

from cache import store

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE    = "https://www.googleapis.com/youtube/v3"
YOUTUBE_BATCH_SIZE  = 50
YOUTUBE_CONCURRENCY = 10
YT_MAX_RETRIES      = 3
YT_RETRYABLE        = {429, 500, 502, 503, 504}


# ── Helpers ───────────────────────────────────────────────────────────────────

def convert_duration(iso_duration: str) -> str:
    try:
        td: timedelta = isodate.parse_duration(iso_duration)
        total_seconds = int(td.total_seconds())
        h, remainder  = divmod(total_seconds, 3600)
        m, s          = divmod(remainder, 60)
        parts = []
        if h: parts.append(f"{h} hours")
        if m: parts.append(f"{m} mins")
        if s: parts.append(f"{s} secs")
        return " ".join(parts) if parts else "0s"
    except Exception as e:
        logger.error(f"Error converting duration '{iso_duration}': {e}")
        return "Unknown"


def parse_date(published_at: str) -> str:
    try:
        dt = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
        return dt.astimezone(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except Exception:
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')


def best_thumbnail(thumbnails: dict) -> str | None:
    for q in ('maxres', 'standard', 'high', 'medium', 'default'):
        if q in thumbnails:
            return thumbnails[q]['url']
    return None


# ── Async API helpers ─────────────────────────────────────────────────────────

async def _yt_get(session: ClientSession, endpoint: str,
                  api_key: str, params: dict) -> dict | None:
    """Async GET with exponential-backoff retry."""
    params = {**params, 'key': api_key}
    url    = f"{YOUTUBE_API_BASE}/{endpoint}"

    for attempt in range(YT_MAX_RETRIES):
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status in YT_RETRYABLE:
                    wait = 2 ** attempt
                    logger.warning(
                        f"[YouTube] {endpoint} → {resp.status} "
                        f"(attempt {attempt+1}/{YT_MAX_RETRIES}, retrying in {wait}s)"
                    )
                    await asyncio.sleep(wait)
                    continue
                body = await resp.text()
                logger.error(
                    f"[YouTube] {endpoint} → {resp.status} (non-retryable): {body[:200]}"
                )
                return None
        except Exception as e:
            wait = 2 ** attempt
            if attempt == YT_MAX_RETRIES - 1:
                logger.error(
                    f"[YouTube] {endpoint} — final failure after "
                    f"{YT_MAX_RETRIES} attempts: {e}"
                )
                return None
            logger.warning(
                f"[YouTube] {endpoint} — network error: {e}, retrying in {wait}s"
            )
            await asyncio.sleep(wait)
    return None


async def fetch_channel_details(session: ClientSession, api_key: str,
                                 channel_id: str) -> dict:
    cached = store.get_yt_channel(channel_id)
    if cached is not None:
        return cached

    result = {'Channel Custom URL': None, 'Channel Logo URL': None}
    data = await _yt_get(session, "channels", api_key,
                         {'part': 'snippet,brandingSettings', 'id': channel_id})
    if data and data.get('items'):
        snippet    = data['items'][0]['snippet']
        custom_url = snippet.get('customUrl')
        thumbs     = snippet.get('thumbnails', {})
        logo       = thumbs.get('high', thumbs.get('medium', thumbs.get('default')))
        result = {
            'Channel Custom URL': f"https://www.youtube.com/{custom_url}" if custom_url else None,
            'Channel Logo URL':   logo['url'] if logo else None,
        }

    store.set_yt_channel(channel_id, result)
    return result


async def fetch_category_name(session: ClientSession, api_key: str,
                               category_id: str) -> str | None:
    cached = store.get_category(category_id)
    if cached is not False:
        return cached

    name = None
    data = await _yt_get(session, "videoCategories", api_key,
                         {'part': 'snippet', 'id': category_id})
    if data and data.get('items'):
        name = data['items'][0]['snippet']['title']

    store.set_category(category_id, name)
    return name


# ── Batch processor ───────────────────────────────────────────────────────────

async def _process_batch(session: ClientSession, api_key: str,
                          batch: list[str], batch_num: int,
                          total_batches: int) -> list[dict]:
    """
    Fetch one batch of up to 50 video IDs from YouTube.

    Each result dict includes a 'etag' key (item-level) so notion/client.py
    can apply the three-way skip/restore/update decision per video.

    We always fetch the full batch from YouTube regardless of cached ETags
    because YouTube bills per-batch not per-video — skipping individual
    videos inside a batch doesn't save quota. The Notion PATCH is where
    the real saving happens (via ETag comparison in add_or_update_video).
    """
    data = await _yt_get(session, "videos", api_key, {
        'part': 'snippet,contentDetails,statistics',
        'id':   ','.join(batch),
    })

    if not data or not data.get('items'):
        logger.warning(f"[YouTube] Batch {batch_num}: no items returned")
        return []

    items = data['items']

    # Deduplicate channel/category IDs before fan-out
    unique_channel_ids  = list(dict.fromkeys(i['snippet']['channelId']  for i in items))
    unique_category_ids = list(dict.fromkeys(i['snippet']['categoryId'] for i in items))

    # Count how many items have unchanged ETags for logging
    unchanged_count = sum(
        1 for i in items
        if store.get_video_etag(i['id']) == i.get('etag') and i.get('etag')
    )

    logger.info(
        f"[YouTube] Batch {batch_num}/{total_batches}: {len(items)} videos | "
        f"{len(unique_channel_ids)} channel(s) | "
        f"{len(unique_category_ids)} category/ies | "
        f"{unchanged_count} etag-unchanged"
    )

    channel_list, category_list = await asyncio.gather(
        asyncio.gather(*[
            fetch_channel_details(session, api_key, cid)
            for cid in unique_channel_ids
        ]),
        asyncio.gather(*[
            fetch_category_name(session, api_key, cat)
            for cat in unique_category_ids
        ]),
    )

    channel_map  = dict(zip(unique_channel_ids,  channel_list))
    category_map = dict(zip(unique_category_ids, category_list))

    results: list[dict] = []
    for item in items:
        try:
            snippet      = item['snippet']
            channel_id   = snippet['channelId']
            category_id  = snippet['categoryId']
            channel_data = channel_map[channel_id]

            results.append({
                # Core video fields
                'Name':               snippet['title'],
                'Video Id':           item['id'],
                'Date':               parse_date(snippet['publishedAt']),
                'Channel':            snippet['channelTitle'],
                'Channel Id':         channel_id,
                'Duration':           convert_duration(item['contentDetails']['duration']),
                'Thumbnail':          best_thumbnail(snippet.get('thumbnails', {})),
                'Category Id':        category_id,
                'Category Name':      category_map.get(category_id),
                'URL':                f"https://www.youtube.com/watch?v={item['id']}",
                'Channel Custom URL': channel_data['Channel Custom URL'],
                'Channel Logo URL':   channel_data['Channel Logo URL'],
                # Item-level ETag — used by notion/client.py for change detection
                'etag':               item.get('etag'),
            })
        except Exception as e:
            logger.error(f"Error processing video {item.get('id')}: {e}")

    return results


# ── Streaming async generator ─────────────────────────────────────────────────

async def get_video_stats_stream(
    api_key: str,
    video_ids: list[str],
) -> AsyncGenerator[list[dict], None]:
    """
    Async generator that yields completed batches as they arrive.

    Each batch is a list of video dicts including the 'etag' field.
    notion/client.py uses this etag in add_or_update_video to decide
    between FULL UPDATE, TRUE SKIP, or RESTORE.

    asyncio.as_completed yields futures in completion order (fastest first)
    so Notion writes start as soon as the first batch finishes, not after
    the slowest batch unblocks everything.
    """
    batches = [
        video_ids[i:i + YOUTUBE_BATCH_SIZE]
        for i in range(0, len(video_ids), YOUTUBE_BATCH_SIZE)
    ]
    total = len(batches)
    logger.info(
        f"[YouTube] {len(video_ids)} video(s) → {total} batch(es), "
        f"concurrency={YOUTUBE_CONCURRENCY}"
    )

    semaphore = asyncio.Semaphore(YOUTUBE_CONCURRENCY)

    async def _bounded_batch(batch: list[str], num: int) -> list[dict]:
        async with semaphore:
            return await _process_batch(session, api_key, batch, num, total)

    async with ClientSession() as session:
        tasks = [
            asyncio.create_task(_bounded_batch(batch, num))
            for num, batch in enumerate(batches, 1)
        ]
        for fut in asyncio.as_completed(tasks):
            try:
                batch_result = await fut
                if batch_result:
                    yield batch_result
            except Exception as e:
                logger.error(f"[YouTube] Batch failed: {e}")
