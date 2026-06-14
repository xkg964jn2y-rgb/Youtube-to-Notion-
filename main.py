"""
main.py
───────
Entry point and async orchestration layer.

Flow:
  1. Validate environment variables
  2. Load disk-persisted caches
  3. Read video IDs from CSV or manual input
  4. Prefetch all existing Notion video IDs + last_edited_times in one pass
  5. Checkpoint cache immediately after prefetch
  6. Stream video metadata from YouTube (fully async, batched)
  7. For each batch: resolve channels, then apply per-video decision:
       FULL UPDATE / TRUE SKIP / RESTORE
  8. Checkpoint every 500 videos; final save on exit
"""

import asyncio
import csv
import logging
import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
from aiohttp import ClientSession, ClientTimeout

from cache import store
from youtube.client import get_video_stats_stream
from notion.client import (
    prefetch_existing_video_ids,
    get_or_create_channel,
    add_or_update_video,
    NOTION_CONCURRENCY,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)


# ── Environment validation ────────────────────────────────────────────────────

def load_config() -> dict:
    load_dotenv()
    required = {
        'YOUTUBE_API_KEY':     os.getenv('YOUTUBE_API_KEY'),
        'NOTION_API_KEY':      os.getenv('NOTION_API_KEY'),
        'VIDEO_DATABASE_ID':   os.getenv('VIDEO_DATABASE_ID'),
        'CHANNEL_DATABASE_ID': os.getenv('CHANNEL_DATABASE_ID'),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Please check your .env file."
        )
    return required  # type: ignore[return-value]


# ── Input reading ─────────────────────────────────────────────────────────────

def read_video_ids(input_option: str, file_path: str | None = None) -> list[str]:
    if input_option == "csv":
        if not file_path:
            raise ValueError("--file is required when using the 'csv' option.")
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or 'Video Id' not in reader.fieldnames:
                raise ValueError("CSV must have a 'Video Id' column.")
            seen: set[str] = set()
            ids: list[str] = []
            for row in reader:
                vid = (row.get('Video Id') or '').strip()
                if vid and vid not in seen:
                    seen.add(vid)
                    ids.append(vid)

        if not ids:
            raise ValueError("No valid video IDs found in CSV.")
        return ids

    if input_option == "manual":
        raw = input("Enter video IDs separated by commas: ").strip()
        seen_m: set[str] = set()
        ids_m: list[str] = []
        for v in raw.split(','):
            vid = v.strip()
            if vid and vid not in seen_m:
                seen_m.add(vid)
                ids_m.append(vid)
        if not ids_m:
            raise ValueError("No video IDs provided.")
        return ids_m

    raise ValueError(f"Unknown input option '{input_option}'. Choose 'csv' or 'manual'.")


# ── In-flight channel deduplication ──────────────────────────────────────────

_channel_inflight: dict[str, asyncio.Task] = {}


async def _resolve_one_channel(session: ClientSession, notion_key: str,
                                channel_db_id: str, channel_id: str,
                                video: dict) -> tuple[str, str | None]:
    page_id = await get_or_create_channel(
        session, notion_key, channel_db_id,
        video['Channel'], channel_id,
        video['Channel Logo URL'], video['Channel Custom URL']
    )
    return channel_id, page_id


async def _resolve_channels(session: ClientSession, notion_key: str,
                             channel_db_id: str,
                             video_batch: list[dict]) -> dict[str, str | None]:
    unique: dict[str, dict] = {}
    for v in video_batch:
        unique.setdefault(v['Channel Id'], v)

    awaitables = []
    for cid, v in unique.items():
        if cid not in _channel_inflight:
            task = asyncio.create_task(
                _resolve_one_channel(session, notion_key, channel_db_id, cid, v)
            )
            _channel_inflight[cid] = task
        awaitables.append(_channel_inflight[cid])

    results = await asyncio.gather(*awaitables, return_exceptions=True)

    mapping: dict[str, str | None] = {}
    for cid, result in zip(unique.keys(), results):
        if isinstance(result, Exception):
            logger.error(f"Channel {cid} failed: {result}")
            mapping[cid] = None
        else:
            _, page_id = result
            mapping[cid] = page_id
    return mapping


async def _push_batch_to_notion(session: ClientSession, notion_key: str,
                                 video_db_id: str, channel_db_id: str,
                                 video_batch: list[dict],
                                 semaphore: asyncio.Semaphore,
                                 progress: dict,
                                 progress_lock: asyncio.Lock,
                                 sync_start_time: float):
    channel_page_map = await _resolve_channels(
        session, notion_key, channel_db_id, video_batch
    )
    tasks = [
        add_or_update_video(
            session, notion_key, video_db_id,
            v,
            v.get('etag'),                          # item-level YouTube etag
            channel_page_map.get(v['Channel Id']),
            semaphore, progress, progress_lock,
            sync_start_time,
        )
        for v in video_batch
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


# ── Main async pipeline ───────────────────────────────────────────────────────

async def run_sync(config: dict, video_ids: list[str]):
    yt_key        = config['YOUTUBE_API_KEY']
    notion_key    = config['NOTION_API_KEY']
    video_db_id   = config['VIDEO_DATABASE_ID']
    channel_db_id = config['CHANNEL_DATABASE_ID']

    semaphore     = asyncio.Semaphore(NOTION_CONCURRENCY)
    progress_lock = asyncio.Lock()
    progress      = {"done": 0, "total": len(video_ids), "skipped": 0, "restored": 0}
    pending_tasks: set[asyncio.Task] = set()
    wall_start    = time.monotonic()
    total_fetched = 0

    timeout = ClientTimeout(connect=5, total=20)

    async with ClientSession(timeout=timeout) as session:

        # ── Prefetch ──────────────────────────────────────────────────────────
        logger.info("Prefetching existing Notion video IDs + last_edited_times...")
        await prefetch_existing_video_ids(session, notion_key, video_db_id)

        # Persist immediately — so a Ctrl-C after prefetch doesn't lose
        # the page_id mappings and last_edited_times we just collected.
        store.save_to_disk()
        logger.info("Cache checkpointed after prefetch.")

        # sync_start_time is AFTER prefetch so throughput reflects actual
        # video processing speed, not the prefetch overhead.
        sync_start_time = time.monotonic()

        logger.info(f"Streaming sync for {len(video_ids)} video(s)...")

        async for video_batch in get_video_stats_stream(yt_key, video_ids):
            total_fetched += len(video_batch)
            logger.info(
                f"[Stream] Batch of {len(video_batch)} received "
                f"({total_fetched}/{len(video_ids)}) — dispatching to Notion"
            )
            task = asyncio.create_task(
                _push_batch_to_notion(
                    session, notion_key, video_db_id, channel_db_id,
                    video_batch, semaphore, progress, progress_lock,
                    sync_start_time,
                )
            )
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        if pending_tasks:
            results = await asyncio.gather(*pending_tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.error(f"[Notion] Batch push failed: {r}")

    # ── Summary ───────────────────────────────────────────────────────────────
    wall_elapsed   = time.monotonic() - wall_start
    sync_elapsed   = time.monotonic() - sync_start_time
    done           = progress["done"]
    skipped        = progress.get("skipped", 0)
    restored       = progress.get("restored", 0)
    written        = done - skipped
    errors         = progress["total"] - done
    throughput     = done / sync_elapsed if sync_elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info(f"Sync complete — wall time {wall_elapsed:.1f}s "
                f"(sync only: {sync_elapsed:.1f}s)")
    logger.info(f"  {done} processed  |  {written} written  |  "
                f"{skipped} skipped  |  {restored} restored  |  {errors} error(s)")
    logger.info(f"  Throughput: {throughput:.1f} videos/sec")
    logger.info("=" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    try:
        config = load_config()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    store.load_from_disk()

    input_option = input("Choose input option (csv/manual): ").strip().lower()
    file_path    = None
    if input_option == "csv":
        file_path = input("Enter path to CSV file: ").strip()

    try:
        video_ids = read_video_ids(input_option, file_path)
    except (ValueError, FileNotFoundError) as e:
        logger.error(str(e))
        sys.exit(1)

    logger.info(f"Starting YouTube → Notion sync for {len(video_ids)} video(s)...")

    try:
        asyncio.run(run_sync(config, video_ids))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl-C) — saving caches and exiting.")
    finally:
        store.save_to_disk()


if __name__ == "__main__":
    main()