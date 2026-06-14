"""
run_sync.py
───────────
Entry point for GitHub Actions. Reads inputs from environment variables,
writes a temporary CSV if needed, then delegates to the async pipeline
in main.py.

Environment variables (set by sync.yml):
  INPUT_OPTION     — "csv" or "manual"
  VIDEO_IDS        — comma-separated IDs (manual mode)
  CSV_CONTENT      — comma-separated IDs (csv mode, from UI paste/upload)
  CSV_FILE_PATH    — path to a pre-written CSV file (default: input_videos.csv)

  YOUTUBE_API_KEY, NOTION_API_KEY, VIDEO_DATABASE_ID, CHANNEL_DATABASE_ID
  — passed as GitHub Secrets, picked up by main.py via load_dotenv() /
    os.getenv() which also reads process environment variables directly.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# ── Logging (mirrors main.py format so GitHub log output is consistent) ───────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)


def _build_csv(video_ids: list[str], path: Path):
    """
    Write a minimal CSV that read_video_ids() in main.py can consume.
    Header must be 'Video Id' (matches the DictReader check).
    """
    path.write_text("Video Id\n" + "\n".join(video_ids), encoding="utf-8")
    logger.info(f"Wrote {len(video_ids)} video ID(s) to {path}")


def _parse_ids(raw: str) -> list[str]:
    """Split a comma/newline-separated string into a clean list of IDs."""
    ids = []
    seen: set[str] = set()
    for part in raw.replace("\n", ",").split(","):
        vid = part.strip()
        if vid and vid not in seen:
            seen.add(vid)
            ids.append(vid)
    return ids


def prepare_inputs() -> tuple[str, list[str]]:
    """
    Resolve the input mode and final list of video IDs from env vars.

    Returns (input_option, video_ids_list).
    Raises SystemExit on bad config so GitHub Actions shows a clear failure.
    """
    input_option  = os.getenv("INPUT_OPTION", "").strip().lower()
    video_ids_raw = os.getenv("VIDEO_IDS", "").strip()       # manual mode
    csv_content   = os.getenv("CSV_CONTENT", "").strip()     # csv mode (UI paste/upload)
    csv_file_path = Path(os.getenv("CSV_FILE_PATH", "input_videos.csv").strip())

    if input_option not in ("csv", "manual"):
        logger.error(
            f"INPUT_OPTION must be 'csv' or 'manual', got: '{input_option}'"
        )
        sys.exit(1)

    if input_option == "manual":
        if not video_ids_raw:
            logger.error("VIDEO_IDS env var is empty for manual mode.")
            sys.exit(1)
        ids = _parse_ids(video_ids_raw)
        if not ids:
            logger.error("No valid video IDs found in VIDEO_IDS.")
            sys.exit(1)
        logger.info(f"Manual mode — {len(ids)} video ID(s) provided.")
        return "manual", ids

    # csv mode — prefer CSV_CONTENT (from UI) over a pre-existing file
    if csv_content:
        ids = _parse_ids(csv_content)
        if not ids:
            logger.error("CSV_CONTENT is set but contains no valid video IDs.")
            sys.exit(1)
        _build_csv(ids, csv_file_path)
        logger.info(f"CSV mode (content) — {len(ids)} video ID(s).")
    elif csv_file_path.exists():
        logger.info(f"CSV mode (file) — using {csv_file_path}.")
        # IDs will be read by main.py's read_video_ids(); return empty list as signal
        ids = []
    else:
        logger.error(
            f"CSV mode: neither CSV_CONTENT nor file '{csv_file_path}' found."
        )
        sys.exit(1)

    return "csv", ids


def main():
    # ── Import here so missing deps fail with a clean message ─────────────────
    try:
        from main import load_config, read_video_ids, run_sync
        from cache import store
    except ImportError as e:
        logger.error(f"Import failed — is requirements.txt installed? {e}")
        sys.exit(1)

    # ── Validate secrets ───────────────────────────────────────────────────────
    try:
        config = load_config()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # ── Resolve inputs ─────────────────────────────────────────────────────────
    input_option, provided_ids = prepare_inputs()

    # ── Load disk cache (GitHub Actions runner has a clean workspace each run,
    #    so this will be empty — but it's safe and future-proof if you add
    #    cache/ as an Actions artifact and restore it.) ─────────────────────────
    store.load_from_disk()

    # ── Build final video ID list ──────────────────────────────────────────────
    if input_option == "manual":
        # IDs already parsed; pass via a temp CSV so read_video_ids() handles
        # deduplication consistently.
        tmp = Path("input_videos.csv")
        _build_csv(provided_ids, tmp)
        video_ids = read_video_ids("csv", str(tmp))
    else:
        # For csv mode, read_video_ids reads the file we wrote (or the one
        # the workflow pre-created).
        csv_path = os.getenv("CSV_FILE_PATH", "input_videos.csv").strip()
        try:
            video_ids = read_video_ids("csv", csv_path)
        except (ValueError, FileNotFoundError) as e:
            logger.error(str(e))
            sys.exit(1)

    logger.info(f"Starting YouTube → Notion sync for {len(video_ids)} video(s)...")

    # ── Run async pipeline ─────────────────────────────────────────────────────
    try:
        asyncio.run(run_sync(config, video_ids))
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
    finally:
        store.save_to_disk()


if __name__ == "__main__":
    main()
