"""
run_sync.py — Entry point for GitHub Actions.
Reads INPUT_OPTION, VIDEO_IDS, and CSV_FILE_PATH from environment variables
and delegates to the existing get_video_data / main logic.
"""

import os
import sys
import logging
from main import get_video_data, get_or_create_channel_entry, add_data_to_notion
from main import youtube, channel_database_id, logger

def main():
    input_option = os.getenv("INPUT_OPTION", "").strip().lower()
    video_ids_raw = os.getenv("VIDEO_IDS", "").strip()
    csv_file_path = os.getenv("CSV_FILE_PATH", "input_videos.csv").strip()

    logger.info(f"Starting sync — mode: {input_option}")

    if input_option == "manual":
        if not video_ids_raw:
            logger.error("VIDEO_IDS env var is empty for manual mode.")
            sys.exit(1)
        video_ids = [v.strip() for v in video_ids_raw.split(",") if v.strip()]
        video_data = get_video_data(input_option="manual", video_ids=video_ids)

    elif input_option == "csv":
        if not os.path.exists(csv_file_path):
            logger.error(f"CSV file not found: {csv_file_path}")
            sys.exit(1)
        video_data = get_video_data(input_option="csv", file_path=csv_file_path)

    else:
        logger.error(f"Unknown INPUT_OPTION: '{input_option}'. Must be 'csv' or 'manual'.")
        sys.exit(1)

    if not video_data:
        logger.error("No video data retrieved. Exiting.")
        sys.exit(1)

    success_count = 0
    error_count = 0

    for video_info in video_data:
        try:
            channel_entry_id = get_or_create_channel_entry(
                video_info["Channel"],
                video_info["Channel Id"],
                video_info["Channel Logo URL"],
                video_info["Channel Custom URL"],
                channel_database_id,
            )

            if not channel_entry_id:
                logger.warning(f"Skipping '{video_info['Name'][:50]}' — channel create/fetch failed.")
                error_count += 1
                continue

            add_data_to_notion(video_info, channel_entry_id)
            success_count += 1

        except Exception as e:
            logger.error(f"Error on '{video_info.get('Name', 'Unknown')[:50]}': {e}")
            error_count += 1

    logger.info("=" * 50)
    logger.info(f"Sync complete! ✅ {success_count} succeeded, ❌ {error_count} failed.")
    logger.info("=" * 50)

    if error_count > 0 and success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
