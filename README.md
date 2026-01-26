# Youtube-to-Notion-
This Python project fetches video data from YouTube using the YouTube Data API and automatically adds or updates entries in a Notion database, including channel details and video metadata.
Features

Fetch video metadata such as title, duration, publish date, category, thumbnail, and more from YouTube.

Retrieve channel information including logo and custom URL.

Automatically add new video entries to Notion database or update existing entries.

Maintain a relational link between videos and channels in Notion.

Supports batch input of video IDs via CSV or manual entry.

Converts YouTube video durations to human-readable format.

Automatically fetches YouTube video category names.

Requirements

Python 3.8+

YouTube Data API Key

Notion Integration Token and Database IDs

Required Python packages:

pip install requests pandas pytz google-api-python-client

Setup

Clone the repository:

git clone https://github.com/your-username/youtube-notion-sync.git
cd youtube-notion-sync


Obtain API keys:

YouTube Data API Key

Notion Integration Token

Update the script with your credentials:

api_key = 'YOUR_YOUTUBE_API_KEY'
notion_api_key = 'YOUR_NOTION_API_KEY'
video_database_id = 'YOUR_NOTION_VIDEO_DATABASE_ID'
channel_database_id = 'YOUR_NOTION_CHANNEL_DATABASE_ID'


Prepare video IDs:

Option 1: Create a CSV file with a column Video Id.

Option 2: Input video IDs manually when prompted.

Usage

Run the script:

python main.py


Choose the input option:

csv – Provide a CSV file with video IDs.

manual – Enter video IDs manually separated by commas.

The script fetches video and channel data from YouTube and updates or adds entries in Notion automatically.

How it Works

Fetch Video Data

Uses YouTube Data API to get video details: title, duration, thumbnail, category, channel info.

Converts ISO 8601 duration format to a human-readable string.

Fetch Channel Data

Retrieves channel logo and custom URL.

Checks if the channel already exists in Notion before adding a new entry.

Add or Update Video in Notion

Checks if the video already exists in the Notion database.

Updates the entry if video data has changed.

Links the video to the corresponding channel entry in Notion.

Example

CSV input:

Video Id
dQw4w9WgXcQ
3JZ_D3ELwOQ


Manual input:

Enter video IDs separated by commas: dQw4w9WgXcQ, 3JZ_D3ELwOQ


The script then fetches all video and channel details and updates your Notion database.

Notes

Make sure your Notion database has the required properties:

Video Database: Name, Video Id, Date, Duration, Thumbnail, URL, Category Id, Category Name, Channel (relation to Channel database)

Channel Database: Name, Channel Id, URL, Logo (optional)

Ensure the YouTube Data API quota is sufficient for your video requests.
