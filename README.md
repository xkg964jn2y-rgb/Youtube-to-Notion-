# YouTube to Notion

A Python pipeline that fetches YouTube video and channel metadata via the YouTube Data API and syncs it into a Notion database — with deduplication, smart updates, and relational linking between videos and channels.

## 📊 Scale
- **22,000+** YouTube videos processed
- **3,000+** channels tracked
- Handles batching, error recovery, and duplicate detection automatically

## ✨ Features
- Fetches video details: title, duration, thumbnail, category, publish date
- Fetches channel details: name, logo, custom URL
- Creates **relational links** between videos and their channels in Notion
- **Deduplication** — skips or updates existing entries, never creates duplicates
- Supports two input modes: CSV file or manual video ID entry
- Structured logging throughout for easy debugging

## 🛠 Tech Stack
- Python
- YouTube Data API v3
- Notion API
- `google-api-python-client`, `pandas`, `requests`, `python-dotenv`

## ⚙️ Setup

### Configure environment variables
Create a `.env` file in the root directory:
```
YOUTUBE_API_KEY=your_youtube_api_key
NOTION_API_KEY=your_notion_integration_token
VIDEO_DATABASE_ID=your_notion_video_database_id
CHANNEL_DATABASE_ID=your_notion_channel_database_id
```

### Set up Notion Databases

**Video Database** — requires these properties:

| Property      | Type                   |
|---------------|------------------------|
| Name          | Title                  |
| Video Id      | Text                   |
| Date          | Date                   |
| Duration      | Text                   |
| Thumbnail     | URL                    |
| URL           | URL                    |
| Category Id   | Select                 |
| Category Name | Select                 |
| Channel       | Relation → Channel DB  |

**Channel Database** — requires these properties:

| Property   | Type  |
|------------|-------|
| Name       | Title |
| Channel Id | Text  |
| URL        | URL   |

## ▶️ Usage

```bash
python main.py
```

You'll be prompted to choose an input method:
- **csv** — provide a CSV file with a `Video Id` column
- **manual** — enter video IDs directly, comma-separated


## 🔑 Getting API Keys

- **YouTube Data API v3** → [Google Cloud Console](https://console.cloud.google.com/)
- **Notion Integration Token** → [Notion Developers](https://www.notion.so/my-integrations)
