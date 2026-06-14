# [YouTube → Notion Sync Platform](https://xkg964jn2y-rgb.github.io/Youtube-to-Notion-/)

An automated backend system that synchronizes YouTube video metadata into Notion at scale.

The system processes **32,000+ YouTube videos across 5,000+ channels**, using asynchronous processing, caching, and API integration to efficiently manage large-scale data synchronization.

---

## 🚀 Features

- Fetches video and channel metadata using YouTube Data API v3
- Stores structured data into Notion databases
- Async pipeline for concurrent API requests
- Deduplication to prevent duplicate entries
- Incremental sync using caching and change detection
- Smart upsert logic (create/update only when data changes)
- GitHub Actions-based automation (no server required)
- CSV upload + manual input support
- Lightweight web UI for triggering and monitoring runs
- Run history + live log tracking

---

## 🧠 Key Engineering Concepts

- Asynchronous programming (Python AsyncIO)
- API rate-limit optimization
- Caching & state persistence
- Deduplication & data consistency
- Workflow automation using CI/CD (GitHub Actions)
- System design for large-scale batch processing

---

## 🏗️ Architecture Overview

YouTube Data API → Async Fetcher → Cache Layer → Dedup Engine → Notion Sync Engine → Notion Database

---

## ⚙️ Tech Stack

- Python
- AsyncIO
- YouTube Data API v3
- Notion API
- GitHub Actions
- HTML / JavaScript (UI)

---

## 📊 Scale

- 32,000+ videos processed
- 5,000+ channels indexed
- Optimized for batch + incremental updates

---

## 🔥 Highlights

- Reduced redundant API calls using caching + deduplication
- Designed system to avoid unnecessary Notion writes
- Fully automated pipeline with zero manual backend hosting
