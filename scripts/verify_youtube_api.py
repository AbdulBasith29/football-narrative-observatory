from __future__ import annotations

import os

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def main() -> None:
    load_dotenv()

    api_key = os.getenv("YOUTUBE_API_KEY")
    video_id = os.getenv("TEST_YOUTUBE_VIDEO_ID")

    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is missing from .env")

    youtube = build(
        "youtube",
        "v3",
        developerKey=api_key,
        cache_discovery=False,
    )

    try:
        response = (
            youtube.commentThreads()
            .list(
                part="snippet",
                videoId=video_id,
                order="time",
                maxResults=10,
                textFormat="plainText",
            )
            .execute()
        )
    except HttpError as exc:
        raise RuntimeError(f"YouTube API request failed: {exc}") from exc

    comments = response.get("items", [])

    print(f"API connection successful.")
    print(f"Comments retrieved: {len(comments)}")
    print(f"Next page available: {bool(response.get('nextPageToken'))}")

    for item in comments[:3]:
        snippet = item["snippet"]["topLevelComment"]["snippet"]
        print("-" * 50)
        print(snippet.get("publishedAt"))
        print(snippet.get("textOriginal"))


if __name__ == "__main__":
    main()
