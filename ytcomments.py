import csv
import re
from googleapiclient.discovery import build

# ================= CONFIG =================
API_KEY = "AIzaSyB2p8GXoG_AOm9TEDmqpoQiHoyO7IIe3eU"

VIDEO_URLS = [
    "https://www.youtube.com/shorts/TgRsBG8YCII",
    "https://www.youtube.com/shorts/WLk0n9vCM1A",
    "https://www.youtube.com/shorts/YflhldCLE8Q",
    "https://www.youtube.com/shorts/4gfwwpD9GMc",
    "https://www.youtube.com/shorts/AbjK7vgGpxI",
    "https://www.youtube.com/watch?v=O-dbvoNpB-c",
    "https://www.youtube.com/watch?v=tq4al6ZgXAc",
]

OUTPUT_CSV = "youtube_comments_export.csv"
# ==========================================

youtube = build("youtube", "v3", developerKey=API_KEY)


def extract_video_id(url):
    patterns = [
        r"v=([a-zA-Z0-9_-]{11})",
        r"youtu\.be/([a-zA-Z0-9_-]{11})",
        r"shorts/([a-zA-Z0-9_-]{11})"
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def get_video_details(video_id):
    request = youtube.videos().list(
        part="snippet,statistics",
        id=video_id
    )
    response = request.execute()

    item = response["items"][0]
    snippet = item["snippet"]
    stats = item["statistics"]

    return {
        "title": snippet["title"],
        "views": stats.get("viewCount", 0),
        "likes": stats.get("likeCount", 0),
        "comment_count": stats.get("commentCount", 0)
    }


def export_comments(video_id, video_url, writer):
    video_data = get_video_details(video_id)

    request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id,
        maxResults=100,
        textFormat="plainText"
    )

    while request:
        response = request.execute()

        for item in response["items"]:
            top_comment = item["snippet"]["topLevelComment"]["snippet"]

            writer.writerow([
                video_data["title"],
                video_url,
                video_data["views"],
                video_data["likes"],
                video_data["comment_count"],
                top_comment["authorDisplayName"],
                top_comment["textDisplay"],
                top_comment["likeCount"],
                top_comment["publishedAt"],
                "COMMENT"
            ])

            # Handle replies
            if "replies" in item:
                for reply in item["replies"]["comments"]:
                    reply_snippet = reply["snippet"]
                    writer.writerow([
                        video_data["title"],
                        video_url,
                        video_data["views"],
                        video_data["likes"],
                        video_data["comment_count"],
                        reply_snippet["authorDisplayName"],
                        reply_snippet["textDisplay"],
                        reply_snippet["likeCount"],
                        reply_snippet["publishedAt"],
                        "REPLY"
                    ])

        request = youtube.commentThreads().list_next(request, response)


def main():
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Video Title",
            "Video URL",
            "Video Views",
            "Video Likes",
            "Total Comments",
            "Author",
            "Comment Text",
            "Likes",
            "Published At",
            "Type"
        ])

        for url in VIDEO_URLS:
            video_id = extract_video_id(url)
            if video_id:
                print(f"Fetching comments for: {url}")
                export_comments(video_id, url, writer)
            else:
                print(f"Invalid URL: {url}")

    print(f"\nExport complete → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
