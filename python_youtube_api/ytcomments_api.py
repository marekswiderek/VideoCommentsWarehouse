import os
import googleapiclient.discovery
from airflow.models import Variable
import pandas as pd
import numpy as np
import traceback

def setup_youtube_api():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = os.getenv("YT_API_KEY")

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    
    return youtube

def get_video(youtube_api, video_id):
    request = youtube_api.videos().list(
        part="snippet, statistics",
        id=video_id,
    )
    response = request.execute()
    #print(response["items"][0])

    return response

def prepare_video_data(videos_list, video_response):
    extracted_video = {
        "channel_name": video_response["items"][0]["snippet"]["channelTitle"],
        "video_title": video_response["items"][0]["snippet"]["title"],
        "published_at": video_response["items"][0]["snippet"]["publishedAt"],
        "view_count": video_response["items"][0]["statistics"]["viewCount"],
        "like_count": video_response["items"][0]["statistics"]["likeCount"],
        "comment_count": video_response["items"][0]["statistics"]["commentCount"]
    }
    videos_list.append(extracted_video)

    return videos_list

def get_video_comments(youtube_api, video_id, page_token=None, max_results=100):
    request = youtube_api.commentThreads().list(
        part="snippet,replies",
        videoId=video_id,
        maxResults=max_results,
        pageToken=page_token
    )
    response = request.execute()

    return response["items"], response.get("nextPageToken")

def prepare_video_comments_data(comments_list, comment_threads_response):
    for comment in comment_threads_response:
        extracted_comment = {
            "comment_author": comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
            "comment_text": comment["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
            "like_count": comment["snippet"]["topLevelComment"]["snippet"]["likeCount"],
            "published_at": comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
            "total_replies": comment["snippet"]["totalReplyCount"],
            "is_public": comment["snippet"]["isPublic"]
        }
        comments_list.append(extracted_comment)
    
    return comments_list

def extract_video_to_csv(video_id):
    print(">> Extracting video data for VIDEO_ID: "+video_id)
    youtube_api = setup_youtube_api()

    # Empty list of videos for prepare_video_data function to append prepared data to
    videos = []

    # CSV file name 
    csv_file_name = video_id + "_video"

    prepare_video_data(videos, get_video(youtube_api, video_id))

    video_df = pd.DataFrame(videos)
    # Add video_id column
    video_df.insert(loc=0, column="video_id", value=video_id)
    # Save DataFrame to CSV
    try:
        video_df.to_csv(rf"./source_files/{csv_file_name}.csv", index=False)
    except Exception:
        print(f">> There was an error saving {csv_file_name}.csv file.")
        traceback.print_exc()
    else:
        print(f">> Created {csv_file_name}.csv file")
    

def extract_video_comments_to_csv(video_id):
    print(">> Extracting comments data for VIDEO_ID: "+video_id)
    youtube_api = setup_youtube_api()

    # First execution should be with next_page_token set to None - fetch 1st page of comments
    next_page_token = None
    # Empty list of comments for prepare_video_comments_data function to append prepared data to
    comments = []

    # CSV file name 
    csv_file_name = video_id + "_comments"

    # Iterate over comments until next_page_token is None
    while True:
        response, next_page_token = get_video_comments(youtube_api, video_id, next_page_token)
        prepare_video_comments_data(comments, response)

        if next_page_token is None:
            break
    
    comments_df = pd.DataFrame(comments)

    # Add video_id column
    comments_df.insert(loc=0, column="video_id", value=video_id)
    # Save DataFrame to CSV
    try:
        comments_df.to_csv(rf"./source_files/{csv_file_name}.csv", index=False)
    except Exception as e:
         print(f">> There was an error saving {csv_file_name}.csv file.")
         traceback.print_exc()
    else:
        print(f">> Created {csv_file_name}.csv file")

def prepare_video_and_comments_data():
    print(f"Current wd: {os.getcwd()}")
    video_id = Variable.get("video_id")
    extract_video_comments_to_csv(video_id)
    extract_video_to_csv(video_id)

def main():
    pass
    
if __name__ == "__main__":
    main()


    
