import streamlit as st
import pymongo
import psycopg2
from googleapiclient.discovery import build
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient(
    "mongodb://akshata:akshata@ac-yyhborw-shard-00-00.3lgvrmk.mongodb.net:27017,ac-yyhborw-shard-00-01.3lgvrmk.mongodb.net:27017,ac-yyhborw-shard-00-02.3lgvrmk.mongodb.net:27017/?ssl=true&replicaSet=atlas-h8gknb-shard-0&authSource=admin&retryWrites=true&w=majority")
mongo_db = mongo_client["youtube_data"]
mongo_coll = mongo_db["channels"]

# PostgreSQL connection
pg_connection = psycopg2.connect(
    host="localhost",
    port=5432,
    user=postgres,
    password=Akshata,
database = "youtube_data"
)
pg_cursor = pg_connection.cursor()


def api_connect():
    api_key = 'AIzaSyANG7OdhL5_5UVDrQLo4OlKG3eO2VeLspI'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube


# Function to retrieve channel data from YouTube API
def get_channel_data(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    response = request.execute()

    datas = []
    for item in response["items"]:
        data = {
            'channel_id': item["id"],
            'channel_name': item["snippet"]["title"],
            'subscription_count': int(item["statistics"]["subscriberCount"]),
            'channel_views': int(item["statistics"]["viewCount"]),
            'channel_description': item["snippet"]["description"],
            'playlist_id': item["contentDetails"]["relatedPlaylists"]["uploads"],
            'publishedAt': item["snippet"]["publishedAt"],
            'videoCount': int(item["statistics"]["videoCount"])
        }
        datas.append(data)

    return datas


# Retrieve videos for the channel
def get_video_ids(youtube, playlist_id):
    video_ids = []

    videos_request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = videos_request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        videos_request = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = videos_request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids


def get_video_details(youtube, video_ids):
    all_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response['items']:
            video_info = {
                'video_id': video['id'],
                'channel_title': video['snippet']['channelTitle'],
                'title': video['snippet']['title'],
                'description': video['snippet']['description'],
                'tags': video['snippet']['tags'],
                'publishedAt': video['snippet']['publishedAt'],
                'viewCount': video['statistics'].get('viewCount', 0),
                'likeCount': video['statistics'].get('likeCount', 0),
                'commentCount': video['statistics'].get('commentCount', 0),
                'duration': video['contentDetails']['duration'],
                'definition': video['contentDetails']['definition']
            }
            all_info.append(video_info)

    return all_info


def get_comments_in_video(youtube, video_id):
    all_comments = []

    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100
    )
    response = request.execute()

    while response:
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            all_comments.append(comment)

        if 'nextPageToken' in response:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                pageToken=response['nextPageToken'],
                maxResults=100
            )
            response = request.execute()
        else:
            break

    return all_comments


# Streamlit app
def main():
    st.title("YouTube Data Analysis")

    # Connect to YouTube API
    youtube = api_connect()

    # User input for channel ID
    channel_id = st.text_input("Enter YouTube Channel ID")

    if channel_id:
        # Retrieve channel data
        channel_data = get_channel_data(youtube, channel_id)

        if st.button("Save Channel Data to MongoDB"):
            # Store channel data in MongoDB
            mongo_coll.insert_many(channel_data)
            st.success("Channel data saved to MongoDB")

    if st.button("Retrieve Data from MongoDB"):
        # Retrieve channel data from MongoDB
        channel_data = mongo_coll.find()
        channel_df = pd.DataFrame(list(channel_data))
        st.dataframe(channel_df)

    if st.button("Migrate Data to SQL Data Warehouse"):
        # Create tables in SQL data warehouse
        create_table_query = """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            subscription_count INT,
            channel_views INT,
            channel_description TEXT,
            playlist_id TEXT,
            publishedAt TEXT,
            videoCount INT
        );

        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_title TEXT,
            title TEXT,
            description TEXT,
            tags TEXT,
            publishedAt TEXT,
            viewCount INT,
            likeCount INT,
            commentCount INT,
            duration TEXT,
            definition TEXT
        );

        CREATE TABLE IF NOT EXISTS comments (
            video_id TEXT,
            comment TEXT
        );
        """
        pg_cursor.execute(create_table_query)

        # Retrieve data from MongoDB and insert into SQL data warehouse
        channel_data = mongo_coll.find()
        for channel in channel_data:
            pg_cursor.execute(
                "INSERT INTO channels VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    channel["channel_id"],
                    channel["channel_name"],
                    channel["subscription_count"],
                    channel["channel_views"],
                    channel["channel_description"],
                    channel["playlist_id"],
                    channel["publishedAt"],
                    channel["videoCount"]
                )
            )

            video_ids = get_video_ids(youtube, channel["playlist_id"])
            video_details = get_video_details(youtube, video_ids)
            for video in video_details:
                pg_cursor.execute(
                    "INSERT INTO videos VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        video["video_id"],
                        video["channel_title"],
                        video["title"],
                        video["description"],
                        ",".join(video["tags"]),
                        video["publishedAt"],
                        int(video["viewCount"]),
                        int(video["likeCount"]),
                        int(video["commentCount"]),
                        video["duration"],
                        video["definition"]
                    )
                )

                comments = get_video_comments(youtube, video["video_id"])
                for comment in comments:
                    pg_cursor.execute(
                        "INSERT INTO comments VALUES (%s, %s)",
                        (video["video_id"], comment)
                    )

        pg_connection.commit()
        st.success("Data migrated to SQL data warehouse")

    if st.button("Retrieve Data from SQL Data Warehouse"):
        # Retrieve data from SQL data warehouse
        pg_cursor.execute("SELECT * FROM channels")
        channel_data = pg_cursor.fetchall()
        channel_df = pd.DataFrame(channel_data, columns=[desc[0] for desc in pg_cursor.description])
        st.dataframe(channel_df)

        pg_cursor.execute("SELECT * FROM videos")
        video_data = pg_cursor.fetchall()
        video_df = pd.DataFrame(video_data, columns=[desc[0] for desc in pg_cursor.description])
        st.dataframe(video_df)

        pg_cursor.execute("SELECT * FROM comments")
        comment_data = pg_cursor.fetchall()
        comment_df = pd.DataFrame(comment_data, columns=[desc[0] for desc in pg_cursor.description])
        st.dataframe(comment_df)

    # SQL queries and display results as tables
    if st.button("Query: Names of all videos and their corresponding channels"):
        query = """
        SELECT videos.title, channels.channel_name
        FROM videos
        INNER JOIN channels
        ON videos.channel_title = channels.channel_name
        """
        pg_cursor.execute(query)
        result = pg_cursor.fetchall()
        query_df = pd.DataFrame(result, columns=["Video Title", "Channel Name"])
        st.dataframe(query_df)

    if st.button("Query: Channels with the most number of videos and their video count"):
        query = """
        SELECT channel_name, COUNT(*) AS video_count
        FROM videos
        INNER JOIN channels
        ON videos.channel_title = channels.channel_name
        GROUP BY channel_name
        ORDER BY video_count DESC
        """
        pg_cursor.execute(query)
        result = pg_cursor.fetchall()
        query_df = pd.DataFrame(result, columns=["Channel Name", "Video Count"])
        st.dataframe(query_df)

    if st.button("Query: Top 10 most viewed videos and their respective channels"):
        query = """
        SELECT videos.title, channels.channel_name, videos.viewCount
        FROM videos
        INNER JOIN channels
        ON videos.channel_title = channels.channel_name
        ORDER BY videos.viewCount DESC
        LIMIT 10
        """
        pg_cursor.execute(query)
        result = pg_cursor.fetchall()
        query_df = pd.DataFrame(result, columns=["Video Title", "Channel Name", "View Count"])
        st.dataframe(query_df)


# Run the Streamlit app
if __name__ == '__main__':
    main()
