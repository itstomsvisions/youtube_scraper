import os
from dotenv import load_dotenv
import pandas as pd
from googleapiclient.discovery import build


class YoutubeScraper:
    """ ... """
    def __init__(self):
        """Initialize api service"""
        load_dotenv()
        self.devkey = os.getenv('API_KEY')
        if not self.devkey:
            raise ValueError('Cannot find API_KEY...')
        self.api_service_name = 'youtube'
        self.version = 'v3'
        self.yt = build(self.api_service_name, self.version, developerKey=self.devkey)
    

    def find_channel_id(self, username):
        """Find a youtube channel from a username. Return the channel_id of the first search result"""
        try:
            channel = self.yt.search().list(
                part ='snippet',
                q= username,
                type='channel',
                maxResults=1
            ).execute()
            
            if not channel['items']:
                raise ValueError(f'Cannot find any channel named {username}')
            
            channel_id = channel['items'][0]['id']['channelId']
            print('Channel found!')
            return channel_id
        
        except Exception as e:
            print(f'Something went wrong: {e} Check your code dummy!')
            return None


    def get_channel_data(self, channel_id):
        """Create a DataFrame with the data from the desireed username"""
        try:
            channel_info = self.yt.channels().list(
                part='contentDetails, snippet, statistics, topicDetails',
                id=channel_id,
                maxResults=1
            ).execute()

            channel_data = {
                "url": channel_info['items'][0]['snippet'].get('customUrl', ''),
                "name": channel_info['items'][0]['snippet'].get('title'),             
                "description": channel_info['items'][0]['snippet'].get('description', ''),
                "creation_date": channel_info['items'][0]['snippet'].get('publishedAt', ''),
                "country": channel_info['items'][0]['snippet'].get('country', ''),
                "subscribers": channel_info['items'][0]['statistics'].get('subscriberCount'),
                "total_views": channel_info['items'][0]['statistics'].get('viewCount'),
                "total_videos": channel_info['items'][0]['statistics'].get('videoCount'),
                "topic_category": channel_info['items'][0].get('topicDetails', {}).get('topicCategories', []),
                "uploads_playlist_id": channel_info['items'][0]['contentDetails']['relatedPlaylists'].get('uploads'),
                "channel_id": channel_info['items'][0].get('id')
            }
            return channel_data
        
        except Exception as e:
            print(f'There was an error while getting the channel data: {e}')
            return None
  

    def save_channel_data(self, channel_data, csv_path='data/raw_channel_data.csv'):
        """Takes dataframe and saves it to the main csv."""
        # Convert dict into DataFrame
        print('Creating channel Dataframe...')
        df_channel_data = pd.DataFrame([channel_data])
        # Ensure directory exists
        print('Checking if directory exists...')
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        # Check if CSV already exists. If not creates a new one
        print('Checking if channels CSV already exists and updating...')
        if os.path.exists(csv_path):
            existing_csv = pd.read_csv(csv_path)
            updated_csv = pd.concat([existing_csv, df_channel_data], ignore_index=True)
            updated_csv = updated_csv.drop_duplicates(subset='name', keep='last', ignore_index=True)
            updated_csv.to_csv(csv_path, index=False)
        else:
            df_channel_data.to_csv(csv_path, index=False)

    def get_video_ids(self, channel_data):
        playlist_id = channel_data['uploads_playlist_id']
        video_ids = []
        next_page_token = None
        print('Collecting videos IDs...')
        while True:
            uploads = self.yt.playlistItems().list(
                part='id,snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for video in uploads['items']:
                video_id = video['snippet']['resourceId']['videoId']
                video_ids.append(video_id)
            
            next_page_token = uploads.get('nextPageToken')
            if not next_page_token:
                print('Last page reached...')
                break
        return video_ids


    def get_video_data(self, channel_data):
        """Get list of video ids from the upload playlist and use it to get details from every single video uploaded"""
        videos = []
        video_ids = self.get_video_ids(channel_data)
        print(f'Collecting data from {len(video_ids)} videos...')
        for id in video_ids:
            try:
                video_info = self.yt.videos().list(
                    part='contentDetails, snippet, statistics, topicDetails',
                    id=id
                ).execute()
            
                video_data = {
                    "title": video_info['items'][0]['snippet'].get('title'),             
                    "description": video_info['items'][0]['snippet'].get('description'),
                    "channel_name": video_info['items'][0]['snippet'].get('channelTitle'),
                    "thumbnail": video_info['items'][0]['snippet']['thumbnails']['default'].get('url'),
                    "tags": video_info['items'][0]['snippet'].get('tags'),
                    "category": video_info['items'][0]['snippet'].get('categoryId'),
                    "duration": video_info['items'][0]['contentDetails'].get('duration'),
                    "views": video_info['items'][0]['statistics'].get('viewCount'),
                    "likes": video_info['items'][0]['statistics'].get('likeCount'),
                    "comments": video_info['items'][0]['statistics'].get('commentCount'),
                    "upload_date": video_info['items'][0]['snippet'].get('publishedAt'),
                    "video_id": video_info['items'][0].get('id')
                }
                videos.append(video_data)
            except Exception as e:
                print(f'Error while processing video {id=}: {e}')
            
        df_video_data = pd.DataFrame(videos)
        return df_video_data
        

    def save_video_data(self, df_video_data, csv_path='data/raw_video_data.csv'):
        print('Checking if directory exists...')
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        # Check if CSV already exists. If not creates a new one
        print('Checking if video CSV already exists and updating...')
        if os.path.exists(csv_path):
            existing_csv = pd.read_csv(csv_path)
            updated_csv = pd.concat([existing_csv, df_video_data], ignore_index=True)
            updated_csv = updated_csv.drop_duplicates(subset='title', keep='last', ignore_index=True)
            updated_csv.to_csv(csv_path, index=False)
        else:
            df_video_data.to_csv(csv_path, index=False)


def main():
    username = str(input('Select any youtube channel: '))
    
    scraper = YoutubeScraper()

    channel = scraper.find_channel_id(username)
    channel_data = scraper.get_channel_data(channel)
    scraper.save_channel_data(channel_data)

    videos_df = scraper.get_video_data(channel_data)
    scraper.save_video_data(videos_df)

main()