import requests
import pandas as pd
import pytz
import os
from datetime import datetime
from googleapiclient.discovery import build
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# API Keys and Database IDs from environment variables
api_key = os.getenv('YOUTUBE_API_KEY')
notion_api_key = os.getenv('NOTION_API_KEY')
video_database_id = os.getenv('VIDEO_DATABASE_ID')
channel_database_id = os.getenv('CHANNEL_DATABASE_ID')

# Validate that all required environment variables are set
if not all([api_key, notion_api_key, video_database_id, channel_database_id]):
    logger.error("Missing required environment variables. Please check your .env file.")
    exit(1)

# YouTube API setup
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

# Constants
YOUTUBE_BATCH_SIZE = 50  # YouTube API limit
NOTION_API_VERSION = '2022-06-28'


def convert_duration(duration):
    """Convert YouTube video duration format to human-readable format"""
    try:
        duration_obj = pd.to_timedelta(duration)
        hours, minutes, seconds = (
            duration_obj.components.hours,
            duration_obj.components.minutes,
            duration_obj.components.seconds
        )
        
        duration_str = ""
        if hours > 0:
            duration_str += f"{hours} hours "
        if minutes > 0:
            duration_str += f"{minutes} mins "
        if seconds > 0:
            duration_str += f"{seconds} secs"
        
        return duration_str.strip() if duration_str else "0s"
    except Exception as e:
        logger.error(f"Error converting duration '{duration}': {e}")
        return "Unknown"


def get_video_stats(youtube, video_ids):
    """
    Retrieves video statistics from YouTube API and processes the data.
    Handles batching for large lists of video IDs.
    """
    all_data = []
    
    # Process in batches of 50 (YouTube API limit)
    for i in range(0, len(video_ids), YOUTUBE_BATCH_SIZE):
        batch = video_ids[i:i + YOUTUBE_BATCH_SIZE]
        
        try:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(batch)
            )
            response = request.execute()
            
            if not response.get('items'):
                logger.warning(f"No videos found for batch: {batch}")
                continue
            
            # Process each video item in the API response
            for item in response['items']:
                try:
                    # Parse and format video published date
                    published_at = parse_and_format_published_date(item['snippet']['publishedAt'])
                    
                    # Get the highest resolution thumbnail URL
                    thumbnail_url = get_thumbnail_url(item['snippet']['thumbnails'])
                    
                    # Get human-readable video duration
                    duration_human_readable = convert_duration(item['contentDetails']['duration'])
                    
                    # Extract relevant video information
                    video_data = {
                        'Name': item['snippet']['title'],
                        'Video Id': item['id'],
                        'Date': published_at,
                        'Channel': item['snippet']['channelTitle'],
                        'Channel Id': item['snippet']['channelId'],
                        'Duration': duration_human_readable,
                        'Thumbnail': thumbnail_url,
                        'Category Id': item['snippet']['categoryId'],
                        'URL': f"https://www.youtube.com/watch?v={item['id']}",
                        'Channel Custom URL': None,
                        'Channel Logo URL': None,
                        'Category Name': None
                    }
                    
                    # Retrieve channel details
                    channel_data = get_channel_details(youtube, item['snippet']['channelId'])
                    if channel_data:
                        video_data.update(channel_data)
                    
                    # Retrieve category name
                    category_name = get_category_name(youtube, item['snippet']['categoryId'])
                    if category_name:
                        video_data['Category Name'] = category_name
                    
                    all_data.append(video_data)
                    
                except Exception as e:
                    logger.error(f"Error processing video {item.get('id', 'unknown')}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"YouTube API error for batch {i//YOUTUBE_BATCH_SIZE + 1}: {e}")
            continue
    
    return all_data


def parse_and_format_published_date(published_at):
    """Parses and formats the video published date."""
    try:
        published_at_obj = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
        published_at_formatted = published_at_obj.astimezone(
            pytz.timezone('Asia/Kolkata')
        ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        return published_at_formatted
    except Exception as e:
        logger.error(f"Error parsing date '{published_at}': {e}")
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')


def get_thumbnail_url(thumbnails):
    """Retrieves the highest resolution thumbnail URL from the available options."""
    max_resolution_url = thumbnails.get(
        'maxres',
        thumbnails.get(
            'standard',
            thumbnails.get(
                'high',
                thumbnails.get('medium', thumbnails.get('default'))
            )
        )
    )
    return max_resolution_url['url'] if max_resolution_url else None


def get_channel_details(youtube, channel_id):
    """Retrieves channel details from YouTube API."""
    try:
        request = youtube.channels().list(part="snippet,brandingSettings", id=channel_id)
        response = request.execute()
        
        if 'items' in response and len(response['items']) > 0:
            channel_snippet = response['items'][0]['snippet']
            
            # Get channel custom URL
            channel_custom_url = channel_snippet.get('customUrl', None)
            full_channel_url = f"https://www.youtube.com/{channel_custom_url}" if channel_custom_url else None
            
            # Get channel logo URL
            channel_thumbnail = channel_snippet['thumbnails']
            max_resolution_logo_url = channel_thumbnail.get(
                'high',
                channel_thumbnail.get('medium', channel_thumbnail.get('default'))
            )
            channel_logo_url = max_resolution_logo_url['url'] if max_resolution_logo_url else None
            
            return {
                'Channel Custom URL': full_channel_url,
                'Channel Logo URL': channel_logo_url
            }
    except Exception as e:
        logger.error(f"Error fetching channel details for {channel_id}: {e}")
    
    return {
        'Channel Custom URL': None,
        'Channel Logo URL': None
    }


def get_category_name(youtube, category_id):
    """Retrieves category name based on category ID from YouTube API."""
    try:
        request = youtube.videoCategories().list(part="snippet", id=category_id)
        response = request.execute()
        
        if 'items' in response and len(response['items']) > 0:
            return response['items'][0]['snippet']['title']
    except Exception as e:
        logger.error(f"Error fetching category name for {category_id}: {e}")
    
    return None


def create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url, channel_database_id):
    """Create a new channel entry in Notion database"""
    notion_url = 'https://api.notion.com/v1/pages'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    properties = {
        'Name': {
            'title': [
                {
                    'text': {
                        'content': channel_name[:2000]  # Notion title limit
                    }
                }
            ]
        },
        'Channel Id': {
            'rich_text': [
                {
                    'text': {
                        'content': channel_id
                    }
                }
            ]
        }
    }
    
    # Only add URL if it exists
    if channel_custom_url:
        properties['URL'] = {'url': channel_custom_url}
    
    payload = {
        'parent': {
            'database_id': channel_database_id
        },
        'properties': properties
    }
    
    # Only add icon if logo URL exists
    if channel_logo_url:
        payload['icon'] = {
            'type': 'external',
            'external': {
                'url': channel_logo_url
            }
        }
    
    try:
        response = requests.post(notion_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f'Channel entry for "{channel_name}" created successfully!')
            return response.json().get('id')
        else:
            logger.error(f'Failed to create channel entry for "{channel_name}": {response.json()}')
            return None
    except Exception as e:
        logger.error(f'Exception creating channel entry for "{channel_name}": {e}')
        return None


def check_if_channel_exists(channel_id, channel_database_id):
    """Check if channel already exists in Notion database using Channel ID"""
    notion_url = f'https://api.notion.com/v1/databases/{channel_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    payload = {
        "filter": {
            "property": "Channel Id",
            "rich_text": {
                "equals": channel_id
            }
        }
    }
    
    try:
        response = requests.post(notion_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('results') and len(data['results']) > 0:
                return data['results'][0]['id']
            return None
        else:
            logger.error(f'Failed to check if channel "{channel_id}" exists: {response.json()}')
            return None
    except Exception as e:
        logger.error(f'Exception checking channel existence for "{channel_id}": {e}')
        return None


def get_existing_channel_data(existing_channel_id):
    """Fetch existing channel data from Notion"""
    notion_url = f'https://api.notion.com/v1/pages/{existing_channel_id}'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    try:
        response = requests.get(notion_url, headers=headers)
        
        if response.status_code == 200:
            existing_data = response.json().get('properties', {})
            data_dict = {
                'Name': existing_data.get('Name', {}).get('title', [{}])[0].get('text', {}).get('content', '') if existing_data.get('Name', {}).get('title') else '',
                'Channel Id': existing_data.get('Channel Id', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') if existing_data.get('Channel Id', {}).get('rich_text') else '',
                'URL': existing_data.get('URL', {}).get('url', '') if existing_data.get('URL', {}) else ''
            }
            return data_dict
        else:
            logger.error(f'Failed to fetch existing channel data: {response.json()}')
            return None
    except Exception as e:
        logger.error(f'Exception fetching existing channel data: {e}')
        return None


def update_channel_entry(existing_channel_id, channel_name, channel_id, channel_logo_url, channel_custom_url):
    """Update existing channel entry if data has changed"""
    existing_data = get_existing_channel_data(existing_channel_id)
    
    if not existing_data:
        return
    
    # Check if update is needed
    update_required = False
    if (existing_data.get('Name') != channel_name or 
        existing_data.get('URL') != channel_custom_url or
        existing_data.get('Channel Id') != channel_id):
        update_required = True
    
    if not update_required:
        logger.info(f'Channel "{channel_name}" is already up to date')
        return
    
    notion_url = f'https://api.notion.com/v1/pages/{existing_channel_id}'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    properties = {
        'Name': {
            'title': [
                {
                    'text': {
                        'content': channel_name[:2000]
                    }
                }
            ]
        },
        'Channel Id': {
            'rich_text': [
                {
                    'text': {
                        'content': channel_id
                    }
                }
            ]
        }
    }
    
    # Only add URL if it exists
    if channel_custom_url:
        properties['URL'] = {'url': channel_custom_url}
    
    payload = {
        'properties': properties
    }
    
    # Only add icon if logo URL exists
    if channel_logo_url:
        payload['icon'] = {
            'type': 'external',
            'external': {
                'url': channel_logo_url
            }
        }
    
    try:
        response = requests.patch(notion_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f'Channel entry for "{channel_name}" updated successfully!')
        else:
            logger.error(f'Failed to update channel entry for "{channel_name}": {response.json()}')
    except Exception as e:
        logger.error(f'Exception updating channel entry for "{channel_name}": {e}')


def get_or_create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url, channel_database_id):
    """Get existing channel or create new one, with update capability"""
    existing_channel_id = check_if_channel_exists(channel_id, channel_database_id)
    
    if existing_channel_id:
        # Update the channel if it exists
        update_channel_entry(existing_channel_id, channel_name, channel_id, channel_logo_url, channel_custom_url)
        return existing_channel_id
    else:
        # Create new channel if it doesn't exist
        return create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url, channel_database_id)


def check_if_video_exists(video_id, video_database_id):
    """Check if a video entry already exists in Notion database"""
    notion_url = f'https://api.notion.com/v1/databases/{video_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    query = {
        "filter": {
            "property": "Video Id",
            "rich_text": {
                "equals": video_id  # Changed from "contains" to "equals" for exact match
            }
        }
    }
    
    try:
        response = requests.post(notion_url, headers=headers, json=query)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('results') and len(data['results']) > 0:
                return data['results'][0]['id']
            return None
        else:
            logger.error(f'Failed to check if video "{video_id}" exists: {response.json()}')
            return None
    except Exception as e:
        logger.error(f'Exception checking video existence for "{video_id}": {e}')
        return None


def get_existing_video_data(existing_video_id):
    """Fetch existing video data from Notion"""
    notion_url = f'https://api.notion.com/v1/pages/{existing_video_id}'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    try:
        response = requests.get(notion_url, headers=headers)
        
        if response.status_code == 200:
            existing_data = response.json().get('properties', {})
            data_dict = {
                'Name': existing_data.get('Name', {}).get('title', [{}])[0].get('text', {}).get('content', '') if existing_data.get('Name', {}).get('title') else '',
                'Date': existing_data.get('Date', {}).get('date', {}).get('start', '') if existing_data.get('Date', {}).get('date') else '',
                'Duration': existing_data.get('Duration', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') if existing_data.get('Duration', {}).get('rich_text') else '',
                'Thumbnail': existing_data.get('Thumbnail', {}).get('url', '') if existing_data.get('Thumbnail', {}) else '',
                'URL': existing_data.get('URL', {}).get('url', '') if existing_data.get('URL', {}) else '',
                'Category Id': existing_data.get('Category Id', {}).get('select', {}).get('name', '') if existing_data.get('Category Id', {}).get('select') else '',
                'Category Name': existing_data.get('Category Name', {}).get('select', {}).get('name', '') if existing_data.get('Category Name', {}).get('select') else '',
                'Channel': existing_data.get('Channel', {}).get('relation', [{}])[0].get('id', '') if existing_data.get('Channel', {}).get('relation') else ''
            }
            return data_dict
        else:
            logger.error(f'Failed to fetch existing video data: {response.json()}')
            return None
    except Exception as e:
        logger.error(f'Exception fetching existing video data: {e}')
        return None


def update_video_entry(existing_video_id, data):
    """Update existing video entry in Notion database if new data differs"""
    existing_data = get_existing_video_data(existing_video_id)
    
    if not existing_data:
        return
    
    # Check if update is needed
    update_required = False
    for key, value in data.items():
        if key in existing_data and existing_data[key] != value:
            existing_data[key] = value
            update_required = True
    
    if not update_required:
        logger.info(f'Video "{data["Name"][:50]}..." is already up to date')
        return
    
    notion_url = f'https://api.notion.com/v1/pages/{existing_video_id}'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    properties = {
        'Name': {
            'title': [
                {
                    'text': {
                        'content': existing_data['Name'][:2000]
                    }
                }
            ]
        },
        'Video Id': {
            'rich_text': [
                {
                    'text': {
                        'content': data['Video Id']
                    }
                }
            ]
        },
        'Date': {
            'date': {
                'start': existing_data['Date']
            }
        },
        'Duration': {
            'rich_text': [
                {
                    'text': {
                        'content': data.get('Duration', '')
                    }
                }
            ]
        },
        'Category Id': {
            'select': {
                'name': data.get('Category Id', '')
            }
        },
        'Category Name': {
            'select': {
                'name': data.get('Category Name', '')
            }
        }
    }
    
    # Only add URL properties if they exist
    if data.get('Thumbnail'):
        properties['Thumbnail'] = {'url': data.get('Thumbnail')}
    
    if data.get('URL'):
        properties['URL'] = {'url': data.get('URL')}
    
    payload = {
        'properties': properties
    }
    
    # Only add cover if thumbnail exists
    if data.get('Thumbnail'):
        payload['cover'] = {
            'type': 'external',
            'external': {
                'url': data.get('Thumbnail')
            }
        }
    
    try:
        response = requests.patch(notion_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f'Video entry for "{existing_data["Name"][:50]}..." updated successfully!')
        else:
            logger.error(f'Failed to update video entry: {response.json()}')
    except Exception as e:
        logger.error(f'Exception updating video entry: {e}')


def add_data_to_notion(data, channel_entry_id):
    """Add data to Notion video database with a relation to the channel database"""
    video_id = data['Video Id']
    existing_video_id = check_if_video_exists(video_id, video_database_id)
    
    if existing_video_id:
        update_video_entry(existing_video_id, data)
        return
    
    notion_url = 'https://api.notion.com/v1/pages'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION
    }
    
    properties = {
        'Name': {
            'title': [
                {
                    'text': {
                        'content': data['Name'][:2000]
                    }
                }
            ]
        },
        'Video Id': {
            'rich_text': [
                {
                    'text': {
                        'content': data['Video Id']
                    }
                }
            ]
        },
        'Date': {
            'date': {
                'start': data['Date']
            }
        },
        'Duration': {
            'rich_text': [
                {
                    'text': {
                        'content': data.get('Duration', '')
                    }
                }
            ]
        },
        'Category Id': {
            'select': {
                'name': data.get('Category Id', '')
            }
        },
        'Category Name': {
            'select': {
                'name': data.get('Category Name', '')
            }
        }
    }
    
    # Only add URL properties if they exist
    if data.get('Thumbnail'):
        properties['Thumbnail'] = {'url': data.get('Thumbnail')}
    
    if data.get('URL'):
        properties['URL'] = {'url': data.get('URL')}
    
    # Add the relation property to link the video entry with the channel entry
    if channel_entry_id:
        properties['Channel'] = {
            'relation': [
                {
                    'id': channel_entry_id
                }
            ]
        }
    
    payload = {
        'parent': {
            'database_id': video_database_id
        },
        'properties': properties
    }
    
    # Only add cover if thumbnail exists
    if data.get('Thumbnail'):
        payload['cover'] = {
            'type': 'external',
            'external': {
                'url': data.get('Thumbnail')
            }
        }
    
    try:
        response = requests.post(notion_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info(f'Data for video "{data["Name"][:50]}..." added to Notion database successfully!')
        else:
            logger.error(f'Failed to add data for video "{data["Name"][:50]}...": {response.json()}')
    except Exception as e:
        logger.error(f'Exception adding video to Notion: {e}')


def get_video_data(input_option=None, video_ids=None, file_path=None):
    """Get video data based on input option"""
    if input_option == "csv":
        if not file_path:
            logger.error("File path is required for CSV input option.")
            return None
        
        try:
            df = pd.read_csv(file_path)
            if 'Video Id' not in df.columns:
                logger.error("CSV file must contain a 'Video Id' column")
                return None
            
            video_ids = df['Video Id'].dropna().astype(str).tolist()
            
            if not video_ids:
                logger.error("No valid video IDs found in CSV file")
                return None
                
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return None
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return None
            
    elif input_option == "manual":
        user_input = input("Enter video IDs separated by commas: ").strip()
        if not user_input:
            logger.error("No video IDs provided")
            return None
        
        video_ids = [vid.strip() for vid in user_input.split(',') if vid.strip()]
        
        if not video_ids:
            logger.error("No valid video IDs found in input")
            return None
    else:
        logger.error("Invalid input option. Choose 'csv' or 'manual'")
        return None
    
    logger.info(f"Fetching data for {len(video_ids)} videos...")
    video_data = get_video_stats(youtube, video_ids)
    
    if not video_data:
        logger.warning("No video data retrieved from YouTube API")
        return None
    
    logger.info(f"Successfully fetched data for {len(video_data)} videos")
    return video_data


def main():
    """Main execution function"""
    logger.info("Starting YouTube to Notion sync...")
    
    # Get input option
    input_option = input("Choose input option (csv/manual): ").strip().lower()
    
    file_path = None
    if input_option == "csv":
        file_path = input("Enter the path to the CSV file: ").strip()
    
    # Get video data
    video_data = get_video_data(input_option=input_option, file_path=file_path)
    
    if not video_data:
        logger.error("No video data to process. Exiting.")
        return
    
    # Process each video
    success_count = 0
    error_count = 0
    
    for video_info in video_data:
        try:
            channel_name = video_info['Channel']
            channel_id = video_info['Channel Id']
            channel_logo_url = video_info['Channel Logo URL']
            channel_custom_url = video_info['Channel Custom URL']
            
            # Get or create channel entry
            channel_entry_id = get_or_create_channel_entry(
                channel_name,
                channel_id,
                channel_logo_url,
                channel_custom_url,
                channel_database_id
            )
            
            if not channel_entry_id:
                logger.warning(f"Skipping video '{video_info['Name'][:50]}...' - failed to get/create channel")
                error_count += 1
                continue
            
            # Add video to Notion
            add_data_to_notion(video_info, channel_entry_id)
            success_count += 1
            
        except Exception as e:
            logger.error(f"Error processing video '{video_info.get('Name', 'Unknown')[:50]}...': {e}")
            error_count += 1
            continue
    
    # Summary
    logger.info("=" * 50)
    logger.info(f"Sync completed!")
    logger.info(f"Successfully processed: {success_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Total videos: {len(video_data)}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
