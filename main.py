import requests
import pandas as pd
import pytz
from datetime import datetime
from googleapiclient.discovery import build
import logging

# YouTube API Key
api_key = 'YOUR_YOUTUBE_API_KEY'

# Notion API Key and Database IDs
notion_api_key = 'YOUR_NOTION_API_KEY'
video_database_id = 'YOUR_NOTION_VIDEO_DATABASE_ID'
channel_database_id = 'YOUR_NOTION_CHANNEL_DATABASE_ID'

# YouTube API setup
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

# Function to convert YouTube video duration format to human-readable format
def convert_duration(duration):
    # Parse duration string
    duration_obj = pd.to_timedelta(duration)
    # Extract hours, minutes, and seconds
    hours, minutes, seconds = duration_obj.components.hours, duration_obj.components.minutes, duration_obj.components.seconds
    # Create human-readable duration string
    duration_str = ""
    if hours > 0:
        duration_str += f"{hours}h"
    if minutes > 0:
        duration_str += f"{minutes}m"
    if seconds > 0:
        duration_str += f"{seconds}s"
    return duration_str.strip()

def get_video_stats(youtube, video_ids):
    """
    Retrieves video statistics from YouTube API and processes the data.

    Args:
        youtube (googleapiclient.discovery.Resource): YouTube API resource object.
        video_ids (list): List of video IDs to retrieve statistics for.

    Returns:
        list: List of dictionaries containing video statistics.
    """
    all_data = []

    # Prepare and execute API request to get video details
    request = youtube.videos().list(part="snippet,contentDetails,statistics", id=','.join(video_ids))
    response = request.execute()

    # Process each video item in the API response
    for item in response['items']:
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
            #'Tags': item['snippet'].get('tags', []),
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

        # Append processed video data to the list
        all_data.append(video_data)

    return all_data

def parse_and_format_published_date(published_at):
    """
    Parses and formats the video published date.

    Args:
        published_at (str): Published date string from YouTube API response.

    Returns:
        str: Formatted published date string.
    """
    # Parse publishedAt string to datetime object
    published_at_obj = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')

    # Set the UTC timezone and convert to the desired format
    published_at_formatted = published_at_obj.astimezone(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    return published_at_formatted

def get_thumbnail_url(thumbnails):
    """
    Retrieves the highest resolution thumbnail URL from the available options.

    Args:
        thumbnails (dict): Dictionary containing available thumbnail URLs.

    Returns:
        str: URL of the highest resolution thumbnail.
    """
    max_resolution_url = thumbnails.get('maxres', thumbnails.get('standard', thumbnails.get('high', thumbnails.get('medium'))))
    return max_resolution_url['url'] if max_resolution_url else None

def get_channel_details(youtube, channel_id):
    """
    Retrieves channel details from YouTube API.

    Args:
        youtube (googleapiclient.discovery.Resource): YouTube API resource object.
        channel_id (str): Channel ID to retrieve details for.

    Returns:
        dict: Dictionary containing channel details.
    """
    request = youtube.channels().list(part="snippet,brandingSettings", id=channel_id)
    response = request.execute()
    if 'items' in response and len(response['items']) > 0:
        channel_snippet = response['items'][0]['snippet']
        branding_settings = response['items'][0].get('brandingSettings', {})
        
        # Get channel custom URL
        channel_custom_url = channel_snippet.get('customUrl', None)
        full_channel_url = f"https://www.youtube.com/{channel_custom_url}" if channel_custom_url else None
        
        # Get channel logo URL
        channel_thumbnail = channel_snippet['thumbnails']
        max_resolution_logo_url = channel_thumbnail.get('high', channel_thumbnail.get('medium', channel_thumbnail.get('default')))
        channel_logo_url = max_resolution_logo_url['url'] if max_resolution_logo_url else None
        
        return {
            'Channel Custom URL': full_channel_url,
            'Channel Logo URL': channel_logo_url
        }
    else:
        return {
            'Channel Custom URL': None,
            'Channel Logo URL': None
        }

def get_category_name(youtube, category_id):
    """
    Retrieves category name based on category ID from YouTube API.

    Args:
        youtube (googleapiclient.discovery.Resource): YouTube API resource object.
        category_id (str): Category ID to retrieve name for.

    Returns:
        str: Category name.
    """
    request = youtube.videoCategories().list(part="snippet", id=category_id)
    response = request.execute()
    if 'items' in response and len(response['items']) > 0:
        return response['items'][0]['snippet']['title']
    else:
        return None


# Function to create a new channel entry in Notion database
def create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url,channel_database_id):
    notion_url = f'https://api.notion.com/v1/pages'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': '2021-08-16'  # Check Notion API version
    }

    properties = {
        'Name': {
            'title': [
                {
                    'text': {
                        'content': channel_name
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
        },
        # 'Logo': {
        #     'url': channel_logo_url
        # },
        'URL': {
            'url': channel_custom_url
        }
        # Add other properties as needed for your channel entries
    }
    icon = {
        'type': 'external',
        'external': {
            'url': channel_logo_url
        }
    }

    payload = {
        'parent': {
            'database_id': channel_database_id
        },
        'properties': properties,
        'icon': icon
    }

    response = requests.post(notion_url, headers=headers, json=payload)

    if response.status_code == 200:
        print(f'Channel entry for "{channel_name}" created successfully!')
        return response.json().get('id')  # Return the ID of the created channel entry
    else:
        print(f'Failed to create channel entry for "{channel_name}" in Notion database.')
        print(response.json())
        return None

# Function to check if channel already exists in Notion database using Channel ID
def check_if_channel_exists(channel_id, channel_database_id):
    notion_url = f'https://api.notion.com/v1/databases/{channel_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': '2021-08-16'
    }

    filter_conditions = {
        "property": "Channel Id",  # Assuming "Channel Id" is the property in your Notion database for channel IDs
        "text": {
            "equals": channel_id
        }
    }

    payload = {
        "filter": {
            "or": [filter_conditions]
        }
    }

    response = requests.post(notion_url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('results') and len(data['results']) > 0:
            # Channel found, return its ID
            return data['results'][0]['id']
        else:
            # Channel not found
            return None
    else:
        print(f'Failed to check if channel "{channel_id}" exists in Notion database.')
        print(response.json())
        return None
        
def get_or_create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url, channel_database_id):
    existing_channel_id = check_if_channel_exists(channel_id, channel_database_id)
    
    if existing_channel_id:
        # Channel already exists, return the existing channel entry ID
        return existing_channel_id
    else:
        # Channel does not exist, create a new channel entry and return the new channel entry ID
        return create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url, channel_database_id)

# Function to check if a video entry already exists in Notion database
def check_if_video_exists(video_id, video_database_id):
    notion_url = f'https://api.notion.com/v1/databases/{video_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': '2021-08-16'
    }

    # Prepare the query to search for the video by Video Id
    query = {
        "filter": {
            "property": "Video Id",  # Replace with the actual property name in your Notion database for video IDs
            "rich_text": {
                "contains": video_id
            }
        }
    }

    response = requests.post(notion_url, headers=headers, json=query)

    if response.status_code == 200:
        data = response.json()
        if data.get('results') and len(data['results']) > 0:
            # Video found, return its ID
            return data['results'][0]['id']
        else:
            # Video not found
            return None
    else:
        print(f'Failed to check if video "{video_id}" exists in Notion database.')
        print(response.json())
        return None

# Function to update existing video entry in Notion database if new data differs
def update_video_entry(existing_video_id, data):
    # Fetch existing data from Notion for comparison
    existing_data = get_existing_video_data(existing_video_id)

    # Compare existing data with new data to identify differences
    update_required = False
    for key, value in data.items():
        if key in existing_data and existing_data[key] != value:
            existing_data[key] = value
            update_required = True

    # If update is required, perform the update
    if update_required:
        notion_url = f'https://api.notion.com/v1/pages/{existing_video_id}'
        headers = {
            'Authorization': f'Bearer {notion_api_key}',
            'Content-Type': 'application/json',
            'Notion-Version': '2021-08-16'
        }

        # Prepare data for updating the video entry in Notion
        properties = {
            'Name': {
                'title': [
                    {
                        'text': {
                            'content': existing_data['Name']
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
            # 'Tags': {
            #     'multi_select': [
            #         {
            #             'name': tag
            #         } for tag in existing_data['Tags']
            #     ]
            # },
            'Duration': {
                'rich_text': [
                    {
                        'text': {
                            'content': data.get('Duration', '')
                        }
                    }
                ]
            },
            'Thumbnail': {
                'url': data.get('Thumbnail', '')  # Provide a valid URL string here
            },
            'URL': {
                'url': data.get('URL', '')  # Provide a valid URL string here
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
            # Add other properties based on your Notion video database schema that need updating
        cover = {
            'type': 'external',
            'external': {
                'url': data.get('Thumbnail', '')  # Provide a valid URL string here for the cover image
            }
        }

        # # Extract tags from data and join them into a comma-separated string
        # tags = ', '.join(data['Tags'])

        
        
        # # Create a single children block for tags as a comma-separated text
        # tag_child = {
        #     'object': 'block',
        #     'type': 'paragraph',
        #     'paragraph': {
        #         'text': [
        #             {
        #                 'type': 'text',
        #                 'text': {
        #                     'content': tags
        #                 }
        #             }
        #         ]
        #     }
        # }
        
        # # Initialize children as a list containing the tag child block
        # children = [tag_child]
        payload = {
            'properties': properties,
            'cover': cover,
            #'children': children
        }

        response = requests.patch(notion_url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f'Video entry for "{existing_data["Name"]}" updated successfully in Notion database!')
        else:
            print(f'Failed to update video entry for "{existing_data["Name"]}" in Notion database.')
            print(response.json())

# Function to fetch existing video data from Notion
def get_existing_video_data(existing_video_id):
    notion_url = f'https://api.notion.com/v1/pages/{existing_video_id}'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': '2021-08-16'
    }

    response = requests.get(notion_url, headers=headers)

    if response.status_code == 200:
        existing_data = response.json().get('properties', {})
        # Convert Notion properties to a dictionary
        data_dict = {
            'Name': existing_data.get('Name', {}).get('title', [{}])[0].get('text', {}).get('content', '') if existing_data.get('Name', {}).get('title') else '',
            'Date': existing_data.get('Date', {}).get('date', {}).get('start', '') if existing_data.get('Date', {}).get('date') else '',
            #'Tags': [tag['name'] for tag in existing_data.get('Tags', {}).get('multi_select', [])] if existing_data.get('Tags', {}).get('multi_select') else [],
            'Duration': existing_data.get('Duration', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '') if existing_data.get('Duration', {}).get('rich_text') else '',
            'Thumbnail': existing_data.get('Thumbnail', {}).get('url', '') if existing_data.get('Thumbnail', {}) else '',
            'URL': existing_data.get('URL', {}).get('url', '') if existing_data.get('URL', {}) else '',
            'Category Id': existing_data.get('Category Id', {}).get('select', {}).get('name', '') if existing_data.get('Category Id', {}).get('select') else '',
            'Category Name': existing_data.get('Category Name', {}).get('select', {}).get('name', '') if existing_data.get('Category Name', {}).get('select') else '',
            'Channel': existing_data.get('Channel', {}).get('relation', [{}])[0].get('id', '') if existing_data.get('Channel', {}).get('relation') else ''
            #'Channel': existing_data.get('Channel', {}).get('relation', [{}])[0].get('id', '') if existing_data.get('Channel', {}).get('relation') else ''
        }
        return data_dict
    else:
        print(f'Failed to fetch existing video data for ID "{existing_video_id}" from Notion database.')
        print(response.json())
        return None


# Function to add data to Notion video database with a relation to the channel database
def add_data_to_notion(data, channel_entry_id):
    notion_url = f'https://api.notion.com/v1/pages'
    headers = {
        'Authorization': f'Bearer {notion_api_key}',
        'Content-Type': 'application/json',
        'Notion-Version': '2021-08-16'
    }

    video_id = data['Video Id']
    existing_video_id = check_if_video_exists(video_id, video_database_id)
    if existing_video_id:
        update_video_entry(existing_video_id, data)
    else:
        
        # Prepare data for the new video entry in Notion
        properties = {
            'Name': {
                'title': [
                    {
                        'text': {
                            'content': data['Name']
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
            # 'Tags': {
            #     'multi_select': [
            #         {
            #             'name': tag
            #         } for tag in data['Tags']
            #     ]
            # },
            'Duration': {
                'rich_text': [
                    {
                        'text': {
                            'content': data.get('Duration', '')
                        }
                    }
                ]
            },
            'Thumbnail': {
                'url': data.get('Thumbnail', '')  # Provide a valid URL string here
            },
            'URL': {
                'url': data.get('URL', '')  # Provide a valid URL string here
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
            
            #Add other properties based on your Notion video database schema
    
        # Add the relation property to link the video entry with the channel entry
        if channel_entry_id:
            properties['Channel'] = {
                'relation': [
                    {
                        'id': channel_entry_id
                    }
                ]
            } 

        cover = {
            'type': 'external',
            'external': {
                'url': data.get('Thumbnail', '')  # Provide a valid URL string here for the cover image
            }
        }

        
        # # Extract tags from data and join them into a comma-separated string
        # tags = ', '.join(data['Tags'])

        
        
        # # Create a single children block for tags as a comma-separated text
        # tag_child = {
        #     'object': 'block',
        #     'type': 'paragraph',
        #     'paragraph': {
        #         'text': [
        #             {
        #                 'type': 'text',
        #                 'text': {
        #                     'content': tags
        #                 }
        #             }
        #         ]
        #     }
        # }
        
        # # Initialize children as a list containing the tag child block
        # children = [tag_child]


        payload = {
            'parent': {
                'database_id': video_database_id
            },
            'properties': properties,
            'cover': cover,
            #'children': children
            
        }
        
        response = requests.post(notion_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            print(f'Data for video "{data["Name"]}" added to Notion database successfully!')
        else:
            print(f'Failed to add data for video "{data["Name"]}" to Notion database.')
            print(response.json())

def get_video_data(input_option=None, video_ids=None, file_path=None):
    if input_option == "csv":
        if file_path:
            # Read video IDs from the specified CSV file
            try:
                df = pd.read_csv(file_path)
                video_ids = df['Video Id'].tolist()
            except FileNotFoundError:
                print("File not found. Please check the file path.")
                return
        else:
            print("File path is required for CSV input option.")
            return
    elif input_option == "manual":
        # Get video IDs from user input
        user_input = input("Enter video IDs separated by commas: ")
        video_ids = user_input.split(',')
    else:
        print("Invalid input option.")
        return
    
    # Fetch video data using video IDs
    video_data = get_video_stats(youtube, video_ids)
    
    return video_data

# Example usage:
input_option = input("Choose input option (csv/manual): ")
if input_option == "csv":
    file_path = input("Enter the path to the CSV file: ")
else:
    file_path = None

video_data = get_video_data(input_option=input_option, file_path=file_path)

# Add video data to Notion video database with relation to the channel database
for video_info in video_data:
    channel_name = video_info['Channel']
    channel_id = video_info['Channel Id']
    channel_logo_url = video_info['Channel Logo URL']
    channel_custom_url = video_info['Channel Custom URL']
    channel_entry_id = get_or_create_channel_entry(channel_name, channel_id, channel_logo_url, channel_custom_url, channel_database_id)
    add_data_to_notion(video_info, channel_entry_id)
