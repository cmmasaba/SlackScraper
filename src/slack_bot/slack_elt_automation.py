import os                                               # Accessing env variables
from slack_bolt import App                              # Initializing the Slack client
from slack_sdk.errors import SlackApiError              # Error handling for errors from SLACK API
import json                                             # Reading and writing to JSON/JSONL files
from datetime import datetime, timedelta                # Calculating the date to append to file names
from time import sleep                                  # Suspending the bot when necessary
from pathlib import Path                                # Accessing files in storage
from google.cloud import storage                        # Interacting with Google Cloud Storage
from http. client import IncompleteRead                 # Error handling for unstable network conditions
from dotenv import load_dotenv                          # Handling environment variables
import requests                                         # Used for downloading files
import mimetypes                                        # Define the mime types of the expected files

class SlackScraper:
    def __init__(self) -> None:
        """
        Initialize the app.
        """
        load_dotenv()

        self.slack_bot_token = os.environ['SLACK_BOT_TOKEN']
        self.app = App(token=self.slack_bot_token)
        self.client = self.app.client
        self.checkpoint_file = Path('checkpoints.json')
        self.checkpoint_file.touch(exist_ok=True)
        self.read_channels = {}
        self.storage_client = storage.Client(project=os.environ['GCP_PROJECT'])
        self.storage_bucket = self.storage_client.bucket(os.environ['GCP_STORAGE_BUCKET'])
        self.last_checkpoint = 0
    
    def read_checkpoints(self, checkpoint_file: Path) -> dict:
        """
        Read the checkpoint to determine where to resume.
        Args:
            checkpoint_file: the path of the file where checkpoint data is stored.
        Returns:
            A list of channels that have been written, or an empty list if there's no checkpoints.
        """
        try:
            if checkpoint_file.exists():
                with checkpoint_file.open('r') as fp:
                    return json.load(fp)
            return {}
        except json.decoder.JSONDecodeError:
            return {}
    
    def write_checkpoint(self, checkpoint_file: Path, channel_name: str, message_number: int) -> None:
        """
        Write each channel name on a new line to the checkpoint file.
        Args:
            checkpoint_file: the path of the file where checkpoint data is stored.
            channel_name: the name of the channel being written.
            message_number: the number of the message being written.
        Returns:
            None
        """
        checkpoints = self.read_checkpoint(checkpoint_file)
        checkpoints[channel_name] = message_number
        with checkpoint_file.open('w') as fp:
            json.dump(checkpoints, fp, indent=4)
    
    def get_slack_workspace_members(self) -> None:
        """
        Retrieve the users in the Slack workspace and store the info in JSONL format.
        """
        users = self.client.users_list()

        Path(f'SlackDownloads/Users/').mkdir(parents=True, exist_ok=True)
        with open(f"SlackDownloads/Users/users_{datetime.today().strftime('%Y%m%d')}.jsonl", 'w') as fp:
            for user in users['members']:
                json.dump(user, fp)
                fp.write('\n')
        self.gcs_add_directory('users')
        self.gcs_add_file(f"SlackDownloads/Users/users_{datetime.today().strftime('%Y%m%d')}.jsonl", 'users')

    def directory_exists(self, directory_name) -> bool:
        """
        Check if directory_name is in the bucket.

        Args:
            bucket: the Google Cloud Storage to check in.
            directory_name: the name of the directory to search for.
        Returns:
            True if the directory name is in the bucket, otherwise False
        """
        directory_path = directory_name.rstrip('/') + '/'               # dir names must end with a /

        blobs = list(self.storage_bucket.list_blobs(prefix=directory_path, max_results=1))
        
        return len(blobs) > 0

    def gcs_add_directory(self, directory_name: str) -> bool:
        """
        Add an empty directory to the cloud storage bucket.
    
        Args:
            directory_name: the name of the directory to add.
        Returns:
            True to signal success.
        """
        if not self.directory_exists(directory_name):
            if directory_name[-1] != '/':
                directory_name = directory_name + '/'                   # dir names must end with a /

            blob = self.storage_bucket.blob(directory_name)
            blob.upload_from_string("", content_type="application/x-www-form-urlencoded:charset=UTF-8")
        return True
    
    def gcs_add_file(self, file_path, directory_name) -> str:
        """
        Add a file to the cloud storage bucket.
    
        Args:
            file_path: the path to the file.
            directory_name: the name of the GCS directory to upload the file to..
        Returns:
            a link to the file in Google Cloud Storage.
        """
        if directory_name[-1] != '/':
            directory_name = directory_name + '/'                       # dir names must end with a /
        
        blob = self.storage_bucket.blob(directory_name + os.path.basename(file_path))
        blob.upload_from_filename(file_path)
        return blob.self_link
    
    def get_private_slack_channel_ids(self) -> dict:
        """
        Get the private channel IDs and names from the Slack workspace and store the infor in JSON format.
        Returns:
            a dictionary of channel id and channel name the private channels in the workspace.
        """
        try:
            channels = {}
            for result in self.client.conversations_list(types="private_channel"):
                for channel in result["channels"]:
                    channels[channel["id"]] = channel['name']
            Path(f'SlackDownloads/Channels/').mkdir(parents=True, exist_ok=True)
            with open('SlackDownloads/Channels/private_channels.json', 'w') as fp:
                json.dump(channels, fp, indent=4)
            return channels
        except SlackApiError as e:
            print(f"Error: {e}")
    
    def get_public_slack_channel_ids(self) -> dict:
        """
        Get the public channel IDs and names from the Slack workspace and store the infor in JSON format.
        Returns:
            a dictionary of channel id and channel name the public channels in the workspace.
        """
        try:
            channels = {}
            for result in self.client.conversations_list(types="public_channel"):
                for channel in result["channels"]:
                    channels[channel["id"]] = channel['name']
            Path(f'SlackDownloads/Channels/').mkdir(parents=True, exist_ok=True)
            with open('SlackDownloads/Channels/public_channels.json', 'w') as fp:
                json.dump(channels, fp, indent=4)
            return channels
        except SlackApiError as e:
            print(f"Error: {e}")
    
    def get_slack_messages(self) -> bool:
        '''
        Download slack messages, threads and their related files.
        Returns:
            True if the download happens without error, else False.
        '''
        try:
            threaded_replies = []
            current_date = datetime.today().strftime('%Y%m%d')

            with open('SlackDownloads/Channels/private_channels.json', 'r') as fp:
                channels = json.load(fp)

            Path(f'SlackDownloads/Messages/').mkdir(parents=True, exist_ok=True)
            Path(f'SlackDownloads/Messages/slack_{current_date}.jsonl').touch(exist_ok=True)

            with open(f'SlackDownloads/Messages/slack_{current_date}.jsonl', 'a') as messages_fp:
                timestamp_since_last_backup = datetime.now() - timedelta(days=1)
                for channel_id, channel_name in channels.items():
                    messages = []
                    self.last_checkpoint = 0
                    print(channel_name)
                    print()

                    if channel_name in self.read_channels:
                        self.last_checkpoint = self.read_channels[channel_name]

                    conversation_history = self.client.conversations_history(
                                                channel=channel_id,
                                                oldest=timestamp_since_last_backup,
                                                limit=999,
                                            )
                    if conversation_history['ok']:
                        messages = conversation_history['messages']
                    print(f'Number of messages in {channel_name}: {len(messages)}', end='\n')

                    for message_number, message in enumerate(messages):
                        if self.last_checkpoint == len(messages):
                            message_number = self.last_checkpoint - 1
                            break
                        if message_number < self.last_checkpoint:
                            continue
                        print('Message: ', len(messages) - message_number, end='\n')
                        message['channel_name'] = channel_name
                        message['channel_id'] = channel_id

                        thread = self.client.conversations_replies(channel=channel_id, ts=message['ts'])
                        if thread['ok']:
                            threaded_replies = thread['messages']
                            for threaded_reply in threaded_replies:
                                file_paths = []
                                if threaded_reply.get('files'):
                                    self.gcs_add_directory(f'files/{current_date}/{channel_name}')
                                    for file in threaded_reply.get('files'):
                                        if file.get('url_private_download'):
                                            file_path = self.download_and_verify_slack_file(
                                                            file.get('url_private_download'),
                                                            f'SlackDownloads/Files/{current_date}/{channel_name}'
                                                        )
                                            if file_path:
                                                try:
                                                    file_storage_path = self.gcs_add_file(file_path, f'files/{current_date}/{channel_name}')
                                                    file_paths.append({
                                                        'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                        'filename': file.get('name'),
                                                        'storage_url': file_storage_path
                                                    })
                                                except (TimeoutError, ConnectionError, Exception):
                                                    continue
                                threaded_reply['files'] = file_paths

                        if message:
                            file_paths = []
                            if message.get('files'):
                                self.gcs_add_directory(f'files/{current_date}/{channel_name}')
                                for file in message.get('files'):
                                    if file.get('url_private_download'):
                                        file_path = self.download_and_verify_slack_file(
                                                        file.get('url_private_download'),
                                                        f'SlackDownloads/Files/{current_date}/{channel_name}'
                                                    )
                                        if file_path:
                                            try:
                                                file_storage_path = self.gcs_add_file(file_path, f'files/{current_date}/{channel_name}')
                                                file_paths.append({
                                                    'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                    'filename': file.get('name'),
                                                    'storage_url': file_storage_path
                                                })
                                            except (TimeoutError, ConnectionError, Exception):
                                                continue
                            message['files'] = file_paths
                            message['threads'] = threaded_replies
                            json.dump(message, messages_fp)                 # Save in JSONL format
                            messages_fp.write('\n')
                    self.write_checkpoint(self.checkpoint_file, channel_name, message_number + 1)
                    message_number = 0                                      # In case it fails at the start of the next channel, message number should be zero
                    sleep(5)
                self.gcs_add_directory(f'messages/')
                self.gcs_add_file(f'messages/slack_{current_date}.jsonl', f'messages/')
            return True
        except (SlackApiError, IncompleteRead, KeyboardInterrupt, Exception) as e:
            print(f"Error: {e}")
            try:
                if message_number < self.last_checkpoint:
                    message_number = self.last_checkpoint
                self.write_checkpoint(self.checkpoint_file, channel_name, message_number)
            except UnboundLocalError:
                pass
            finally:
                return False
    
    def format_nested_json_fields(self, file_path: str) -> bool:
        '''
        Format deeply nested JSON fields as strings to make loading the data to 
        BigQuery easier. Rewrites the formatted JSON entries back to the given
        file.
        Args:
            file_path: the path to the JSONL file with data to be formatted.
        Returns:
            True if formatted with no errors, otherwise False.
        '''
        with open(file_path, 'r') as fp:
            data = []
            for linenumber, line in enumerate(fp):
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Failed on line {linenumber}")
                    print(e)
                    return False

        with open(file_path, 'w') as fp:
            for line in data:
                if not line.get('old_name'):
                    line['old_name'] = None
                if not line.get('name'):
                    line['name'] = None
                if not line.get('purpose'):
                    line['purpose'] = None
                if line.get("blocks"):
                    line['blocks'] = [str(line['blocks'])]
                if line.get('root') and line['root'].get('blocks'):
                    line['root']['blocks'] = [str(line['root']['blocks'])]
                if line.get('pinned_to'):
                    del line['pinned_to']
                if line.get('pinned_info'):
                    del line['pinned_info']
                if line.get('root') and line['root'].get('attachments'):
                    for attachment in line['root']['attachments']:
                        if attachment.get('blocks'):
                            attachment['blocks'] = [str(attachment['blocks'])]
                        if not attachment.get('thumb_url'):
                            attachment['thumb_url'] = None
                        if not attachment.get('thumb_width'):
                            attachment['thumb_width'] = None
                        if not attachment.get('thumb_height'):
                            attachment['thumb_height'] = None
                        if  not attachment.get('title'):
                            attachment['title'] = None
                        if not attachment.get('title_link'):
                            attachment['title_link'] = None
                        if not attachment.get('image_url'):
                            attachment['image_url'] = None
                        if not attachment.get('image_width'):                       
                            attachment['image_width'] = None
                        if not attachment.get('image_height'):                       
                            attachment['image_height'] = None
                        if not attachment.get('image_bytes'):                       
                            attachment['image_bytes'] = None
                        if not attachment.get('from_url'):                       
                            attachment['from_url'] = None
                        if not attachment.get('service_icon'):                       
                            attachment['service_icon'] = None
                        if not attachment.get('original_url'):                       
                            attachment['original_url'] = None
                        if not attachment.get('fallback'):                       
                            attachment['fallback'] = None
                        if not attachment.get('is_share'):                       
                            attachment['is_share'] = None
                        if not attachment.get('is_reply_unfurl'):                       
                            attachment['is_reply_unfurl'] = None
                        if not attachment.get('service_name'):                       
                            attachment['service_name'] = None
                        if attachment.get('message_blocks'):
                            attachment['message_blocks'] = [str(attachment['message_blocks'])]
                        if attachment.get('files'):
                            del attachment['files']
                if line.get('attachments'):
                    for attachment in line['attachments']:
                        if attachment.get('blocks'):
                            attachment['blocks'] = [str(attachment['blocks'])]
                        if not attachment.get('private_channel_prompt'):
                            attachment['private_channel_prompt'] = None
                        if attachment.get('message_blocks'):
                            attachment['message_blocks'] = [str(attachment['message_blocks'])]
                        if attachment.get('files'):
                            del attachment['files']
                        if not attachment.get('author_name'):
                            attachment['author_name'] = None
                        if not attachment.get('author_link'):
                            attachment['author_link'] = None
                        if not attachment.get('author_icon'):
                            attachment['author_icon'] = None
                        if not attachment.get('author_subname'):
                            attachment['author_subname'] = None
                        if attachment.get('mrkdwn_in'):
                            del attachment['mrkdwn_in']
                        if not attachment.get('fallback'):
                            attachment['fallback'] = None
                        if not attachment.get('original_url'):
                            attachment['original_url'] = None
                        if not attachment.get('from_url'):
                            attachment['from_url'] = None
                        if not attachment.get('is_msg_unfurl'):
                            attachment['is_msg_unfurl'] = None
                        if not attachment.get('is_animated'):
                            attachment['is_animated'] = None
                        if not attachment.get('author_id'):
                            attachment['author_id'] = None
                        if not attachment.get('channel_team'):
                            attachment['channel_team'] = None
                        if not attachment.get('channel_id'):
                            attachment['channel_id'] = None
                        if not attachment.get('footer_icon'):                       
                            attachment['footer_icon'] = None
                        if not attachment.get('footer'):                       
                            attachment['footer'] = None
                        if attachment.get('pinned_to'):
                            del item['pinned_to']
                        if attachment.get('pinned_info'):
                            del item['pinned_info']
                if line.get('root') and line['root'].get('files'):
                    del line['root']['files']


                if line.get('threads'):
                    for item in line['threads']:
                        if item.get("blocks"):
                            item['blocks'] = [str(item['blocks'])]
                        if not item.get('old_name'):
                            item['old_name'] = None
                        if not item.get('name'):
                            item['name'] = None
                        if not item.get('purpose'):
                            item['purpose'] = None
                        if item.get('pinned_to'):
                            del item['pinned_to']
                        if item.get('pinned_info'):
                            del item['pinned_info']
                        if item.get('root') and item['root'].get('attachments'):
                            for attachment in item['root']['attachments']:
                                if attachment.get('blocks'):
                                    attachment['blocks'] = [str(attachment['blocks'])]
                                if not attachment.get('thumb_url'):
                                    attachment['thumb_url'] = None
                                if not attachment.get('thumb_width'):
                                    attachment['thumb_width'] = None
                                if not attachment.get('thumb_height'):
                                    attachment['thumb_height'] = None
                                if  not attachment.get('title'):
                                    attachment['title'] = None
                                if not attachment.get('title_link'):
                                    attachment['title_link'] = None
                                if not attachment.get('image_url'):
                                    attachment['image_url'] = None
                                if not attachment.get('image_width'):                       
                                    attachment['image_width'] = None
                                if not attachment.get('image_height'):                       
                                    attachment['image_height'] = None
                                if not attachment.get('image_bytes'):                     
                                    attachment['image_bytes'] = None
                                if not attachment.get('from_url'):                       
                                    attachment['from_url'] = None
                                if not attachment.get('service_icon'):                       
                                    attachment['service_icon'] = None
                                if not attachment.get('original_url'):                       
                                    attachment['original_url'] = None
                                if not attachment.get('fallback'):                       
                                    attachment['fallback'] = None
                                if not attachment.get('is_share'):                       
                                    attachment['is_share'] = None
                                if not attachment.get('is_reply_unfurl'):                       
                                    attachment['is_reply_unfurl'] = None
                                if not attachment.get('service_name'):                       
                                    attachment['service_name'] = None
                                if attachment.get('message_blocks'):
                                    attachment['message_blocks'] = [str(attachment['message_blocks'])]
                                if attachment.get('files'):
                                    del attachment['files']
                        if item.get('root') and item['root'].get('blocks'):
                            item['root']['blocks'] = [str(item['root']['blocks'])]
                        if item.get('root') and item['root'].get('files'):
                            del item['root']['files']
                        if item.get('attachments'):
                            for attachment in item['attachments']:
                                if attachment.get('blocks'):
                                    attachment['blocks'] = [str(attachment['blocks'])]
                                if attachment.get('message_blocks'):
                                    attachment['message_blocks'] = [str(attachment['message_blocks'])]
                                if attachment.get('files'):
                                    del attachment['files']
                                if not attachment.get('private_channel_prompt'):
                                    attachment['private_channel_prompt'] = None
                                if not attachment.get('author_name'):
                                    attachment['author_name'] = None
                                if not attachment.get('author_link'):
                                    attachment['author_link'] = None
                                if not attachment.get('author_icon'):
                                    attachment['author_icon'] = None
                                if not attachment.get('author_subname'):
                                    attachment['author_subname'] = None
                                if attachment.get('mrkdwn_in'):
                                    del attachment['mrkdwn_in']
                                if not attachment.get('fallback'):
                                    attachment['fallback'] = None
                                if not attachment.get('original_url'):
                                    attachment['original_url'] = None
                                if not attachment.get('from_url'):
                                    attachment['from_url'] = None
                                if not attachment.get('is_msg_unfurl'):
                                    attachment['is_msg_unfurl'] = None
                                if not attachment.get('is_animated'):
                                    attachment['is_animated'] = None
                                if not attachment.get('author_id'):
                                    attachment['author_id'] = None
                                if not attachment.get('channel_team'):
                                    attachment['channel_team'] = None
                                if not attachment.get('channel_id'):
                                    attachment['channel_id'] = None
                                if not attachment.get('footer_icon'):                       
                                    attachment['footer_icon'] = None
                                if not attachment.get('footer'):                       
                                    attachment['footer'] = None
                                if attachment.get('pinned_to'):
                                    del item['pinned_to']
                                if attachment.get('pinned_info'):
                                    del item['pinned_info']
                json.dump(line, fp)
                fp.write('\n')

        return True
    
    def download_and_verify_file(self, file_url, storage_location='SlackDownloads') -> str:
        """
        Download files attached to messages and threads.

        Args:
            file_url: the download url of the file.
            storage_location: the location where to store the file.
        Returns:
            The path to where the file was stored.
        """
        # Download the file
        file_path = self.download_slack_file(file_url, storage_location)

        
        if file_path:
            # Verify the downloaded file
            if self.verify_file_content(file_path):
                print("✓ File downloaded and verified successfully\n")
            else:
                print("⚠ File may be corrupted or in unexpected format\n")
            return file_path
        else:
            print("✗ File download failed\n")
            return None

    def _verify_file_content(self, file_path) -> bool:
        """
        Verify if the file downloaded seems to be valid based on its content.
        Args:
            file_path: the path to the file to be verified.
        Returns:
            True if the file seems valid, otherwise False.
        """
        try:
            with open(file_path, 'rb') as f:
                # Read first few bytes to check file signature
                header = f.read(8)
                
            # Check common file signatures
            file_signatures = {
                b'%PDF': 'PDF file',
                b'\xFF\xD8\xFF': 'JPEG image',
                b'\x89PNG\r\n\x1A\n': 'PNG image',
                b'PK\x03\x04': 'ZIP archive',
                b'GIF87a': 'GIF image',
                b'GIF89a': 'GIF image',
            }
            
            for signature, _ in file_signatures.items():
                if header.startswith(signature):
                    return True
                    
            # If no signature match but file has content
            if len(header) > 0:
                return True
                
            return False
            
        except IOError as e:
            return False
    
    def download_file(self, file_url, save_dir='SlackDownloads') -> str:
        """
        Download a file from Slack API and save it to local storage.
        Args:
            file_url: the url to the file to be downloaded.
            save_dir: the directory where the file should be saved in.
        Returns:
            The file path to the downloaded file if successful, otherwise None.
        """
        try:
            # Setup headers with authentication
            headers = {
                "Authorization": f"Bearer {self.slack_bot_token}",
                "User-Agent": "SlackDownloader/1.0"
            }
            
            # Make the initial request
            response = requests.get(file_url, headers=headers, stream=True)
            response.raise_for_status()
            
            # Get content type and filename from headers
            content_type = response.headers.get('content-type', '').split(';')[0]
            content_disp = response.headers.get('content-disposition', '')
            
            # Try to get filename from content disposition
            if 'filename=' in content_disp:
                filename = content_disp.split('filename=')[-1].strip('"')
                filename = filename.split('";')[0]
            else:
                # Generate filename based on content type
                ext = mimetypes.guess_extension(content_type) or ''
                filename = f"slack_file{ext}"
            
            # Create save directory if it doesn't exist
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            save_path = self.get_next_filename(os.path.join(save_dir, filename))
            
            # Save the file in binary mode
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return save_path
            
        except requests.exceptions.RequestException as e:
            return None
    
    def get_next_filename(self, file_path) -> str:
        """
        Add incremental number to a file name it the name already exists.
        Example: file.txt, file(1).txt, file(2).txt, etc.
        Args:
            file_path: the name of the file.
        Returns:
            The new name of the file given.
        """
        if not os.path.exists(file_path):
            return file_path
        
        name, ext = os.path.splitext(file_path)
        counter = 1
        
        while os.path.exists(file_path):
            file_path = f"{name}({counter}){ext}"
            counter += 1
            
        return file_path
    
    def start(self,):
        # TODO: implement the process to start the bot
        pass