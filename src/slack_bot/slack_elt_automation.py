import os                                                   # Accessing env variables
from slack_bolt import App                                  # Initializing the Slack client
from slack_sdk.errors import SlackApiError                  # Error handling for errors from SLACK API
import json                                                 # Reading and writing to JSON/JSONL files
from datetime import datetime, timedelta, timezone, date    # Calculating the date to append to file names
from time import sleep                                      # Suspending the bot when necessary
from pathlib import Path                                    # Accessing files in storage
from google.cloud import storage, bigquery                  # Interacting with Google Cloud Storage
from http. client import IncompleteRead                     # Error handling for unstable network conditions
from dotenv import load_dotenv                              # Handling environment variables
import requests                                             # Used for downloading files
import mimetypes                                            # Define the mime types of the expected files
from util.logging import GclClient
import time
from typing import Dict, List, Any
import jsonschema

class SlackScraper:
    def __init__(self, save_to_cloud = True) -> None:
        """
        Initialize the app.
        """
        load_dotenv()

        self.slack_bot_token = os.environ['SLACK_BOT_TOKEN']
        self.app = App(token=self.slack_bot_token)
        self.client = self.app.client
        self.downloads_folder = Path('downloads')
        self.downloads_folder.mkdir(exist_ok=True)
        self.checkpoint_file = Path('downloads/checkpoints.json')
        self.checkpoint_file.touch(exist_ok=True)
        self.read_channels = {}
        self.storage_client = storage.Client(project=os.environ['GCP_PROJECT'])
        self.bigquery_client = bigquery.Client()
        self.storage_bucket = self.storage_client.bucket(os.environ['GCP_STORAGE_BUCKET'])
        self.last_checkpoint = 0
        self.save_to_cloud = save_to_cloud
        self.logger = GclClient().get_logger()

    def _read_checkpoints(self) -> dict:
        """
        Read the checkpoint to determine where to resume.
        Args:
            checkpoint_file: the path of the file where checkpoint data is stored.
        Returns:
            A list of channels that have been written, or an empty list if there's no checkpoints.
        """
        try:
            if self.checkpoint_file.exists():
                with self.checkpoint_file.open('r') as fp:
                    return json.load(fp)
            return {}
        except json.decoder.JSONDecodeError:
            return {}

    def _write_checkpoint(self,channel_name: str, message_number: int) -> None:
        """
        Write each channel name on a new line to the checkpoint file.
        Args:
            channel_name: the name of the channel being written.
            message_number: the number of the message being written.
        Returns:
            None
        """
        checkpoints = self._read_checkpoints()
        checkpoints[channel_name] = message_number
        with self.checkpoint_file.open('w') as fp:
            json.dump(checkpoints, fp, indent=4)

    def get_slack_workspace_members(self) -> None:
        """
        Retrieve the users in the Slack workspace and store the info in JSONL format.
        """
        is_call_successful = False
        while not is_call_successful:
            try:
                response = self.client.users_list()
                is_call_successful = True
            except SlackApiError as e:
                self.logger.error(f"[get_slack_workspace_members] Error: {e}")
                sleep(15)
            except IncompleteRead:
                self.logger.error("[get_slack_workspace_members] Unable to fetch Slack members, unstable network")

        Path(f'downloads/Users/').mkdir(parents=True, exist_ok=True)
        with open(f"downloads/Users/users_{datetime.today().strftime('%Y%m%d')}.jsonl", 'w') as fp:
            for user in response['members']:
                json.dump(user, fp)
                fp.write('\n')
        self._gcs_add_directory('users')
        self._gcs_add_file(f"downloads/Users/users_{datetime.today().strftime('%Y%m%d')}.jsonl", 'users')

    def _directory_exists(self, directory_name) -> bool:
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

    def _gcs_add_directory(self, directory_name: str) -> bool:
        """
        Add an empty directory to the cloud storage bucket.
    
        Args:
            directory_name: the name of the directory to add.

        Returns:
            True to signal success.
        """
        if not self._directory_exists(directory_name):
            if directory_name[-1] != '/':
                directory_name = directory_name + '/'

            blob = self.storage_bucket.blob(directory_name)
            blob.upload_from_string("", content_type="application/x-www-form-urlencoded:charset=UTF-8")
        return True

    def _gcs_add_file(self, file_path, directory_name) -> str:
        """
        Add a file to the cloud storage bucket.
    
        Args:
            file_path: the path to the file.
            directory_name: the name of the GCS directory to upload the file to.

        Returns:
            a link to the file in Google Cloud Storage.
        """
        if directory_name[-1] != '/':
            directory_name = directory_name + '/'

        blob = self.storage_bucket.blob(directory_name + os.path.basename(file_path))
        blob.upload_from_filename(file_path)
        return blob.self_link

    def get_private_slack_channels_ids(self) -> dict:
        """
        Get the private channel IDs and names from the Slack workspace and store the infor in JSON format.

        Returns:
            a dictionary of channel id and channel name the private channels in the workspace.
        """
        is_call_successful = False
        while not is_call_successful:
            try:
                channels = {}
                for result in self.client.conversations_list(types="private_channel"):
                    for channel in result["channels"]:
                        channels[channel["id"]] = channel['name']
                Path(f'downloads/channels/').mkdir(parents=True, exist_ok=True)
                with open('downloads/channels/private_channels.json', 'w') as fp:
                    json.dump(channels, fp, indent=4)
                is_call_successful = False
                return channels
            except SlackApiError as e:
                self.logger.error(f"[get_private_slack_channels_ids][SlackApiError] Error: {e}")
                sleep(15)
            except IncompleteRead:
                self.logger.warning("[get_private_slack_channels_ids][IncompleteRead] Unable to fetch Slack channels IDs, unstable network")

    def get_public_slack_channels_ids(self) -> dict:
        """
        Get the public channel IDs and names from the Slack workspace and store the infor in JSON format.

        Returns:
            a dictionary of channel id and channel name the public channels in the workspace.
        """
        is_call_successful = False
        while is_call_successful:
            try:
                channels = {}
                for result in self.client.conversations_list(types="public_channel"):
                    for channel in result["channels"]:
                        channels[channel["id"]] = channel['name']
                Path(f'downloads/channels/').mkdir(parents=True, exist_ok=True)
                with open('downloads/channels/public_channels.json', 'w') as fp:
                    json.dump(channels, fp, indent=4)
                is_call_successful = True
                return channels
            except SlackApiError as e:
                self.logger.error(f"[get_public_slack_channels_ids][SlackApiError] Error: {e}")
                sleep(15)
            except IncompleteRead:
                self.logger.warning("[get_public_slack_channels_ids][IncompleteRead] Unable to fetch Slack channels IDs, unstable network")


    '''
    The date returned must be of yesterday. The goal is to load on any execution the information known for yesterday.
    '''
    def get_execution_tm(self):
        yesterday = datetime.today() - timedelta(days=1)
        return yesterday.strftime('%Y%m%d')

    def get_slack_timestamp(self, year, month, day):
        dt = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
        timestamp = time.mktime(dt.timetuple())
        return timestamp

    def get_slack_messages(self) -> bool:
        '''
        Download slack messages, threads and their related files.
        Returns:
            True if the download happens without error, else False.
        '''
        try:
            threaded_replies = []
            current_date = self.get_execution_tm()
            end_date = date.today()
            start_date = end_date - timedelta(days=1)

            with open('downloads/channels/private_channels.json', 'r') as fp:
                channels = json.load(fp)

            Path(f'downloads/messages/').mkdir(parents=True, exist_ok=True)
            Path(f'downloads/messages/slack_{current_date}.jsonl').touch(exist_ok=True)

            with open(f'downloads/messages/slack_{current_date}.jsonl', 'a') as messages_fp:
                #
                oldest_timestamp_tm = self.get_slack_timestamp(start_date.year, start_date.month, start_date.day)
                latest_timestamp_tm = self.get_slack_timestamp(end_date.year, end_date.month, end_date.day)
                for channel_id, channel_name in channels.items():
                    messages = []
                    self.last_checkpoint = 0
                    self.logger.info(f'[get_slack_messages] {channel_name}')

                    if channel_name in self.read_channels:
                        self.last_checkpoint = self.read_channels[channel_name]

                    conversation_history = self.client.conversations_history(
                        channel=channel_id,
                        oldest=oldest_timestamp_tm,
                        latest=latest_timestamp_tm,
                        limit=999,
                        inclusive=True
                    )
                    if conversation_history['ok']:
                        messages = conversation_history['messages']
                    self.logger.info(f'[get_slack_messages] Number of messages in {channel_name}: {len(messages)}')

                    if len(messages) > 0:
                        for message_number, message in enumerate(messages):
                            if self.last_checkpoint == len(messages):
                                message_number = self.last_checkpoint - 1
                                break
                            if message_number < self.last_checkpoint:
                                continue
                            message['channel_name'] = channel_name
                            message['channel_id'] = channel_id

                            thread = self.client.conversations_replies(channel=channel_id, ts=message['ts'])
                            if thread['ok']:
                                threaded_replies = thread['messages']
                                for thread in threaded_replies:
                                    if thread.get("blocks"):
                                        thread['blocks'] = [str(thread['blocks'])]
                                    if not thread.get('old_name'):
                                        thread['old_name'] = None
                                    if not thread.get('name'):
                                        thread['name'] = None
                                    if not thread.get('purpose'):
                                        thread['purpose'] = None
                                    if thread.get('pinned_to'):
                                        del thread['pinned_to']
                                    if thread.get('pinned_info'):
                                        del thread['pinned_info']
                                    if thread.get('root') and thread['root'].get('attachments'):
                                        for attachment in thread['root']['attachments']:
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
                                    if thread.get('root') and thread['root'].get('blocks'):
                                        thread['root']['blocks'] = [str(thread['root']['blocks'])]
                                    if thread.get('root') and thread['root'].get('files'):
                                        del thread['root']['files']
                                    if thread.get('attachments'):
                                        for attachment in thread['attachments']:
                                            if attachment.get('blocks'):
                                                attachment['blocks'] = [str(attachment['blocks'])]
                                            if attachment.get('message_blocks'):
                                                attachment['message_blocks'] = [str(attachment['message_blocks'])]
                                            if attachment.get('files'):
                                                del attachment['files']
                                            if not attachment.get('private_channel_prompt'):
                                                attachment['private_channel_prompt'] = False
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
                                                del thread['pinned_to']
                                            if attachment.get('pinned_info'):
                                                del thread['pinned_info']

                                    file_paths = []
                                    if thread.get('files'):
                                        self._gcs_add_directory(f'files/{current_date}/{channel_name}')
                                        for file in thread.get('files'):
                                            if file.get('url_private_download'):
                                                file_path = self._download_and_verify_slack_file(
                                                    file.get('url_private_download'),
                                                    f'downloads/files/{current_date}/{channel_name}'
                                                )
                                                if file_path:
                                                    try:
                                                        file_storage_path = None
                                                        file_storage_path = self._gcs_add_file(file_path, f'files/{current_date}/{channel_name}')
                                                        if file_storage_path:
                                                            self.logger.info(f'File successfully backed to Cloud Storage')
                                                            file_paths.append({
                                                                'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                                'filename': file.get('name'),
                                                                'storage_url': file_storage_path
                                                            })
                                                        else:
                                                            file_paths.append({
                                                                'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                                'filename': file.get('name'),
                                                            })
                                                    except (TimeoutError, ConnectionError):
                                                        continue
                                    thread['files'] = file_paths

                            if message:
                                file_paths = []
                                if not message.get('old_name'):
                                    message['old_name'] = None
                                if not message.get('name'):
                                    message['name'] = None
                                if not message.get('purpose'):
                                    message['purpose'] = None
                                if message.get("blocks"):
                                    message['blocks'] = [str(message['blocks'])]
                                if message.get('root') and message['root'].get('blocks'):
                                    message['root']['blocks'] = [str(message['root']['blocks'])]
                                if message.get('pinned_to'):
                                    del message['pinned_to']
                                if message.get('pinned_info'):
                                    del message['pinned_info']
                                if message.get('root') and message['root'].get('attachments'):
                                    for attachment in message['root']['attachments']:
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
                                if message.get('attachments'):
                                    for attachment in message['attachments']:
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
                                            del thread['pinned_to']
                                        if attachment.get('pinned_info'):
                                            del thread['pinned_info']
                                if message.get('root') and message['root'].get('files'):
                                    del message['root']['files']
                                if message.get('files'):
                                    self._gcs_add_directory(f'files/{current_date}/{channel_name}')
                                    for file in message.get('files'):
                                        if file.get('url_private_download'):
                                            file_path = self._download_and_verify_slack_file(
                                                file.get('url_private_download'),
                                                f'downloads/files/{current_date}/{channel_name}'
                                            )
                                            if file_path:
                                                try:
                                                    file_storage_path = None
                                                    file_storage_path = self._gcs_add_file(file_path, f'files/{current_date}/{channel_name}')
                                                    if file_storage_path:
                                                        self.logger.info(f'File successfully backed to Cloud Storage')
                                                        file_paths.append({
                                                            'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                            'filename': file.get('name'),
                                                            'storage_url': file_storage_path
                                                        })
                                                    else:
                                                        file_paths.append({
                                                            'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                            'filename': file.get('name'),
                                                        })
                                                except (TimeoutError, ConnectionError):
                                                    continue
                                message['files'] = file_paths
                                message['threads'] = threaded_replies
                                json.dump(message, messages_fp)                                                                                     # Save in JSONL format
                                messages_fp.write('\n')
                        self._write_checkpoint(channel_name, message_number + 1)
                        message_number = 0                                                                                                          # In case it fails at the start of the next channel, message number should be zero
                        sleep(5)
                self._gcs_add_directory(f'messages/')
                self._gcs_add_file(f'downloads/messages/slack_{current_date}.jsonl', f'messages/')
                self.logger.info(f'Messages file successfully backed to Cloud Storage')
                sleep(15)
                self._clean_jsonl_file(f'downloads/messages/slack_{current_date}.jsonl')
                if not self._load_to_bigquery(f'downloads/messages/slack_{current_date}.jsonl'):
                    self.logger.error('[get_slack_messages][load] Failed to load the data to BigQuery.')
                else:
                    self.logger.info(f'Data successfully loaded to BigQuery')
            return True
        except SlackApiError as e:
            self.logger.error(f"[get_slack_messages][SlackApiError] Error: {e}")
            try:
                if message_number < self.last_checkpoint:
                    message_number = self.last_checkpoint
                self.write_checkpoint(self.checkpoint_file, channel_name, message_number)
            except UnboundLocalError:
                pass
            finally:
                return False
        except IncompleteRead:
            self.logger.error("[get_slack_messages][IncompleteRead]Unable to fetch channel messages, unstable network", end='\n')
            try:
                if message_number < self.last_checkpoint:
                    message_number = self.last_checkpoint
                self.write_checkpoint(self.checkpoint_file, channel_name, message_number)
            except UnboundLocalError:
                pass
            finally:
                return False

    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean a record by removing None values and empty containers that should be None.
        
        Args:
            record: Dictionary containing the data to clean
        Returns:
            Cleaned dictionary with proper None handling
        """
        def clean_value(value: Any) -> Any:
            if value is None:
                return None
            elif isinstance(value, dict):
                cleaned = {k: clean_value(v) for k, v in value.items() if v is not None}
                return cleaned if cleaned else None
            elif isinstance(value, list):
                cleaned = [clean_value(item) for item in value if item is not None]
                return cleaned if cleaned else None
            elif isinstance(value, str) and '.' in value:
                # Try to convert string timestamps to float
                try:
                    return float(value)
                except ValueError:
                    return value
            elif isinstance(value, str) and value.isdigit():
                # Try to convert string integers
                try:
                    return int(value)
                except ValueError:
                    return value
            return value

        return {k: clean_value(v) for k, v in record.items() if v is not None}

    def modify_schema_for_nulls(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Modify schema to properly handle null values for all fields including arrays.
        
        Args:
            schema: Original JSON schema
        Returns:
            Modified schema that properly handles null values
        """
        def modify_field(field: Dict[str, Any]) -> Dict[str, Any]:
            if "type" in field:
                if isinstance(field["type"], str):
                    field["type"] = [field["type"], "null"]
                elif isinstance(field["type"], list) and "null" not in field["type"]:
                    field["type"].append("null")
            
            if "properties" in field:
                for prop in field["properties"].values():
                    if isinstance(prop, dict):
                        modify_field(prop)
            
            if "items" in field:
                if isinstance(field["items"], dict):
                    modify_field(field["items"])
                
                # For array fields, add oneOf to allow null values
                field["oneOf"] = [
                    {"type": "array"},
                    {"type": "null"}
                ]
            
            return field

        # Create a deep copy of the schema and modify it
        modified_schema = json.loads(json.dumps(schema))
        return modify_field(modified_schema)

    def write_to_jsonl_file(self, data: List[Dict[Any, Any]], output_file: str):
        with open('json_schema.json', 'r') as fp:
            schema = json.load(fp)

        nullable_schema = self.modify_schema_for_nulls(schema)
        
        # Create JSON schema validator
        validator = jsonschema.Draft7Validator(nullable_schema)
        
        valid_records = 0
        invalid_records = 0
        
        with open(output_file, 'a') as f:
            for record in data:
                # Clean the record
                cleaned_record = self.clean_record(record)
                
                # Validate record against schema
                errors = list(validator.iter_errors(cleaned_record))
                if errors:
                    self.logger(f"\nValidation errors found in record:")
                    for error in errors:
                        self.logger(f"- {error.message}")
                        self.logger(f"  Path: {' -> '.join(str(p) for p in error.path)}")
                        self.logger(f"  Value: {error.instance}")
                    invalid_records += 1
                    continue
                    
                # Write valid record to JSONL file
                f.write(json.dumps(cleaned_record) + '\n')
                valid_records += 1
        
        self.logger(f"\nProcessing complete:")
        self.logger(f"Valid records written: {valid_records}")
        self.logger(f"Invalid records skipped: {invalid_records}")

    def download_thread(self, initial_date, results, table):
        current_date = self.get_execution_tm()
        messages = []
        
        for result in results:
                channel_id = str(result['channel_id'][0])
                channel_name = str(result['channel_name'][0])
                chat_timestamp = str(result['top_level_timestamp'])
                oldest_timestamp = str(result['latest_thread_timestamp'])


                message = self.sample_message_structure()
                message['channel_name'] = channel_name
                message['channel_id'] = channel_id
                message['ts'] = float(chat_timestamp)

                threads_response = self.client.conversations_replies(channel=channel_id,
                                                    ts=chat_timestamp,
                                                    oldest=oldest_timestamp)

                if threads_response['ok'] and len(threads_response['messages']) > 2:
                    threaded_replies = threads_response['messages']
                    del threaded_replies[0]
                    threads = []

                    for thread in threaded_replies:
                        thread_schema = self.sample_thread_structure()
                        if thread.get("blocks"):
                            thread_schema['blocks'] = [str(thread['blocks'])]
                        if thread.get('old_name'):
                            thread_schema['old_name'] = thread['old_name']
                        if thread.get('name'):
                            thread_schema['name'] = thread['name']
                        if thread.get('bot_id'):
                            thread_schema['bot_id'] = thread['bot_id']
                        if thread.get('parent_user_id'):
                            thread_schema['parent_user_id'] = thread['parent_user_id']
                        if thread.get('reactions'):
                            thread_schema['reactions'] = thread['reactions']
                        if thread.get('x_files'):
                            thread_schema['x_files'] = thread['x_files']
                        if thread.get('bot_profile'):
                            thread_schema['bot_profile'] = thread['bot_profile']
                        if thread.get('subscribed'):
                            thread_schema['subscribed'] = thread['subscribed']
                        if thread.get('reply_users_count'):
                            thread_schema['reply_users_count'] = thread['reply_users_count']
                        if thread.get('thread_ts'):
                            thread_schema['thread_ts'] = thread['thread_ts']
                        if thread.get('is_locked'):
                            thread_schema['is_locked'] = thread['is_locked']
                        if thread.get('edited'):
                            thread_schema['edited'] = thread['edited']
                        if thread.get('client_msg_id'):
                            thread_schema['client_msg_id'] = thread['client_msg_id']
                        if thread.get('latest_reply'):
                            thread_schema['latest_reply'] = thread['latest_reply']
                        if thread.get('hidden'):
                            thread_schema['hidden'] = thread['hidden']
                        if thread.get('inviter'):
                            thread_schema['inviter'] = thread['inviter']
                        if thread.get('team'):
                            thread_schema['team'] = thread['team']
                        if thread.get('reply_count'):
                            thread_schema['reply_count'] = thread['reply_count']
                        if thread.get('upload'):
                            thread_schema['upload'] = thread['upload']
                        if thread.get('user'):
                            thread_schema['user'] = thread['user']
                        if thread.get('text'):
                            thread_schema['text'] = thread['text']
                        if thread.get('ts'):
                            thread_schema['ts'] = thread['ts']
                        if thread.get('reply_users'):
                            thread_schema['reply_users'] = thread['reply_users']
                        if thread.get('type'):
                            thread_schema['type'] = thread['type']
                        if thread.get('display_as_bot'):
                            thread_schema['display_as_bot'] = thread['display_as_bot']
                        if thread.get('subtype'):
                            thread_schema['subtype'] = thread['subtype']
                        if thread.get('purpose'):
                            thread_schema['purpose'] = thread['purpose']

                        if thread.get('root') and thread['root'].get('attachments'):
                            for attachment in thread['root']['attachments']:
                                if attachment.get('bot_team_id'):
                                    del attachment['bot_team_id']
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

                        if thread.get('root') and thread['root'].get('blocks'):
                            thread['root']['blocks'] = [str(thread['root']['blocks'])]
                        if thread.get('root') and thread['root'].get('files'):
                            del thread['root']['files']
                        if thread.get('root'):
                            thread_schema['root'] = thread['root']

                        if thread.get('attachments'):
                            for attachment in thread['attachments']:
                                if attachment.get('bot_team_id'):
                                    del attachment['bot_team_id']
                                if attachment.get('blocks'):
                                    attachment['blocks'] = [str(attachment['blocks'])]
                                if attachment.get('message_blocks'):
                                    attachment['message_blocks'] = [str(attachment['message_blocks'])]
                                if attachment.get('files'):
                                    del attachment['files']
                                if not attachment.get('private_channel_prompt'):
                                    attachment['private_channel_prompt'] = False
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
                                    del thread['pinned_to']
                                if attachment.get('pinned_info'):
                                    del thread['pinned_info']
                            thread_schema['attachments'] = thread['attachments']
                        
                        file_paths = []
                        if thread.get('files'):
                            self._gcs_add_directory(f'files/{initial_date}/{channel_name}')
                            for file in thread.get('files'):
                                if file.get('preview_is_truncated'):
                                    del file['preview_is_truncated']
                                if file.get('url_private_download'):
                                    file_path = self._download_and_verify_slack_file(
                                        file.get('url_private_download'),
                                        f'downloads/files/{initial_date}/{channel_name}'
                                    )
                                    if file_path:
                                        try:
                                            file_storage_path = None
                                            file_storage_path = self._gcs_add_file(file_path, f'files/{initial_date}/{channel_name}')
                                            if file_storage_path:
                                                self.logger.info(f'File successfully backed to Cloud Storage')
                                                file_paths.append({
                                                    'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                    'filename': file.get('name'),
                                                    'storage_url': file_storage_path
                                                })
                                            else:
                                                file_paths.append({
                                                    'timestamp': str(file.get('timestamp')) if file.get('timestamp') else '',
                                                    'filename': file.get('name'),
                                                })
                                        except (TimeoutError, ConnectionError):
                                            continue
                        thread_schema['files'] = file_paths
                        threads.append(thread_schema)

                    message['threads'] = threads
                    messages.append(message)
                elif not threads_response['ok']:
                    self.logger.info(f'Error fetching threads: {threads_response['error']}')
                else:
                    # No update in threads
                    pass
        if messages:
            self.write_to_jsonl_file(messages, f'downloads/messages/slack_{initial_date}-{current_date}_threads_update.jsonl')

    def threads_sync(self):
        tables = self.get_dataset_slack_tables()
        current_date = self.get_execution_tm()
        Path(f'downloads/messages/').mkdir(parents=True, exist_ok=True)

        for table in tables:
            initial_date = table.split('_')[1]
            start_date = datetime.strptime(initial_date, '%Y%m%d').date() - timedelta(days=21)

            oldest_timestamp_tm = self.get_slack_timestamp(start_date.year, start_date.month, start_date.day)

            Path(f'downloads/messages/slack_{initial_date}-{current_date}_threads_update.jsonl').touch(exist_ok=True)

            channels = self.get_private_slack_channels_ids()

            for channel_id, _ in channels.items():
                messages_query = f"""
                    SELECT
                        t.ts as `top_level_timestamp`,
                        ARRAY_AGG(
                            thread.ts
                            ORDER BY thread.ts DESC
                        ) [OFFSET(0)] as `latest_thread_timestamp`,
                        ARRAY_AGG(t.channel_id LIMIT 1) as `channel_id`,
                        ARRAY_AGG(t.channel_name LIMIT 1) as `channel_name`,
                        ARRAY_AGG(thread) as threads
                    FROM
                        `{os.environ['DATASET_ID']}.{table}` as t,
                        UNNEST(threads) as thread
                    WHERE
                        t.channel_id = '{channel_id}'
                        AND
                        (thread.ts >= {oldest_timestamp_tm} OR t.ts >= {oldest_timestamp_tm})
                    GROUP BY
                        t.ts
                    ORDER BY
                        t.ts
                """
                results = self.bigquery_client.query_and_wait(messages_query)
                self.download_thread(
                    initial_date,
                    results,
                    table
                )
            if not self._load_to_bigquery(f'downloads/messages/slack_{initial_date}-{current_date}_threads_update.jsonl', initial_date):
                self.logger.info(f'Error loading data to BigQuery')
            else:
                self.logger.info(f'Successfully loaded data to BigQuery')

    def get_dataset_slack_tables(self):
        query = f"""
            SELECT
                table_name
            FROM
                {os.environ['DATASET_ID']}.INFORMATION_SCHEMA.TABLES
            WHERE
                table_name LIKE 'slack_202%'
            ORDER BY
                table_name;
        """
        results = self.bigquery_client.query_and_wait(query)
        tables = [table[0] for table in results]
        return tables

    def sample_thread_structure(self):
        thread_structure = {
            "bot_id": None,
            "reactions": None,
            "parent_user_id": None,
            "x_files": None,
            "bot_profile": None,
            "subscribed": None,
            "reply_users_count": None,
            "thread_ts": None,
            "is_locked": None,
            "edited": None,
            "attachments": None,
            "client_msg_id": None,
            "name": None,
            "blocks": None,
            "latest_reply": None,
            "hidden": None,
            "inviter": None,
            "team": None,
            "purpose": None,
            "reply_count": None,
            "old_name": None,
            "upload": None,
            "user": None,
            "text": None,
            "ts": None,
            "reply_users": None,
            "root": None,
            "type": None,
            "files": None,
            "display_as_bot": None,
            "subtype": None
        }
        return thread_structure

    def _clean_jsonl_file(self, file_path):
        errors_found = 0
        output_file = 'downloads/messages/cleaned_jsonl.jsonl'

        with open(file_path, 'r') as infile, open(output_file, 'w') as outfile:
            for line_number, line in enumerate(infile, 1):
                try:
                    json.loads(line)
                    outfile.write(line)
                except json.JSONDecodeError as e:
                    self.logger.error(f"[clean_jsonl_file][JSONDecodeError] Error on line {line_number}: {e}")
                    self.logger.info(f"[clean_jsonl_file] Removing line {line_number}")
                    errors_found += 1

        os.replace(output_file,file_path)

        self.logger.info(f"[clean_jsonl_file] Cleaning complete. {errors_found} lines were removed.")

    def _download_and_verify_slack_file(self, file_url, storage_location='downloads') -> str:
        """
        Download files attached to messages and threads.

        Args:
            file_url: the download url of the file.
            storage_location: the location where to store the file.
        Returns:
            The path to where the file was stored.
        """
        # Download the file
        file_path = self._download_slack_file(file_url, storage_location)

        if file_path:
            # Verify the downloaded file
            if self._verify_file_content(file_path):
                self.logger.info("[download_and_verify_slack_file] ✓ File downloaded and verified successfully")
            else:
                self.logger.warning("[download_and_verify_slack_file] ⚠ File may be corrupted or in unexpected format")
            return file_path
        else:
            self.logger.error("[download_and_verify_slack_file] ✗ File download failed\n")
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

    def _download_slack_file(self, file_url, save_dir='downloads') -> str:
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

            response = requests.get(file_url, headers=headers, stream=True)
            response.raise_for_status()

            content_type = response.headers.get('content-type', '').split(';')[0]
            content_disp = response.headers.get('content-disposition', '')

            if 'filename=' in content_disp:
                filename = content_disp.split('filename=')[-1].strip('"')
                filename = filename.split('";')[0]
            else:
                ext = mimetypes.guess_extension(content_type) or ''
                filename = f"slack_file{ext}"

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            save_path = self._get_next_filename(os.path.join(save_dir, filename))

            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return save_path

        except requests.exceptions.RequestException as e:
            return None

    def _get_next_filename(self, file_path) -> str:
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

    def _load_to_bigquery(self, file_path: str) -> bool:
        '''
        Load the data to Bigquery.

        Args:
            file_path: the path to the file to be uploaded.
        
        Returns:
            True if successful, otherwise False.
        '''
        current_date = self.get_execution_tm()
        table_id = os.environ['DATASET_ID'] + f'.slack_{current_date}'
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )

        with open(file_path, 'rb') as fp:
            try:
                load_job = self.bigquery_client.load_table_from_file(
                    file_obj=fp,
                    destination=table_id,
                    job_config=job_config
                )
            except (ValueError, TypeError) as e:
                self.logger.error(f'[_load_to_bigquery] Error while loading to BigQuery: {e}')
                return False

            try:
                load_job.result()
            except Exception as e:
                self.logger.error(f'[_load_to_bigquery] Load job failed/did not complete: {e}')
                return False
        return True

    def start(self,):
        try:
            self.get_slack_workspace_members()
            self.get_private_slack_channels_ids()
            self.get_public_slack_channels_ids()

            self.read_channels = self._read_checkpoints()
            response = self.get_slack_messages()

            while not response:
                self.logger.info("[start] Restarting download")
                self.read_channels = self._read_checkpoints()
                sleep(15)
                response = self.get_slack_messages()
        except KeyboardInterrupt:
            self.logger.warning(f'[start][KeyboardInterrupt] Stopping the app.')
        finally:
            self._stop()

    def _stop(self,):
        """
        Stop the bot and perform clean up operations.
        """
        import shutil                               # Delete the downloaded content

        try:
            # Delete the downloads folder after the bot is done
            # and is saving content to the cloud. Otherwise don't delete.
            if self.save_to_cloud:
                shutil.rmtree(self.downloads_folder)
        except Exception as e:
            self.logger.error(f'[stop] Error: {e}')
