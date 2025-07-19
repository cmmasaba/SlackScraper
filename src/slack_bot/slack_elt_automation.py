from datetime import datetime, timedelta, timezone, date
from http.client import IncompleteRead
import json
import mimetypes
import os
from pathlib import Path
from time import sleep
import time
from typing import Any

import requests
from slack_bolt import App
from slack_sdk.errors import SlackApiError
from google.cloud import storage, bigquery
from tenacity import retry, stop_after_attempt, wait_random_exponential

from util.logging import GclClient

slack_schema = [
    bigquery.SchemaField("parent_user_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("bot_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "reactions",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("users", "STRING", mode="REPEATED"),
            bigquery.SchemaField("count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("x_files", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "bot_profile",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(
                "icons",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("image_72", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_48", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_36", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("team_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("deleted", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("subscribed", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("is_locked", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("reply_users", "STRING", mode="REPEATED"),
    bigquery.SchemaField("latest_reply", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("channel_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("reply_users_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField(
        "edited",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        "attachments",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("msg_subtype", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_html_height", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("video_html", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("service_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_thread_root_unfurl", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("video_html_width", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_reply_unfurl", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("message_blocks", "STRING", mode="REPEATED"),
            bigquery.SchemaField("image_bytes", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("image_height", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("thumb_height", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
            bigquery.SchemaField("thumb_width", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("is_share", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("service_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("title_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("channel_team", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_animated", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_msg_unfurl", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("original_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("bot_team_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_subname", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("service_icon", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_icon", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("footer_icon", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "fields",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("short", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("author_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("from_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("image_width", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("footer", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_app_unfurl", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("callback_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("private_channel_prompt", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("app_unfurl_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("bot_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("thumb_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fallback", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("client_msg_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
    bigquery.SchemaField("hidden", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("inviter", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("team", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("reply_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("old_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("thread_ts", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("upload", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "threads",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("bot_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "reactions",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("users", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("count", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("parent_user_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("x_files", "STRING", mode="REPEATED"),
            bigquery.SchemaField(
                "bot_profile",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        "icons",
                        "RECORD",
                        mode="NULLABLE",
                        fields=[
                            bigquery.SchemaField("image_72", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("image_48", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("image_36", "STRING", mode="NULLABLE"),
                        ],
                    ),
                    bigquery.SchemaField("team_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("updated", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("deleted", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("subscribed", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("reply_users_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("thread_ts", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("is_locked", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(
                "edited",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "attachments",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("msg_subtype", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("video_html_height", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("video_html", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("service_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_thread_root_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("video_html_width", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_reply_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("message_blocks", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("image_bytes", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("image_height", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("thumb_height", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("thumb_width", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("is_share", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("service_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("title_link", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("channel_team", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_animated", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_msg_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("original_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_subname", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("service_icon", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_icon", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("footer_icon", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("bot_team_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "fields",
                        "RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField("short", "BOOLEAN", mode="NULLABLE"),
                            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                        ],
                    ),
                    bigquery.SchemaField("author_link", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("from_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_width", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("footer", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_app_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("callback_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("private_channel_prompt", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("app_unfurl_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("bot_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("thumb_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("fallback", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("client_msg_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
            bigquery.SchemaField("latest_reply", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("hidden", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("inviter", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("team", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reply_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("old_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("upload", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("reply_users", "STRING", mode="REPEATED"),
            bigquery.SchemaField(
                "root",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(
                        "attachments",
                        "RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField("mrkdwn_in", "STRING", mode="REPEATED"),
                            bigquery.SchemaField("author_subname", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("message_blocks", "STRING", mode="REPEATED"),
                            bigquery.SchemaField("is_thread_root_unfurl", "BOOLEAN", mode="NULLABLE"),
                            bigquery.SchemaField("is_msg_unfurl", "BOOLEAN", mode="NULLABLE"),
                            bigquery.SchemaField("channel_team", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("author_id", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                            bigquery.SchemaField("service_name", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("thumb_width", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
                            bigquery.SchemaField("is_share", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("original_url", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("author_link", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("from_url", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("author_icon", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("service_icon", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("image_bytes", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("footer", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
                            bigquery.SchemaField("image_width", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("image_height", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("title_link", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("thumb_height", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("is_app_unfurl", "BOOLEAN", mode="NULLABLE"),
                            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("thumb_url", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("app_unfurl_url", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("is_reply_unfurl", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("bot_id", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("fallback", "STRING", mode="NULLABLE"),
                        ],
                    ),
                    bigquery.SchemaField("display_as_bot", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("subscribed", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "edited",
                        "RECORD",
                        mode="NULLABLE",
                        fields=[
                            bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                            bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
                        ],
                    ),
                    bigquery.SchemaField("is_locked", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("reply_users", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("reply_users_count", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("team", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("latest_reply", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("upload", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("client_msg_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("reply_count", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("thread_ts", "FLOAT", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "files",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("storage_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("filename", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("timestamp", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("display_as_bot", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("subtype", "STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "root",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(
                "attachments",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("mrkdwn_in", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("author_subname", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("message_blocks", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("is_thread_root_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("is_msg_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("channel_team", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("service_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("thumb_width", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("is_share", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("original_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_link", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("from_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("author_icon", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("service_icon", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_bytes", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("footer", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("image_width", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_height", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("title_link", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("app_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("thumb_height", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_app_unfurl", "BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("thumb_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("app_unfurl_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("is_reply_unfurl", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("bot_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("fallback", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("display_as_bot", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("blocks", "STRING", mode="REPEATED"),
            bigquery.SchemaField("subscribed", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(
                "edited",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("is_locked", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reply_users", "STRING", mode="REPEATED"),
            bigquery.SchemaField("reply_users_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("team", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latest_reply", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("upload", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("user", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("client_msg_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reply_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("thread_ts", "FLOAT", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField("ts", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("display_as_bot", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("subtype", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "files",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("storage_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("filename", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "INTEGER", mode="NULLABLE"),
        ],
    ),
]


class SlackScraper:
    def __init__(self, save_to_cloud=True) -> None:
        """
        Initialize the app.
        """
        self.slack_bot_token = os.environ["SLACK_BOT_TOKEN"]
        self.app = App(token=self.slack_bot_token)
        self.client = self.app.client
        self.downloads_folder = Path("downloads")
        self.downloads_folder.mkdir(exist_ok=True)
        self.checkpoint_file = Path(f"{self.downloads_folder}/checkpoints.json")
        self.checkpoint_file.touch(exist_ok=True)
        self.read_channels = {}
        self.storage_client = storage.Client(project=os.environ["GCP_PROJECT"])
        self.bigquery_client = bigquery.Client()
        self.storage_bucket = self.storage_client.bucket(os.environ["GCP_STORAGE_BUCKET"])
        self.last_checkpoint = 0
        self.save_to_cloud = save_to_cloud
        self.logger = GclClient().get_logger()

    def _read_checkpoints(self) -> dict[str, Any]:
        """
        Read the checkpoint to determine where to resume.
        Args:
            checkpoint_file: the path of the file where checkpoint data is stored.
        Returns:
            A list of channels that have been written, or an empty list if there's no checkpoints.
        """
        try:
            if self.checkpoint_file.exists():
                with self.checkpoint_file.open("r") as fp:
                    return json.load(fp)
            return {}
        except json.decoder.JSONDecodeError:
            return {}

    def _write_checkpoint(self, channel_name: str, message_number: int) -> None:
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
        with self.checkpoint_file.open("w") as fp:
            json.dump(checkpoints, fp, indent=4)

    @retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
    def get_slack_workspace_members(self) -> None:
        """
        Retrieve the users in the Slack workspace and store the info in JSONL format in a file in the Users folder.
        """
        is_call_successful = False
        while not is_call_successful:
            try:
                response = self.client.users_list()
                is_call_successful = True
            except SlackApiError as e:
                self.logger.error("[get_slack_workspace_members] Error: %s", e)
            except IncompleteRead as e:
                self.logger.error(
                    "[get_slack_workspace_members] Unable to fetch Slack members, unstable network. Error: %s", e
                )

        Path(f"{self.downloads_folder}/Users/").mkdir(parents=True, exist_ok=True)

        date_ext = datetime.today().strftime("%Y%m%d")
        with open(f"{self.downloads_folder}/Users/users_{date_ext}.jsonl", "w") as fp:
            for user in response["members"]:
                json.dump(user, fp)
                fp.write("\n")

        self._gcs_add_directory("users")
        self._gcs_add_file(f"{self.downloads_folder}/Users/users_{date_ext}.jsonl", "users")

    def _directory_exists(self, directory_name) -> bool:
        """
        Check if directory_name is in the bucket.

        Args:
            bucket: the Google Cloud Storage to check in.
            directory_name: the name of the directory to search for.
        Returns:
            True if the directory name is in the bucket, otherwise False
        """

        if not directory_name.endswith("/"):
            directory_name = directory_name + "/"

        blobs = list(self.storage_bucket.list_blobs(prefix=directory_name, max_results=1))

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
            if not directory_name.endswith("/"):
                directory_name = directory_name + "/"

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
        if not directory_name.endswith("/"):
            directory_name = directory_name + "/"

        blob = self.storage_bucket.blob(directory_name + os.path.basename(file_path))
        blob.upload_from_filename(file_path)

        return str(blob.self_link)

    @retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
    def get_private_slack_channels_ids(self) -> dict[str, Any]:
        """
        Get the private channel IDs and names from the Slack workspace and store the info in JSON format in 'channels' folder

        Returns:
            a dictionary of channel id and channel name the private channels in the workspace
        """
        is_call_successful = False
        channels = {}

        while not is_call_successful:
            try:
                for result in self.client.conversations_list(types="private_channel"):
                    for channel in result["channels"]:
                        channels[channel["id"]] = channel["name"]

                Path(f"{self.downloads_folder}/channels/").mkdir(parents=True, exist_ok=True)

                with open(f"{self.downloads_folder}/channels/private_channels.json", "w") as fp:
                    json.dump(channels, fp, indent=4)

                is_call_successful = False
            except SlackApiError as e:
                self.logger.error("[get_private_slack_channels_ids][SlackApiError] Error: %s", e)
            except IncompleteRead as e:
                self.logger.warning(
                    "[get_private_slack_channels_ids][IncompleteRead] Unable to fetch Slack channels IDs, unstable network. Error: %s",
                    e,
                )

        return channels

    @retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
    def get_public_slack_channels_ids(self) -> dict:
        """
        Get the public channel IDs and names from the Slack workspace and store the info in JSON format in 'channels' folder

        Returns:
            a dictionary of channel id and channel name the public channels in the workspace
        """
        is_call_successful = False
        channels = {}

        while is_call_successful:
            try:
                for result in self.client.conversations_list(types="public_channel"):
                    for channel in result["channels"]:
                        channels[channel["id"]] = channel["name"]

                Path(f"{self.downloads_folder}/channels/").mkdir(parents=True, exist_ok=True)

                with open(f"{self.downloads_folder}/channels/public_channels.json", "w") as fp:
                    json.dump(channels, fp, indent=4)

                is_call_successful = True
            except SlackApiError as e:
                self.logger.error("[get_public_slack_channels_ids][SlackApiError] Error: %s", e)
            except IncompleteRead:
                self.logger.warning(
                    "[get_public_slack_channels_ids][IncompleteRead] Unable to fetch Slack channels IDs, unstable network. Error: %s",
                    e,
                )

        return channels

    def get_yesterdays_date(self) -> datetime:
        """
        Return yesterday's date. The goal is to load on any execution the information known for yesterday
        """
        yesterday = datetime.today() - timedelta(days=1)

        return yesterday

    def to_timestamp(self, year, month, day) -> str:
        """
        Return a timestamp given year, month and day
        """
        dt = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
        timestamp = time.mktime(dt.timetuple())

        return str(timestamp)

    def process_thread(self, thread: dict[str, Any], channel_name: str, start_date: str) -> dict[str, Any]:
        """
        Process the fields in a thread to be in a consistent format expected by BQ
        Args:
            thread: a single thread object to be processed
            channel_name: the channel the thread belongs to
            start_date: the date the thread was started
        Return:
            Processed thread in a dict
        """
        processed_thread: dict[str, Any] = {}

        if thread.get("blocks"):
            processed_thread["blocks"] = [str(thread["blocks"])]

        if thread.get("root") and thread["root"].get("attachments"):
            processed_thread["root"]["attachments"] = []
            for attachment in thread["root"]["attachments"]:
                if attachment.get("blocks"):
                    attachment["blocks"] = [str(attachment["blocks"])]
                if attachment.get("message_blocks"):
                    attachment["message_blocks"] = [str(attachment["message_blocks"])]
                if attachment.get("files"):
                    del attachment["files"]
                processed_thread["root"]["attachments"].append(attachment)

        if thread.get("root") and thread["root"].get("blocks"):
            processed_thread["root"]["blocks"] = [str(thread["root"]["blocks"])]

        if thread.get("attachments"):
            processed_thread["attachments"] = []
            for attachment in thread["attachments"]:
                if attachment.get("blocks"):
                    attachment["blocks"] = [str(attachment["blocks"])]
                if attachment.get("message_blocks"):
                    attachment["message_blocks"] = [str(attachment["message_blocks"])]
                if attachment.get("files"):
                    del attachment["files"]
                if attachment.get("mrkdwn_in"):
                    del attachment["mrkdwn_in"]
                if attachment.get("pinned_to"):
                    del thread["pinned_to"]
                if attachment.get("pinned_info"):
                    del thread["pinned_info"]
                processed_thread["attachments"].append(attachment)

        file_paths = []
        if thread.get("files"):
            self._gcs_add_directory(f"files/{start_date}/{channel_name}")
            for file in thread["files"]:
                if file.get("url_private_download"):
                    file_path = self._download_and_verify_slack_file(
                        file.get("url_private_download"), f"{self.downloads_folder}/files/{start_date}/{channel_name}"
                    )
                    if file_path:
                        try:
                            file_storage_path = None
                            file_storage_path = self._gcs_add_file(file_path, f"files/{start_date}/{channel_name}")
                            if file_storage_path:
                                self.logger.info("[process_thread] file successfully backed to Cloud Storage")
                                file_paths.append(
                                    {
                                        "timestamp": str(file.get("timestamp")) if file.get("timestamp") else "",
                                        "filename": file.get("name"),
                                        "storage_url": file_storage_path,
                                    }
                                )
                            else:
                                file_paths.append(
                                    {
                                        "timestamp": str(file.get("timestamp")) if file.get("timestamp") else "",
                                        "filename": file.get("name"),
                                    }
                                )
                        except (TimeoutError, ConnectionError):
                            continue

        processed_thread["files"] = file_paths
        return processed_thread

    def process_message(self, message: dict[str, Any], channel_name: str, start_date: str) -> dict[str, Any]:
        """
        Process the fields in a message to be in a consistent format expected by BQ
        Args:
            thread: a single message object to be processed
            channel_name: the channel the thread belongs to
            start_date: the date the thread was started
        Return:
            Processed message in a dict
        """
        processed_message: dict = {}

        if message.get("blocks"):
            processed_message["blocks"] = [str(message["blocks"])]

        if message.get("root") and message["root"].get("blocks"):
            processed_message["root"]["blocks"] = [str(message["root"]["blocks"])]

        if message.get("root") and message["root"].get("attachments"):
            processed_message["root"]["attachments"] = []
            for attachment in message["root"]["attachments"]:
                if attachment.get("blocks"):
                    attachment["blocks"] = [str(attachment["blocks"])]
                if attachment.get("message_blocks"):
                    attachment["message_blocks"] = [str(attachment["message_blocks"])]
                if attachment.get("files"):
                    del attachment["files"]
                processed_message["root"]["attachments"].append(attachment)

        if message.get("attachments"):
            processed_message["attachments"] = []
            for attachment in message["attachments"]:
                if attachment.get("blocks"):
                    attachment["blocks"] = [str(attachment["blocks"])]
                if attachment.get("message_blocks"):
                    attachment["message_blocks"] = [str(attachment["message_blocks"])]
                if attachment.get("files"):
                    del attachment["files"]
                if attachment.get("mrkdwn_in"):
                    del attachment["mrkdwn_in"]
                if attachment.get("pinned_to"):
                    del attachment["pinned_to"]
                if attachment.get("pinned_info"):
                    del attachment["pinned_info"]
                processed_message["attachments"].append(attachment)

        file_paths = []
        if message.get("files"):
            self._gcs_add_directory(f"files/{start_date}/{channel_name}")
            for file in message["files"]: # pyright: ignore
                if file.get("url_private_download"):
                    file_path = self._download_and_verify_slack_file(
                        file.get("url_private_download"), f"{self.downloads_folder}/files/{start_date}/{channel_name}"
                    )
                    if file_path:
                        try:
                            file_storage_path = None
                            file_storage_path = self._gcs_add_file(file_path, f"files/{start_date}/{channel_name}")
                            if file_storage_path:
                                self.logger.info("[process_message] file successfully backed to Cloud Storage")
                                file_paths.append(
                                    {
                                        "timestamp": str(file.get("timestamp")) if file.get("timestamp") else "",
                                        "filename": file.get("name"),
                                        "storage_url": file_storage_path,
                                    }
                                )
                            else:
                                file_paths.append(
                                    {
                                        "timestamp": str(file.get("timestamp")) if file.get("timestamp") else "",
                                        "filename": file.get("name"),
                                    }
                                )
                        except (TimeoutError, ConnectionError):
                            continue

        message["files"] = file_paths
        return processed_message

    def get_slack_messages(self) -> bool:
        """
        Download slack messages, threads and their related files.
        Returns:
            True if the download happens without error, else False.
        """
        try:
            start_date = self.get_yesterdays_date()
            end_date = date.today()

            with open(f"{self.downloads_folder}/channels/private_channels.json", mode="r", encoding="utf-8") as fp:
                channels = json.load(fp)

            Path(f"{self.downloads_folder}/messages/").mkdir(parents=True, exist_ok=True)
            Path(f"{self.downloads_folder}/messages/slack_{start_date}.jsonl").touch(exist_ok=True)

            with open(
                f"{self.downloads_folder}/messages/slack_{start_date}.jsonl", mode="a", encoding="utf-8"
            ) as messages_fp:
                oldest_timestamp_tm = self.to_timestamp(start_date.year, start_date.month, start_date.day)
                latest_timestamp_tm = self.to_timestamp(end_date.year, end_date.month, end_date.day)

                # TODO: add multithreading or multiprocessing to speed up
                for channel_id, channel_name in channels.items():
                    messages = []
                    self.last_checkpoint = 0
                    self.logger.info("[get_slack_messages] %s", channel_name)

                    if channel_name in self.read_channels:
                        self.last_checkpoint = self.read_channels[channel_name]

                    # if fetching messages for the first time, there might be more than 999 messages in the channels
                    # and additional logic will be necessary to download all of them. Refer to the docs for conversation_history
                    # to see how to handle such cases
                    # TODO: implement this case
                    conversation_history = self.client.conversations_history(
                        channel=channel_id,
                        oldest=oldest_timestamp_tm,
                        latest=latest_timestamp_tm,
                        limit=999,  # max limit from Slack API
                        inclusive=True,
                    )

                    if conversation_history["ok"]:
                        messages = conversation_history["messages"]
                        self.logger.info(
                            "[get_slack_messages] Number of messages in %s: %d",
                            channel_name,
                            len(messages),  # pyright: ignore
                        )

                    if len(messages) > 0: # pyright: ignore
                        for message_number, message in enumerate(messages): # pyright: ignore
                            # last_checkpoint is relevant if the bot was suddenly stopped to track what message the bot was at by using the number of messages fetched
                            # only start processing messages if message number > last checkpoint
                            if self.last_checkpoint == len(messages):  # pyright: ignore
                                message_number = self.last_checkpoint - 1  # Off By 1
                                break
                            if message_number < self.last_checkpoint:
                                continue

                            message["channel_name"] = channel_name
                            message["channel_id"] = channel_id

                            thread = self.client.conversations_replies(channel=channel_id, ts=message["ts"])
                            processed_threads = []
                            processed_message = {}

                            if thread["ok"]:
                                for thread in thread["messages"]: # pyright: ignore
                                    processed_threads.append(self.process_thread(thread, channel_name, start_date.strftime("%Y%m%d")))

                            if message:
                                processed_message = self.process_message(message, channel_name, start_date.strftime("%Y%m%d"))
                                processed_message["threads"] = processed_message
                                json.dump(processed_message, messages_fp)
                                messages_fp.write("\n")

                        self._write_checkpoint(channel_name, message_number + 1)
                        message_number = 0  # Reset message number to 0 before start processing for next channel

                self._gcs_add_directory("messages/")
                self._gcs_add_file(f"{self.downloads_folder}/messages/slack_{start_date}.jsonl", "messages/")
                self.logger.info("[get_slack_messages] messages file successfully backed to Cloud Storage")

                self._clean_jsonl_file(f"{self.downloads_folder}/messages/slack_{start_date}.jsonl")

                if not self._load_to_bigquery(f"{self.downloads_folder}/messages/slack_{start_date}.jsonl"):
                    self.logger.error("[get_slack_messages][load] failed to load the data to BigQuery")
                else:
                    self.logger.info("[get_slack_messages] data successfully loaded to BigQuery")
            return True
        except SlackApiError as e:
            self.logger.error(f"[get_slack_messages][SlackApiError] Error: {e}")
            try:
                if message_number < self.last_checkpoint:
                    message_number = self.last_checkpoint
                self._write_checkpoint(channel_name, message_number)
            except UnboundLocalError:
                pass
            return False

        except IncompleteRead:
            self.logger.error(
                "[get_slack_messages][IncompleteRead]Unable to fetch channel messages, unstable network",
            )
            try:
                if message_number < self.last_checkpoint:
                    message_number = self.last_checkpoint
                self._write_checkpoint(channel_name, message_number)
            except UnboundLocalError:
                pass
            return False

    def clean_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Clean a record by removing None values and empty containers that should be None.

        Args:
            record: dictionary containing the data to clean
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
            elif isinstance(value, str) and "." in value:
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

    def write_to_jsonl_file(self, data: list[dict[Any, Any]], output_file: str) -> None:
        with open(output_file, "a") as f:
            for record in data:
                # Clean the record
                # cleaned_record = self.clean_record(record)

                # Write valid record to JSONL file
                f.write(json.dumps(record) + "\n")

        self.logger.info("\nProcessing complete:")
        self._clean_jsonl_file(output_file)

    def _clean_jsonl_file(self, file_path) -> None:
        errors_found = 0
        output_file = "downloads/messages/cleaned_jsonl.jsonl"

        with open(file_path, "r") as infile, open(output_file, "w") as outfile:
            for line_number, line in enumerate(infile, 1):
                try:
                    json.loads(line)
                    outfile.write(line)
                except json.JSONDecodeError as e:
                    self.logger.error(f"[clean_jsonl_file][JSONDecodeError] Error on line {line_number}: {e}")
                    self.logger.info(f"[clean_jsonl_file] Removing line {line_number}")
                    errors_found += 1

        os.replace(output_file, file_path)

        self.logger.info(f"[clean_jsonl_file] Cleaning complete. {errors_found} lines were removed.")

    @retry(
        reraise=True,
        wait=wait_random_exponential(min=1, max=60),
        stop=stop_after_attempt(6),
    )
    def get_user_details(self, user_id: str) -> dict[str, str]:
        """Get user profile details from Slack

        Args:
            user_id (str): slack user ID
        """
        details = {}

        if user_id == "":
            return details

        try:
            user_info = self.client.users_info(user=user_id)

            if user_info["ok"]:
                details["user_name"] = user_info["user"]["profile"].get("real_name")  # pyright: ignore
                details["user_email"] = user_info["user"]["profile"].get("email")  # pyright: ignore
        except SlackApiError as e:
            self.logger.error(f"[get_user_details][SlackApiError] Error: {e}")
        except IncompleteRead:
            self.logger.warning(
                "[get_user_details][IncompleteRead] Unable to fetch Slack channels IDs, unstable network"
            )

        return details

    def download_thread(self, initial_date, results):
        current_date = self.get_execution_tm()
        messages = []

        for result in results:
            channel_id = str(result["channel_id"][0])
            channel_name = str(result["channel_name"][0])
            chat_timestamp = str(result["top_level_timestamp"])
            oldest_timestamp = str(result["latest_thread_timestamp"])

            message: dict[str, Any] = {}
            message["channel_name"] = channel_name  # pyright: ignore
            message["channel_id"] = channel_id  # pyright: ignore
            message["ts"] = float(chat_timestamp)  # pyright: ignore

            user_info = self.get_user_details(result.get("user", ""))
            message["user_name"] = user_info.get("user_name")
            message["user_email"] = user_info.get("user_email")

            is_call_successful = False

            while not is_call_successful:
                try:
                    threads_response = self.client.conversations_replies(
                        channel=channel_id, ts=chat_timestamp, oldest=oldest_timestamp
                    )
                    is_call_successful = True
                except SlackApiError as e:
                    self.logger.error(f"[download_thread] Error fetching from Slack API: {e}")
                    self.logger.info("Timeout for 15s then continuing")
                    sleep(15)

            if threads_response["ok"] and len(threads_response["messages"]) > 2:  # pyright: ignore
                self.logger.info(
                    f"Number of new threads in {channel_name}: {len(threads_response['messages'])}"  # pyright: ignore
                )  # pyright: ignore
                threaded_replies = threads_response["messages"]
                del threaded_replies[0]  # pyright: ignore
                threads = []

                for thread in threaded_replies:  # pyright: ignore
                    user_info = self.get_user_details(thread.get("user"))
                    thread["user_name"] = user_info.get("user_name")
                    thread["user_email"] = user_info.get("user_email")

                    if thread.get("blocks"):
                        thread["blocks"] = [str(thread["blocks"])]

                    if thread.get("root") and thread["root"].get("attachments"):
                        for attachment in thread["root"]["attachments"]:
                            if attachment.get("bot_team_id"):
                                del attachment["bot_team_id"]
                            if attachment.get("blocks"):
                                attachment["blocks"] = [str(attachment["blocks"])]
                            if attachment.get("message_blocks"):
                                attachment["message_blocks"] = [str(attachment["message_blocks"])]
                            if attachment.get("files"):
                                del attachment["files"]

                    if thread.get("root") and thread["root"].get("blocks"):
                        thread["root"]["blocks"] = [str(thread["root"]["blocks"])]
                    if thread.get("root") and thread["root"].get("files"):
                        del thread["root"]["files"]

                    if thread.get("attachments"):
                        for attachment in thread["attachments"]:
                            if attachment.get("bot_team_id"):
                                del attachment["bot_team_id"]
                            if attachment.get("blocks"):
                                attachment["blocks"] = [str(attachment["blocks"])]
                            if attachment.get("message_blocks"):
                                attachment["message_blocks"] = [str(attachment["message_blocks"])]
                            if attachment.get("files"):
                                del attachment["files"]
                            if attachment.get("mrkdwn_in"):
                                del attachment["mrkdwn_in"]
                            if attachment.get("pinned_to"):
                                del thread["pinned_to"]
                            if attachment.get("pinned_info"):
                                del thread["pinned_info"]
                        thread["attachments"] = thread["attachments"]

                    file_paths = []
                    if thread.get("files"):
                        self._gcs_add_directory(f"files/{initial_date}/{channel_name}")
                        for file in thread.get("files"):
                            if file.get("preview_is_truncated"):
                                del file["preview_is_truncated"]
                            if file.get("url_private_download"):
                                file_path = self._download_and_verify_slack_file(
                                    file.get("url_private_download"), f"downloads/files/{initial_date}/{channel_name}"
                                )
                                if file_path:
                                    try:
                                        file_storage_path = None
                                        file_storage_path = self._gcs_add_file(
                                            file_path, f"files/{initial_date}/{channel_name}"
                                        )
                                        if file_storage_path:
                                            self.logger.info("File successfully backed to Cloud Storage.")
                                            file_paths.append(
                                                {
                                                    "timestamp": (
                                                        str(file.get("timestamp")) if file.get("timestamp") else ""
                                                    ),
                                                    "filename": file.get("name"),
                                                    "storage_url": file_storage_path,
                                                }
                                            )
                                        else:
                                            file_paths.append(
                                                {
                                                    "timestamp": (
                                                        str(file.get("timestamp")) if file.get("timestamp") else ""
                                                    ),
                                                    "filename": file.get("name"),
                                                }
                                            )
                                    except (TimeoutError, ConnectionError):
                                        continue
                    thread["files"] = file_paths
                    threads.append(thread)

                message["threads"] = threads  # pyright: ignore
                messages.append(message)
            elif not threads_response["ok"]:
                self.logger.error(f"Error fetching threads: {threads_response['error']}")
            else:
                self.logger.info(f"No new threads in {channel_name}")
        if messages:
            self.write_to_jsonl_file(
                messages, f"downloads/messages/slack_{initial_date}-{current_date}_threads_update.jsonl"
            )

    def threads_sync(self):
        tables = self.get_dataset_slack_tables()
        current_execution_date = self.get_yesterdays_date()
        Path(f"downloads/messages/").mkdir(parents=True, exist_ok=True)

        for table in tables:
            initial_date = table.split("_")[1]
            start_date = datetime.strptime(initial_date, "%Y%m%d").date() - timedelta(days=21)

            oldest_timestamp_tm = self.to_timestamp(start_date.year, start_date.month, start_date.day)

            Path(f"downloads/messages/slack_{initial_date}-{current_execution_date}_threads_update.jsonl").touch(
                exist_ok=True
            )

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
                        ARRAY_AGG(t.channel_name LIMIT 1) as `channel_name`
                    FROM
                        `{os.environ['DATASET_ID']}.{table}` as t,
                        UNNEST(threads) as thread
                    WHERE
                        t.channel_id = @channel_id
                        AND
                        (thread.ts >= @oldest_timestamp_tm OR t.ts >= @oldest_timestamp_tm)
                    GROUP BY
                        t.ts
                    ORDER BY
                        t.ts
                """
                results = self.bigquery_client.query_and_wait(
                    messages_query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("channel_id", "STRING", channel_id),
                            bigquery.ScalarQueryParameter("channel_id", "FLOAT64", oldest_timestamp_tm),
                        ]
                    ),
                )
                self.download_thread(initial_date, results)

            # Save the updated threads to Bigquery if there are newer threads
            if (
                os.stat(
                    f"downloads/messages/slack_{initial_date}-{current_execution_date}_threads_update.jsonl"
                ).st_size
                > 0
            ):
                if not self._load_to_bigquery(
                    f"downloads/messages/slack_{initial_date}-{current_execution_date}_threads_update.jsonl",
                    initial_date,
                ):
                    self.logger.info(f"Error loading data to BigQuery")
                else:
                    self.logger.info(f"Successfully loaded data to BigQuery")

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

    def _download_and_verify_slack_file(self, file_url, storage_location="downloads") -> str:
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
            with open(file_path, "rb") as f:
                # Read first few bytes to check file signature
                header = f.read(8)

            # Check common file signatures
            file_signatures = {
                b"%PDF": "PDF file",
                b"\xff\xd8\xff": "JPEG image",
                b"\x89PNG\r\n\x1a\n": "PNG image",
                b"PK\x03\x04": "ZIP archive",
                b"GIF87a": "GIF image",
                b"GIF89a": "GIF image",
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

    def _download_slack_file(self, file_url, save_dir="downloads") -> str:
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
            headers = {"Authorization": f"Bearer {self.slack_bot_token}", "User-Agent": "SlackDownloader/1.0"}

            response = requests.get(file_url, headers=headers, stream=True)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "").split(";")[0]
            content_disp = response.headers.get("content-disposition", "")

            if "filename=" in content_disp:
                filename = content_disp.split("filename=")[-1].strip('"')
                filename = filename.split('";')[0]
            else:
                ext = mimetypes.guess_extension(content_type) or ""
                filename = f"slack_file{ext}"

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            save_path = self._get_next_filename(os.path.join(save_dir, filename))

            with open(save_path, "wb") as f:
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
        """
        Load the data to Bigquery.

        Args:
            file_path: the path to the file to be uploaded.

        Returns:
            True if successful, otherwise False.
        """
        current_date = self.get_yesterdays_date()
        table_id = os.environ["DATASET_ID"] + f".slack_{current_date}"
        job_config = bigquery.LoadJobConfig(
            autodetect=slack_schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        with open(file_path, "rb") as fp:
            try:
                load_job = self.bigquery_client.load_table_from_file(
                    file_obj=fp, destination=table_id, job_config=job_config
                )
            except (ValueError, TypeError) as e:
                self.logger.error(f"[_load_to_bigquery] Error while loading to BigQuery: {e}")
                return False

            try:
                load_job.result()
            except Exception as e:
                self.logger.error(f"[_load_to_bigquery] Load job failed/did not complete: {e}")
                return False
        return True

    def start(
        self,
    ):
        try:
            self.get_slack_workspace_members()
            self.get_private_slack_channels_ids()
            self.get_public_slack_channels_ids()

            self.read_channels = self._read_checkpoints()
            self.threads_sync()
            response = self.get_slack_messages()

            while not response:
                self.logger.info("[start] Restarting download")
                self.read_channels = self._read_checkpoints()
                sleep(15)
                response = self.get_slack_messages()

        except KeyboardInterrupt:
            self.logger.warning(f"[start][KeyboardInterrupt] Stopping the app.")
        finally:
            self._stop()

    def _stop(
        self,
    ):
        """
        Stop the bot and perform clean up operations.
        """
        import shutil  # Delete the downloaded content

        try:
            # Delete the downloads folder after the bot is done
            # and is saving content to the cloud. Otherwise don't delete.
            if self.save_to_cloud:
                shutil.rmtree(self.downloads_folder)
        except Exception as e:
            self.logger.error(f"[stop] Error: {e}")
