import asyncio
import logging
import os
import shutil
from typing import List, Optional, Tuple, Union

import pyrogram
import yaml
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from utils.file_management import get_next_name, manage_duplicate_file
from utils.log import LogFilter
from utils.meta import print_meta
from utils.updates import check_for_updates

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to INFO to show general progress logs
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)
logger = logging.getLogger("media_downloader")

# Global variables
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_DIR = os.path.join(THIS_DIR, "downloads")
FAILED_IDS: list = []
DOWNLOADED_IDS: list = []
TOTAL_FILES: int = 0  # To track total files in the entire chat


def log_start_end(action: str):
    """Log the start and end of the script."""
    if action == "start":
        logger.info("Script started.")
    elif action == "end":
        logger.info("Script ended.")


def update_config(config: dict, chat_id: str):
    """
    Update existing configuration file and copy it to the chat-specific directory.

    Parameters
    ----------
    config: dict
        Configuration to be written into config file.
    chat_id: str
        ID of the chat where the media was downloaded.
    """
    config["ids_to_retry"] = (
        list(set(config["ids_to_retry"]) - set(DOWNLOADED_IDS)) + FAILED_IDS
    )
    config_path = os.path.join(THIS_DIR, "config.yaml")
    with open(config_path, "w") as yaml_file:
        yaml.dump(config, yaml_file, default_flow_style=False)

    chat_config_dir = os.path.join(DOWNLOADS_DIR, chat_id)
    os.makedirs(chat_config_dir, exist_ok=True)
    chat_config_path = os.path.join(chat_config_dir, "config.yaml")
    shutil.copy(config_path, chat_config_path)


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded, else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists, else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)


def _create_directories(chat_id: str, media_type: str) -> str:
    """
    Create directories for downloads.

    Parameters
    ----------
    chat_id: str
        ID of the chat where the media was sent.
    media_type: str
        Type of media (e.g., photos, videos, documents).

    Returns
    -------
    str
        The directory path where the file will be saved.
    """
    dir_path = os.path.join(DOWNLOADS_DIR, chat_id, media_type)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def _is_valid_file(file_path: str, min_size: int = 512) -> bool:
    """
    Check if the downloaded file is valid based on its size.

    Parameters
    ----------
    file_path: str
        The path to the downloaded file.
    min_size: int
        The minimum acceptable file size in bytes. Defaults to 512 bytes.

    Returns
    -------
    bool
        True if the file is valid, False if it is empty or below the minimum size.
    """
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        logger.error("File is empty: %s", file_path)
        return False
    elif file_size < min_size:
        logger.info("File is small but not empty: %s (Size: %d bytes)", file_path, file_size)
        return True  # Allow small but non-empty files

    return True


def _sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by replacing invalid characters with underscores.

    Parameters
    ----------
    filename: str
        The original filename to be sanitized.

    Returns
    -------
    str
        Sanitized filename with invalid characters replaced.
    """
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "_")
    return filename


async def _get_media_meta(
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
    chat_id: str,
    message_id: int,
) -> Tuple[str, Optional[str]]:
    """
    Extract file name and file extension from media object, with message_id prefix.

    Parameters
    ----------
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice]
        Media object to be extracted.
    _type: str
        Type of media object.
    chat_id: str
        ID of the chat where the media was sent.
    message_id: int
        ID of the message.

    Returns
    -------
    Tuple[str, Optional[str]]
        file_name, file_format
    """
    if _type in ["audio", "document", "video", "voice"]:
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]  # type: ignore
        file_extension = f".{file_format}"
    else:
        file_format = None
        file_extension = ".jpg" if _type == "photo" else ".mp4" if _type == "video_note" else ""

    original_file_name = getattr(media_obj, "file_name", None) or f"{_type}_{media_obj.date.isoformat()}"
    sanitized_file_name = _sanitize_filename(original_file_name)
    if not sanitized_file_name.endswith(file_extension):
        sanitized_file_name += file_extension

    file_name = os.path.join(
        _create_directories(chat_id, _type),
        f"{message_id}_{sanitized_file_name}",
    )
    return file_name, file_format


async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
):
    """
    Download media from Telegram, prefixing file names with message_id.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """
    chat_id = str(message.chat.id)
    message_id = message.id
    for retry in range(3):
        try:
            if message.media is None:
                return message.id
            for _type in media_types:
                _media = getattr(message, _type, None)
                if _media is None:
                    continue
                file_name, file_format = await _get_media_meta(_media, _type, chat_id, message_id)
                if _can_download(_type, file_formats, file_format):
                    if _is_exist(file_name):
                        file_name = get_next_name(file_name)
                    download_path = await client.download_media(message, file_name=file_name)

                    if download_path:
                        if not _is_valid_file(download_path):
                            os.remove(download_path)
                            logger.error(
                                "Download failed - %s (Message ID: %d, Chat ID: %s). File too small or empty.",
                                download_path,
                                message_id,
                                chat_id,
                            )
                            FAILED_IDS.append(message_id)
                            continue

                        download_path = manage_duplicate_file(download_path)  # type: ignore
                        logger.info(
                            "Downloaded successfully - %s (Message ID: %d, Total Files: %d)",
                            download_path,
                            message_id,
                            TOTAL_FILES,
                        )
                        DOWNLOADED_IDS.append(message_id)
            break
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            message = await client.get_messages(chat_id=message.chat.id, message_ids=message_id)
            if retry == 2:
                logger.error(
                    "Download failed - Message ID: %d (File reference expired).",
                    message_id,
                )
                FAILED_IDS.append(message_id)
        except TypeError:
            await asyncio.sleep(5)
            if retry == 2:
                logger.error(
                    "Download failed - Message ID: %d (Timeout after 3 retries).",
                    message_id,
                )
                FAILED_IDS.append(message_id)
        except Exception as e:
            logger.error(
                "Download failed - Message ID: %d due to exception: %s.",
                message_id,
                e,
            )
            FAILED_IDS.append(message_id)
            break
    return message_id


async def process_messages(
    client: pyrogram.client.Client,
    messages: List[pyrogram.types.Message],
    media_types: List[str],
    file_formats: dict,
) -> int:
    """
    Download media from Telegram.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    messages: list
        List of telegram messages.
    media_types: list
        List of strings of media types to be downloaded.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Max value of list of message ids.
    """
    message_ids = await asyncio.gather(
        *[
            download_media(client, message, media_types, file_formats)
            for message in messages
        ]
    )

    last_message_id: int = max(message_ids)
    return last_message_id


async def count_total_files(client: pyrogram.client.Client, chat_id: int) -> int:
    """
    Count the total number of media files available for download in the chat.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    chat_id: int
        ID of the chat where media files are located.

    Returns
    -------
    int
        Total number of media files in the chat.
    """
    total_files = 0
    async for message in client.get_chat_history(chat_id, reverse=True):
        if message.media:
            total_files += 1
    return total_files


async def begin_import(config: dict, pagination_limit: int) -> dict:
    """
    Create pyrogram client and initiate download.

    Parameters
    ----------
    config: dict
        Dict containing the config to create pyrogram client.
    pagination_limit: int
        Number of messages to download asynchronously as a batch.

    Returns
    -------
    dict
        Updated configuration to be written into config file.
    """
    client = pyrogram.Client(
        "media_downloader",
        api_id=config["api_id"],
        api_hash=config["api_hash"],
        proxy=config.get("proxy"),
    )
    await client.start()
    global TOTAL_FILES
    TOTAL_FILES = await count_total_files(client, config["chat_id"])

    last_read_message_id: int = config["last_read_message_id"]
    messages_iter = client.get_chat_history(
        config["chat_id"], offset_id=last_read_message_id, reverse=True
    )
    messages_list: list = []
    pagination_count: int = 0
    if config["ids_to_retry"]:
        skipped_messages: list = await client.get_messages(
            chat_id=config["chat_id"], message_ids=config["ids_to_retry"]
        )
        for message in skipped_messages:
            pagination_count += 1
            messages_list.append(message)

    async for message in messages_iter:
        if pagination_count != pagination_limit:
            pagination_count += 1
            messages_list.append(message)
        else:
            last_read_message_id = await process_messages(
                client,
                messages_list,
                config["media_types"],
                config["file_formats"],
            )
            pagination_count = 0
            messages_list = []
            messages_list.append(message)
            config["last_read_message_id"] = last_read_message_id
            update_config(config, str(config["chat_id"]))
    if messages_list:
        last_read_message_id = await process_messages(
            client,
            messages_list,
            config["media_types"],
            config["file_formats"],
        )

    await client.stop()
    config["last_read_message_id"] = last_read_message_id
    return config


def main():
    """Main function of the downloader."""
    log_start_end("start")
    with open(os.path.join(THIS_DIR, "config.yaml")) as f:
        config = yaml.safe_load(f)
    updated_config = asyncio.get_event_loop().run_until_complete(
        begin_import(config, pagination_limit=100)
    )
    if FAILED_IDS:
        logger.error(
            "%d downloads failed. Failed message IDs added to config file.",
            len(set(FAILED_IDS)),
        )
    update_config(updated_config, str(config["chat_id"]))
    check_for_updates()
    log_start_end("end")


if __name__ == "__main__":
    print_meta(logger)
    main()
