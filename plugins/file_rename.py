import os
import re
import time
import shutil
import asyncio
import logging
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

user_tasks = {}

SEASON_EPISODE_PATTERNS = [
    (re.compile(r'S(\d+)(?:E|EP)(\d+)'), ('season', 'episode')),
    (re.compile(r'Season\s*(\d+)\s*Episode\s*(\d+)', re.IGNORECASE), ('season', 'episode')),
    (re.compile(r'(?:E|EP|Episode)\s*(\d+)', re.IGNORECASE), (None, 'episode')),
    (re.compile(r'\b(\d+)\b'), (None, 'episode'))
]

QUALITY_PATTERNS = [
    (re.compile(r'\b(\d{3,4}[pi])\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\b(4k|2160p)\b', re.IGNORECASE), lambda m: "4k"),
    (re.compile(r'\b(2k|1440p)\b', re.IGNORECASE), lambda m: "2k"),
]

def extract_season_episode(filename):
    for pattern, (season_group, episode_group) in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if not match:
            continue

        try:
            season = match.group(1) if season_group else None
            episode = match.group(2) if episode_group and match.lastindex >= 2 else match.group(1)
            return season, episode
        except IndexError:
            continue

    return None, None

def extract_quality(filename):
    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(filename)
        if match:
            return extractor(match)
    return "Unknown"

async def cleanup_files(*paths):
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    if not thumb_path or not os.path.exists(thumb_path):
        return None
    try:
        with Image.open(thumb_path) as img:
            img = img.convert("RGB").resize((320, 320))
            img.save(thumb_path, "JPEG")
        return thumb_path
    except Exception as e:
        logger.error(f"Thumbnail processing failed: {e}")
        await cleanup_files(thumb_path)
        return None

async def add_metadata(input_path, output_path, user_id):
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError("FFmpeg not found")

    metadata = {
        'title': await codeflixbots.get_title(user_id),
        'artist': await codeflixbots.get_artist(user_id),
        'author': await codeflixbots.get_author(user_id),
        'video_title': await codeflixbots.get_video(user_id),
        'audio_title': await codeflixbots.get_audio(user_id),
        'subtitle': await codeflixbots.get_subtitle(user_id)
    }

    cmd = [
        ffmpeg,
        '-i', input_path,
        '-metadata', f'title={metadata["title"]}',
        '-metadata', f'artist={metadata["artist"]}',
        '-metadata', f'author={metadata["author"]}',
        '-metadata:s:v', f'title={metadata["video_title"]}',
        '-metadata:s:a', f'title={metadata["audio_title"]}',
        '-metadata:s:s', f'title={metadata["subtitle"]}',
        '-map', '0',
        '-c', 'copy',
        '-loglevel', 'error',
        output_path
    ]

    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, stderr = await process.communicate()

    if process.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {stderr.decode()}")

async def handle_file(client, message, media_type, file_id, file_name, file_size):
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)
    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename")

    if await check_anti_nsfw(file_name, message):
        return await message.reply_text("NSFW content detected")

    season, episode = extract_season_episode(file_name)
    quality = extract_quality(file_name)
    replacements = {
        '{season}': season or 'XX',
        '{episode}': episode or 'XX',
        '{quality}': quality,
        'Season': season or 'XX',
        'Episode': episode or 'XX',
        'QUALITY': quality
    }
    for k, v in replacements.items():
        format_template = format_template.replace(k, v)

    ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
    new_filename = f"{format_template}{ext}"
    download_path = f"downloads/{user_id}_{file_id}{ext}"
    metadata_path = f"metadata/{user_id}_{file_id}_meta{ext}"
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

    msg = await message.reply_text("**Downloading...**")
    try:
        await client.download_media(
            message,
            file_name=download_path,
            progress=progress_for_pyrogram,
            progress_args=("Downloading...", msg, time.time())
        )
    except Exception as e:
        await msg.edit(f"Download failed: {e}")
        user_tasks[user_id].remove(file_id)
        return

    await msg.edit("**Adding metadata...**")
    try:
        await add_metadata(download_path, metadata_path, user_id)
    except Exception as e:
        await msg.edit(f"Metadata error: {e}")
        user_tasks[user_id].remove(file_id)
        return

    thumb = await codeflixbots.get_thumbnail(user_id)
    thumb_path = None
    if thumb:
        thumb_path = await client.download_media(thumb)
    elif media_type == "video" and message.video.thumbs:
        thumb_path = await client.download_media(message.video.thumbs[0].file_id)

    thumb_path = await process_thumbnail(thumb_path)
    caption = await codeflixbots.get_caption(user_id) or f"**{new_filename}**"

    await msg.edit("**Uploading...**")
    try:
        upload_args = dict(chat_id=message.chat.id, caption=caption, thumb=thumb_path, progress=progress_for_pyrogram, progress_args=("Uploading...", msg, time.time()))
        if media_type == "document":
            await client.send_document(document=metadata_path, **upload_args)
        elif media_type == "video":
            await client.send_video(video=metadata_path, **upload_args)
        elif media_type == "audio":
            await client.send_audio(audio=metadata_path, **upload_args)
        await msg.delete()
    except Exception as e:
        await msg.edit(f"Upload error: {e}")
    finally:
        await cleanup_files(download_path, metadata_path, thumb_path)
        user_tasks[user_id].remove(file_id)

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_handler(client, message):
    user_id = message.from_user.id

    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        file_size = message.document.file_size
        media_type = "document"
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or "video"
        file_size = message.video.file_size
        media_type = "video"
    elif message.audio:
        file_id = message.audio.file_id
        file_name = message.audio.file_name or "audio"
        file_size = message.audio.file_size
        media_type = "audio"
    else:
        return await message.reply_text("Unsupported file type")

    user_tasks.setdefault(user_id, [])

    if len(user_tasks[user_id]) >= 4:
        return await message.reply_text("You already have 4 tasks running. Please wait...")

    if file_id in user_tasks[user_id]:
        return

    user_tasks[user_id].append(file_id)
    asyncio.create_task(handle_file(client, message, media_type, file_id, file_name, file_size))