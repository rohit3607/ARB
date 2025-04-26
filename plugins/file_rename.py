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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global dictionary to track ongoing operations
renaming_operations = {}

# Enhanced regex patterns for season and episode extraction
SEASON_EPISODE_PATTERNS = [
    # Standard patterns (S01E02, S01EP02)
    (re.compile(r'S(\d+)(?:E|EP)(\d+)'), ('season', 'episode')),
    # Patterns with spaces/dashes (S01 E02, S01-EP02)
    (re.compile(r'S(\d+)[\s-]*(?:E|EP)(\d+)'), ('season', 'episode')),
    # Full text patterns (Season 1 Episode 2)
    (re.compile(r'Season\s*(\d+)\s*Episode\s*(\d+)', re.IGNORECASE), ('season', 'episode')),
    # Patterns with brackets/parentheses ([S01][E02])
    (re.compile(r'\[S(\d+)\]\[E(\d+)\]'), ('season', 'episode')),
    # Fallback patterns (S01 13, Episode 13)
    (re.compile(r'S(\d+)[^\d]*(\d+)'), ('season', 'episode')),
    (re.compile(r'(?:E|EP|Episode)\s*(\d+)', re.IGNORECASE), (None, 'episode')),
    # Final fallback (standalone number)
    (re.compile(r'\b(\d+)\b'), (None, 'episode'))
]

# Quality detection patterns
QUALITY_PATTERNS = [
    (re.compile(r'\b(\d{3,4}[pi])\b', re.IGNORECASE), lambda m: m.group(1)),  # 1080p, 720p
    (re.compile(r'\b(4k|2160p)\b', re.IGNORECASE), lambda m: "4k"),
    (re.compile(r'\b(2k|1440p)\b', re.IGNORECASE), lambda m: "2k"),
    (re.compile(r'\b(HDRip|HDTV)\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\b(4kX264|4kx265)\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\[(\d{3,4}[pi])\]', re.IGNORECASE), lambda m: m.group(1))  # [1080p]
]

def extract_season_episode(filename):
    """Extract season and episode numbers from filename"""
    for pattern, (season_group, episode_group) in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            season = match.group(1) if season_group else None
            episode = match.group(2) if episode_group else match.group(1)
            logger.info(f"Extracted season: {season}, episode: {episode} from {filename}")
            return season, episode
    logger.warning(f"No season/episode pattern matched for {filename}")
    return None, None

def extract_quality(filename):
    """Extract quality information from filename"""
    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(filename)
        if match:
            quality = extractor(match)
            logger.info(f"Extracted quality: {quality} from {filename}")
            return quality
    logger.warning(f"No quality pattern matched for {filename}")
    return "Unknown"

async def cleanup_files(*paths):
    """Safely remove files if they exist"""
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    """Process and resize thumbnail image"""
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
    """Add metadata to media file using ffmpeg"""
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError("FFmpeg not found in PATH")
    
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
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await process.communicate()
    
    if process.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {stderr.decode()}")


# Track renaming operations
renaming_operations = {}

# Limit concurrent downloads/uploads per user
user_semaphores = {}


@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client: Client, message: Message):
    """Handle incoming file messages for renaming"""
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)

    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename.")

    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(4)  # Max 4 parallel per user

    # Immediately launch without waiting
    asyncio.create_task(process_file(client, message, user_semaphores[user_id]))


async def process_file(client: Client, message: Message, semaphore: asyncio.Semaphore):
    """Download -> Metadata -> Upload fully parallel inside user limit"""
    async with semaphore:
        user_id = message.from_user.id

        # Unique file id to prevent duplicates
        file_unique_id = (
            message.document.file_unique_id if message.document else
            message.video.file_unique_id if message.video else
            message.audio.file_unique_id if message.audio else
            None
        )
        if not file_unique_id:
            return await message.reply_text("Unsupported file type.")

        if file_unique_id in renaming_operations:
            if (datetime.now() - renaming_operations[file_unique_id]).seconds < 10:
                return  # Avoid duplicate
        renaming_operations[file_unique_id] = datetime.now()

        try:
            # Identify file
            media = message.document or message.video or message.audio
            media_type = (
                "document" if message.document else
                "video" if message.video else
                "audio"
            )
            file_name = media.file_name or f"file_{file_unique_id}"
            file_size = media.file_size

            # Extract metadata from filename
            season, episode = extract_season_episode(file_name)
            quality = extract_quality(file_name)

            # Prepare new filename
            format_template = await codeflixbots.get_format_template(user_id)
            replacements = {
                '{season}': season or 'XX',
                '{episode}': episode or 'XX',
                '{quality}': quality or 'HD',
                'Season': season or 'XX',
                'Episode': episode or 'XX',
                'QUALITY': quality or 'HD'
            }
            for key, val in replacements.items():
                format_template = format_template.replace(key, val)

            ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
            new_filename = f"{format_template}{ext}"
            download_path = f"downloads/{new_filename}"
            metadata_path = f"metadata/{new_filename}"

            # Ensure folders exist
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

            # Start downloading (no wait for others)
            status_msg = await message.reply_text("**Downloading...**")

            file_path = await asyncio.to_thread(
                client.download_media,
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", status_msg, time.time())
            )

            # Metadata processing
            await status_msg.edit("**Processing metadata...**")
            try:
                await add_metadata(file_path, metadata_path, user_id)
                file_path = metadata_path  # After metadata added
            except Exception as e:
                await status_msg.edit(f"Metadata processing failed: {e}")
                raise

            # Prepare caption and thumbnail
            caption = await get_caption(user_id) or f"**{new_filename}**"
            thumb = await get_thumbnail(user_id)
            thumb_path = None

            if thumb:
                thumb_path = await client.download_media(thumb)
            elif media_type == "video" and message.video.thumbs:
                thumb_path = await client.download_media(message.video.thumbs[0].file_id)

            thumb_path = await process_thumbnail(thumb_path)

            # Uploading
            await status_msg.edit("**Uploading...**")
            upload_params = {
                'chat_id': message.chat.id,
                'caption': caption,
                'thumb': thumb_path,
                'progress': progress_for_pyrogram,
                'progress_args': ("Uploading...", status_msg, time.time())
            }

            if media_type == "document":
                await client.send_document(document=file_path, **upload_params)
            elif media_type == "video":
                await client.send_video(video=file_path, **upload_params)
            elif media_type == "audio":
                await client.send_audio(audio=file_path, **upload_params)

            await status_msg.delete()

        except Exception as e:
            await message.reply_text(f"Error: {str(e)}")

        finally:
            # Clean temp files
            await cleanup_files(download_path, metadata_path, thumb_path)
            renaming_operations.pop(file_unique_id, None)