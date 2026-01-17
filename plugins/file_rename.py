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
# Track media groups: key = (user_id, media_group_id)
media_groups = {}


async def wait_and_finalize(client, chat_id, user_id, media_group_id, media_type, caption):
    """Wait until files stop arriving for the group, then call finalize."""
    key = (user_id, media_group_id)
    group = media_groups.get(key)
    if not group:
        return

    try:
        # loop until last_update is older than threshold
        while True:
            await asyncio.sleep(1)
            if key not in media_groups:
                return
            group = media_groups.get(key)
            if not group:
                return
            if (datetime.now() - group['last_update']).total_seconds() >= 2:
                await finalize_media_group(client, chat_id, user_id, media_group_id, media_type, caption)
                return
    except Exception as e:
        logger.error("wait_and_finalize error: %s", e)
    finally:
        if key in media_groups:
            media_groups[key]['task'] = None

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
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.remove(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")


async def finalize_media_group(client, chat_id, user_id, media_group_id, media_type, caption, thumb_path=None, status_msg=None):
    """Sort stored files alphabetically and upload them sequentially."""
    key = (user_id, media_group_id)
    group = media_groups.get(key)
    if not group:
        return

    # prefer group's stored status_msg and thumb_path if available
    if not status_msg:
        status_msg = group.get('status_msg')
    if not thumb_path:
        thumb_path = group.get('thumb_path')

    # wait briefly to ensure all parts arrived
    await asyncio.sleep(2)
    # if new files arrived recently, defer finalization
    if (datetime.now() - group['last_update']).total_seconds() < 2:
        return

    files = group['files']
    if not files:
        media_groups.pop(key, None)
        return

    # Deduplicate by path and filename, then sort by renamed filename alphabetically
    unique = {}
    for item in files:
        unique[item['path']] = item
    files = list(unique.values())
    files.sort(key=lambda x: x['new_filename'].lower())

    # mark as uploading to prevent concurrent runs
    group['uploading'] = True
    files.sort(key=lambda x: x['new_filename'].lower())

    # Upload files sequentially
    # Determine user preference for upload type
    try:
        pref = await codeflixbots.get_media_preference(user_id)
    except Exception:
        pref = None

    for idx, f in enumerate(files):
        try:
            upload_params = {
                'chat_id': chat_id,
                'progress': progress_for_pyrogram,
                'progress_args': (f"Uploading {f['new_filename']}", status_msg, time.time())
            }

            # decide upload type based on user preference, falling back to detected media_type
            send_as = pref or f.get('media_type')
            if send_as not in ('video', 'audio', 'document'):
                send_as = f.get('media_type')

            # if sending as video, ensure thumb and duration
            local_thumb = thumb_path or group.get('thumb_path')
            duration = None
            if send_as == 'video':
                if not local_thumb:
                    try:
                        local_thumb, duration = await generate_thumb_and_duration(f['path'])
                    except Exception as e:
                        logger.error("Thumbnail generation failed: %s", e)

                    if local_thumb:
                        upload_params['thumb'] = local_thumb

                if duration:
                    upload_params['duration'] = duration

            # caption only for first file
            cap = caption if idx == 0 else None

            if send_as == 'video':
                await client.send_video(video=f['path'], caption=cap, **upload_params)
            elif send_as == 'audio':
                await client.send_audio(audio=f['path'], caption=cap, **upload_params)
            else:
                await client.send_document(document=f['path'], caption=cap, **upload_params)
        except Exception as e:
            logger.error("Upload failed for %s: %s", f.get('path'), e)

    # Cleanup
    for f in files:
        try:
            if os.path.exists(f['path']):
                os.remove(f['path'])
        except Exception:
            pass

    # remove the hold directory if exists and delete status message
    hold_dir = os.path.join('hold', str(user_id), str(media_group_id))
    await cleanup_files(hold_dir)
    try:
        if status_msg:
            await status_msg.delete()
    except Exception:
        pass
    media_groups.pop(key, None)

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


async def generate_thumb_and_duration(input_path):
    """Generate a thumbnail image from a video using ffmpeg and extract duration via hachoir."""
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        return None, None

    thumb_path = f"{input_path}.jpg"
    # extract a frame at 3 seconds (safe fallback)
    cmd = [ffmpeg, '-ss', '3', '-i', input_path, '-vframes', '1', '-q:v', '2', thumb_path, '-y']

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
    except Exception:
        return None, None

    # get duration via hachoir if available
    duration = None
    try:
        parser = createParser(input_path)
        metadata = extractMetadata(parser)
        if metadata and metadata.has('duration'):
            d = metadata.get('duration')
            try:
                duration = int(d.total_seconds())
            except Exception:
                try:
                    duration = int(d.seconds)
                except Exception:
                    duration = None
    except Exception:
        duration = None

    if not os.path.exists(thumb_path):
        return None, duration

    return thumb_path, duration

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    """Main handler for auto-renaming files"""
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)
    
    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename")

    # Get file information
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

    # Prevent duplicate processing
    if file_id in renaming_operations:
        if (datetime.now() - renaming_operations[file_id]).seconds < 10:
            return
    renaming_operations[file_id] = datetime.now()

    try:
        # Extract metadata from filename
        season, episode = extract_season_episode(file_name)
        quality = extract_quality(file_name)
        
        # Replace placeholders in template
        replacements = {
            '{season}': season or 'XX',
            '{episode}': episode or 'XX',
            '{quality}': quality,
            'Season': season or 'XX',
            'Episode': episode or 'XX',
            'QUALITY': quality
        }
        
        for placeholder, value in replacements.items():
            format_template = format_template.replace(placeholder, value)

        # Prepare file paths
        ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
        new_filename = f"{format_template}{ext}"
        download_path = f"downloads/{new_filename}"
        metadata_path = f"metadata/{new_filename}"
        
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        # Download file
        msg = await message.reply_text("**Downloading...**")
        try:
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", msg, time.time())
            )
        except Exception as e:
            await msg.edit(f"Download failed: {e}")
            raise

        # Process metadata
        await msg.edit("**Processing metadata...**")
        try:
            await add_metadata(file_path, metadata_path, user_id)
            file_path = metadata_path
        except Exception as e:
            await msg.edit(f"Metadata processing failed: {e}")
            raise

        # Prepare for upload
        await msg.edit("**Preparing upload...**")
        caption = await codeflixbots.get_caption(message.chat.id) or f"**{new_filename}**"
        thumb = await codeflixbots.get_thumbnail(message.chat.id)
        thumb_path = None

        # Handle thumbnail
        if thumb:
            thumb_path = await client.download_media(thumb)
        elif media_type == "video" and message.video.thumbs:
            thumb_path = await client.download_media(message.video.thumbs[0].file_id)
        
        thumb_path = await process_thumbnail(thumb_path)

        # Instead of uploading immediately, store renamed files in a hold directory
        await msg.edit("**Holding file and preparing sequence...**")

        # Use media_group_id when present; otherwise use message id to treat single file as its own group
        media_group_id = str(message.media_group_id or message.message_id)
        hold_dir = os.path.join('hold', str(user_id), str(media_group_id))
        os.makedirs(hold_dir, exist_ok=True)

        held_path = os.path.join(hold_dir, new_filename)
        try:
            shutil.move(file_path, held_path)
        except Exception:
            # fallback to copy
            shutil.copy2(file_path, held_path)

        key = (user_id, media_group_id)
        entry = {
            'path': held_path,
            'new_filename': new_filename,
            'media_type': media_type
        }

        if key not in media_groups:
            media_groups[key] = {'files': [], 'last_update': datetime.now(), 'uploading': False}

        media_groups[key]['files'].append(entry)
        media_groups[key]['last_update'] = datetime.now()

        # Store the group's status message and thumb_path (use first msg as the group's single status message)
        if not media_groups[key].get('status_msg'):
            media_groups[key]['status_msg'] = msg
        # store thumb path for group if not set
        if thumb_path and not media_groups[key].get('thumb_path'):
            media_groups[key]['thumb_path'] = thumb_path

        # Schedule a single waiter task per group to finalize once no new files arrive
        if not media_groups[key].get('task'):
            media_groups[key]['task'] = asyncio.create_task(wait_and_finalize(client, message.chat.id, user_id, media_group_id, media_type, caption))
        # If this is a single file (not an album), the waiter will finalize shortly.
        # Keep the per-file holding messages visible; a single group's status message will be edited during upload.

    except Exception as e:
        logger.error(f"Processing error: {e}")
        await message.reply_text(f"Error: {str(e)}")
    finally:
        # Clean up files
        await cleanup_files(download_path, metadata_path, thumb_path)
        renaming_operations.pop(file_id, None)
