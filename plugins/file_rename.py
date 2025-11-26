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

# Track renaming operations
renaming_operations = {}

# Limit concurrent downloads/uploads per user
user_semaphores = {}

# Enhanced regex patterns for season and episode extraction
SEASON_EPISODE_PATTERNS = [
    # Priority 1: Most specific and unambiguous patterns first
    
    # Standard SxxExx formats (highest confidence)
    (re.compile(r'\bS(\d{1,2})[\.\-_]?E(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # S01E04, S1-E4, S01.E04
    (re.compile(r'\bS(\d{1,2})\s+E(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # S01 E04
    (re.compile(r'\[S(\d{1,2})[\.\-_]?E(\d{1,3})\]', re.IGNORECASE), (1, 2)),  # [S01E04]
    (re.compile(r'\(S(\d{1,2})[\.\-_]?E(\d{1,3})\)', re.IGNORECASE), (1, 2)),  # (S01E04)
    
    # xxExx formats
    (re.compile(r'\b(\d{1,2})x(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # 1x04, 01x123
    (re.compile(r'\[(\d{1,2})x(\d{1,3})\]', re.IGNORECASE), (1, 2)),  # [1x04]
    (re.compile(r'\((\d{1,2})x(\d{1,3})\)', re.IGNORECASE), (1, 2)),  # (1x04)
    
    # Season/Episode explicit formats
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})[\s\-_.]*Episode[\s\-_.]*(\d{1,3})\b', re.IGNORECASE), (1, 2)),
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})[\s\-_.]*Ep[\s\-_.]*(\d{1,3})\b', re.IGNORECASE), (1, 2)),
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})[\s\-_.]*E[\s\-_.]*(\d{1,3})\b', re.IGNORECASE), (1, 2)),
    
    # Separated bracket formats
    (re.compile(r'\[S(\d{1,2})\][\s\-_.]*\[E(\d{1,3})\]', re.IGNORECASE), (1, 2)),  # [S01][E04]
    (re.compile(r'\(S(\d{1,2})\)[\s\-_.]*\(E(\d{1,3})\)', re.IGNORECASE), (1, 2)),  # (S01)(E04)
    
    # Dot and dash separated formats
    (re.compile(r'\bS(\d{1,2})\.(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # S01.04
    (re.compile(r'\bS(\d{1,2})\-(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # S01-04
    (re.compile(r'\b(\d{1,2})\.(\d{1,3})\b(?!p|fps)', re.IGNORECASE), (1, 2)),  # 1.04 (exclude quality)
    (re.compile(r'\b(\d{1,2})\-(\d{1,3})\b(?!p|fps)', re.IGNORECASE), (1, 2)),  # 1-04 (exclude quality)
    
    # Priority 2: Less specific but still reliable patterns
    
    # Space separated formats
    (re.compile(r'\bS\s*(\d{1,2})\s+(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # S 01 04
    (re.compile(r'\bSeason\s*(\d{1,2})\s+(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # Season 1 04
    
    # Episode-first formats
    (re.compile(r'\bE(\d{1,3})[\s\-_.]*S(\d{1,2})\b', re.IGNORECASE), (2, 1)),  # E04 S01
    (re.compile(r'\bEp[\s\-_.]*(\d{1,3})[\s\-_.]*S(\d{1,2})\b', re.IGNORECASE), (2, 1)),  # Ep 04 S01
    
    # Priority 3: Episode-only patterns (with better context)
    (re.compile(r'(?:^|[\s\-_.(\[])E(\d{2,4})(?=[\s\-_.)\]]|$)(?!p|fps)', re.IGNORECASE), (None, 1)),  # E04, [E04], (E04)
    (re.compile(r'(?:^|[\s\-_.(\[])Episode[\s\-_.]*(\d{1,3})(?=[\s\-_.)\]]|$)', re.IGNORECASE), (None, 1)),  # Episode 04
    (re.compile(r'(?:^|[\s\-_.(\[])Ep[\s\-_.]*(\d{1,3})(?=[\s\-_.)\]]|$)', re.IGNORECASE), (None, 1)),  # Ep 04
    
    # Group tag followed by episode
    (re.compile(r'\[[A-Za-z0-9\-]+\][\s\-_.]+E(\d{1,3})(?![\dp])', re.IGNORECASE), (None, 1)),  # [Group] Title E04
    (re.compile(r'\[[A-Za-z0-9\-]+\][\s\-_.]+Episode[\s\-_.]*(\d{1,3})(?![\dp])', re.IGNORECASE), (None, 1)),  # [Group] Title Episode 04
    
    # Priority 4: Generic patterns (lower confidence)
    (re.compile(r'(?:^|[\s\-_.])(\d{2,3})(?=[\s\-_.]|$)(?!p|fps|\d)', re.IGNORECASE), (None, 1)),  # 04, -04.
    (re.compile(r'\[(\d{2,3})\](?!p|fps)', re.IGNORECASE), (None, 1)),  # [04]
    
    # Season-only patterns (lowest priority)
    (re.compile(r'\bS(\d{1,2})\b(?![\dE])', re.IGNORECASE), (1, None)),  # S01 (not followed by E or digit)
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})\b', re.IGNORECASE), (1, None)),  # Season 1
#---

    # Very specific patterns with strict boundaries
    (re.compile(r'^S(\d{2})E(\d{2})$', re.IGNORECASE), (1, 2)),  # S01E04
    (re.compile(r'^(\d{1,2})x(\d{2})$', re.IGNORECASE), (1, 2)),  # 1x04
    (re.compile(r'^Season\s*(\d{1,2})\s*Episode\s*(\d{1,2})$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^S(\d{2})\s*-\s*E(\d{2})$', re.IGNORECASE), (1, 2)),  # S01 - E04
    
    # Bracket-enclosed exact patterns
    (re.compile(r'^\[S(\d{2})E(\d{2})\]$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^\[(\d{1,2})x(\d{2})\]$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^\(S(\d{2})E(\d{2})\)$', re.IGNORECASE), (1, 2)),
    
    # Dot-separated exact patterns
    (re.compile(r'^S(\d{2})\.E(\d{2})$', re.IGNORECASE), (1, 2)),  # S01.E04
    (re.compile(r'^(\d{1,2})\.(\d{2})$', re.IGNORECASE), (1, 2)),  # 1.04
    
    # Episode-only exact patterns
    (re.compile(r'^E(\d{2,3})$', re.IGNORECASE), (None, 1)),  # E04, E104
    (re.compile(r'^Episode\s*(\d{1,3})$', re.IGNORECASE), (None, 1)),
    (re.compile(r'^\[E(\d{2,3})\]$', re.IGNORECASE), (None, 1)),
]



# Quality detection patterns
QUALITY_PATTERNS = [
    (re.compile(r'(?<!\d)(144p)(?!\d)', re.IGNORECASE), lambda m: "144p"),
    (re.compile(r'(?<!\d)(240p)(?!\d)', re.IGNORECASE), lambda m: "240p"),
    (re.compile(r'(?<!\d)(360p)(?!\d)', re.IGNORECASE), lambda m: "360p"),
    (re.compile(r'(?<!\d)(480p)(?!\d)', re.IGNORECASE), lambda m: "480p"),
    (re.compile(r'\bSD\b', re.IGNORECASE), lambda m: "480p"),
    (re.compile(r'(?<!\d)(540p)(?!\d)', re.IGNORECASE), lambda m: "540p"),
    (re.compile(r'(?<!\d)(720p)(?!\d)', re.IGNORECASE), lambda m: "720p"),
    (re.compile(r'\bHD\b', re.IGNORECASE), lambda m: "720p"),
    (re.compile(r'(?<!\d)(1080p)(?!\d)', re.IGNORECASE), lambda m: "1080p"),
    (re.compile(r'\bFHD\b', re.IGNORECASE), lambda m: "1080p"),
    (re.compile(r'(?<!\d)(1440p)(?!\d)', re.IGNORECASE), lambda m: "1440p"),
    (re.compile(r'(?<!\d)(2160p)(?!\d)', re.IGNORECASE), lambda m: "2160p"),
    (re.compile(r'\b4k\b', re.IGNORECASE), lambda m: "2160p"),
    (re.compile(r'[_\-. ](144p|240p|360p|480p|540p|720p|1080p|1440p|2160p)(?=[_\-. ])', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\bHDRip\b', re.IGNORECASE), lambda m: "HDRip"),
]


def extract_season_episode(filename):
    import re
    filename = re.sub(r'\(.*?\)', ' ', filename)

    for pattern, group_info in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            season = episode = None
            if isinstance(group_info, tuple):
                try:
                    if group_info[0] is not None:
                        group_num = int(group_info[0])
                        if match.lastindex and group_num <= match.lastindex:
                            season = match.group(group_num).zfill(2) if match.group(group_num) else "01"
                        else:
                            continue
                    else:
                        season = "01"
                    
                    if group_info[1] is not None:
                        group_num = int(group_info[1])
                        if match.lastindex and group_num <= match.lastindex:
                            episode = match.group(group_num).zfill(2) if match.group(group_num) else None
                        else:
                            continue
                except (ValueError, IndexError, AttributeError):
                    continue

                if episode:
                    return season, episode

    return "01", None  # Default values if no match found

def extract_part(filename):
    filename = re.sub(r'\(.*?\)', ' ', filename)  # Clean parentheses content

    for pattern, (part_group,) in PART_PATTERNS:
        match = pattern.search(filename)
        if match:
            part = match.group(1).zfill(2)
            logger.info(f"Extracted part: {part} from {filename}")
            return part

    logger.warning(f"No part pattern matched for {filename}")
    return None

def extract_quality(filename):
    seen = set()
    quality_parts = []

    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(filename)
        if match:
            quality = extractor(match).lower()
            if quality not in seen:
                quality_parts.append(quality)
                seen.add(quality)
                filename = filename.replace(match.group(0), '', 1)

    resolution_qualities = [q for q in quality_parts if q.endswith("p") or q == "4k" or q == "2160p"]
    
    if resolution_qualities:
        return resolution_qualities[0] 
    elif "HDrip" in quality_parts:
        return "HDRip"
    
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

async def process_thumbnail_pdf(thumb_path, video_path=None):
    if not thumb_path or not os.path.exists(thumb_path):
        print("PDF thumbnail not found.")
        # Try generating from video
        if video_path and os.path.exists(video_path):
            output_path = os.path.join("downloads", f"{Path(video_path).stem}_pdf_thumb.jpg")
            thumb_path = await asyncio.to_thread(generate_thumbnail, video_path, output_path)
            if thumb_path:
                print("Generated thumbnail from video for PDF.")
            else:
                print("Failed to generate PDF thumbnail from video.")
                return None
        else:
            print("No valid video path provided for PDF thumbnail generation.")
            return None
    else:
        print("Using user-provided thumbnail for PDF.")

    try:
        # Open and convert to RGB
        img = await asyncio.to_thread(Image.open, thumb_path)
        img = await asyncio.to_thread(lambda: img.convert("RGB"))

        # Resize to fit within 320x320 (maintains aspect ratio)
        img = await asyncio.to_thread(lambda: ImageOps.contain(img, (320, 320), Image.LANCZOS))

        # Pad to exactly 320x320 (black background)
        img = await asyncio.to_thread(lambda: ImageOps.pad(img, (320, 320), color=(0, 0, 0)))

        # Save with high quality
        await asyncio.to_thread(img.save, thumb_path, "JPEG", quality=95)

        print(f"PDF Thumbnail processed successfully: {thumb_path}")
        return thumb_path

    except Exception as e:
        print(f"PDF Thumbnail processing failed: {e}")
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
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


user_semaphores = {}
renaming_operations = {}

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client: Client, message: Message):
    """Handle incoming file messages for renaming"""
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)

    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename.")

    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(4)  # Max 4 parallel per user

    asyncio.create_task(process_file(client, message, user_semaphores[user_id]))

async def process_file(client: Client, message: Message, semaphore: asyncio.Semaphore):
    """Download -> Metadata -> Upload fully parallel inside user limit"""
    async with semaphore:
        user_id = message.from_user.id

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
            media = message.document or message.video or message.audio
            media_type = (
                "document" if message.document else
                "video" if message.video else
                "audio"
            )
            file_name = media.file_name or f"file_{file_unique_id}"

            # Extract season, episode, quality
            season, episode = extract_season_episode(file_name)
            quality = extract_quality(file_name)

            # Format new filename
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

            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

            # Download
            status_msg = await message.reply_text("**Downloading...**")
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", status_msg, time.time())
            )

            # Add metadata
            await status_msg.edit("**Processing metadata...**")
            try:
                await add_metadata(file_path, metadata_path, user_id)
                file_path = metadata_path
            except Exception as e:
                await status_msg.edit(f"Metadata processing failed: {e}")
                raise

            # Prepare caption and thumbnail
            caption = await codeflixbots.get_caption(user_id) or f"**{new_filename}**"
            thumb = await codeflixbots.get_thumbnail(user_id)
            thumb_path = None

            if thumb:
                thumb_path = await client.download_media(thumb)
            elif media_type == "video" and message.video.thumbs:
                thumb_path = await client.download_media(message.video.thumbs[0].file_id)

            thumb_path = await process_thumbnail(thumb_path)

            # Upload
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
            await cleanup_files(download_path, metadata_path, thumb_path)
            renaming_operations.pop(file_unique_id, None)