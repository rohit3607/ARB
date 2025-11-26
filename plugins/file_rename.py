import os
import re
import time
import shutil
import asyncio
import logging
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageOps
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Track renaming operations (avoid duplicates)
renaming_operations: dict[str, datetime] = {}

# Per-user job queue: user_id -> list[(sort_key, Message)]
user_jobs: dict[int, list[tuple[float, Message]]] = {}

# Per-user worker task
user_workers: dict[int, asyncio.Task] = {}

# -------------------- REGEX PATTERNS --------------------

SEASON_EPISODE_PATTERNS = [
    # Priority 1: Most specific and unambiguous patterns first
    (re.compile(r'\bS(\d{1,2})[\.\-_]?E(\d{1,3})\b', re.IGNORECASE), (1, 2)),  # S01E04, S1-E4, S01.E04
    (re.compile(r'\bS(\d{1,2})\s+E(\d{1,3})\b', re.IGNORECASE), (1, 2)),        # S01 E04
    (re.compile(r'\[S(\d{1,2})[\.\-_]?E(\d{1,3})\]', re.IGNORECASE), (1, 2)),   # [S01E04]
    (re.compile(r'\(S(\d{1,2})[\.\-_]?E(\d{1,3})\)', re.IGNORECASE), (1, 2)),   # (S01E04)

    # xxExx formats
    (re.compile(r'\b(\d{1,2})x(\d{1,3})\b', re.IGNORECASE), (1, 2)),            # 1x04, 01x123
    (re.compile(r'\[(\d{1,2})x(\d{1,3})\]', re.IGNORECASE), (1, 2)),            # [1x04]
    (re.compile(r'\((\d{1,2})x(\d{1,3})\)', re.IGNORECASE), (1, 2)),            # (1x04)

    # Season/Episode explicit formats
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})[\s\-_.]*Episode[\s\-_.]*(\d{1,3})\b', re.IGNORECASE), (1, 2)),
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})[\s\-_.]*Ep[\s\-_.]*(\d{1,3})\b', re.IGNORECASE), (1, 2)),
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})[\s\-_.]*E[\s\-_.]*(\d{1,3})\b', re.IGNORECASE), (1, 2)),

    # Separated bracket formats
    (re.compile(r'\[S(\d{1,2})\][\s\-_.]*\[E(\d{1,3})\]', re.IGNORECASE), (1, 2)),  # [S01][E04]
    (re.compile(r'\(S(\d{1,2})\)[\s\-_.]*\(E(\d{1,3})\)', re.IGNORECASE), (1, 2)),  # (S01)(E04)

    # Dot and dash separated formats
    (re.compile(r'\bS(\d{1,2})\.(\d{1,3})\b', re.IGNORECASE), (1, 2)),          # S01.04
    (re.compile(r'\bS(\d{1,2})\-(\d{1,3})\b', re.IGNORECASE), (1, 2)),          # S01-04
    (re.compile(r'\b(\d{1,2})\.(\d{1,3})\b(?!p|fps)', re.IGNORECASE), (1, 2)),  # 1.04 (exclude quality)
    (re.compile(r'\b(\d{1,2})\-(\d{1,3})\b(?!p|fps)', re.IGNORECASE), (1, 2)),  # 1-04 (exclude quality)

    # Priority 2: Less specific but still reliable patterns
    (re.compile(r'\bS\s*(\d{1,2})\s+(\d{1,3})\b', re.IGNORECASE), (1, 2)),      # S 01 04
    (re.compile(r'\bSeason\s*(\d{1,2})\s+(\d{1,3})\b', re.IGNORECASE), (1, 2)), # Season 1 04

    # Episode-first formats
    (re.compile(r'\bE(\d{1,3})[\s\-_.]*S(\d{1,2})\b', re.IGNORECASE), (2, 1)),  # E04 S01
    (re.compile(r'\bEp[\s\-_.]*(\d{1,3})[\s\-_.]*S(\d{1,2})\b', re.IGNORECASE), (2, 1)),  # Ep 04 S01

    # Priority 3: Episode-only patterns (with better context)
    (re.compile(r'(?:^|[\s\-_.(\[])E(\d{2,4})(?=[\s\-_.)\]]|$)(?!p|fps)', re.IGNORECASE), (None, 1)),
    (re.compile(r'(?:^|[\s\-_.(\[])Episode[\s\-_.]*(\d{1,3})(?=[\s\-_.)\]]|$)', re.IGNORECASE), (None, 1)),
    (re.compile(r'(?:^|[\s\-_.(\[])Ep[\s\-_.]*(\d{1,3})(?=[\s\-_.)\]]|$)', re.IGNORECASE), (None, 1)),

    # Group tag followed by episode
    (re.compile(r'\[[A-Za-z0-9\-]+\][\s\-_.]+E(\d{1,3})(?![\dp])', re.IGNORECASE), (None, 1)),
    (re.compile(r'\[[A-Za-z0-9\-]+\][\s\-_.]+Episode[\s\-_.]*(\d{1,3})(?![\dp])', re.IGNORECASE), (None, 1)),

    # Priority 4: Generic patterns (lower confidence)
    (re.compile(r'(?:^|[\s\-_.])(\d{2,3})(?=[\s\-_.]|$)(?!p|fps|\d)', re.IGNORECASE), (None, 1)),
    (re.compile(r'\[(\d{2,3})\](?!p|fps)', re.IGNORECASE), (None, 1)),

    # Season-only patterns (lowest priority)
    (re.compile(r'\bS(\d{1,2})\b(?![\dE])', re.IGNORECASE), (1, None)),
    (re.compile(r'\bSeason[\s\-_.]*(\d{1,2})\b', re.IGNORECASE), (1, None)),

    # Very specific exact patterns
    (re.compile(r'^S(\d{2})E(\d{2})$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^(\d{1,2})x(\d{2})$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^Season\s*(\d{1,2})\s*Episode\s*(\d{1,2})$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^S(\d{2})\s*-\s*E(\d{2})$', re.IGNORECASE), (1, 2)),

    # Bracket-enclosed exact patterns
    (re.compile(r'^\[S(\d{2})E(\d{2})\]$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^\[(\d{1,2})x(\d{2})\]$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^\(S(\d{2})E(\d{2})\)$', re.IGNORECASE), (1, 2)),

    # Dot-separated exact patterns
    (re.compile(r'^S(\d{2})\.E(\d{2})$', re.IGNORECASE), (1, 2)),
    (re.compile(r'^(\d{1,2})\.(\d{2})$', re.IGNORECASE), (1, 2)),

    # Episode-only exact patterns
    (re.compile(r'^E(\d{2,3})$', re.IGNORECASE), (None, 1)),
    (re.compile(r'^Episode\s*(\d{1,3})$', re.IGNORECASE), (None, 1)),
    (re.compile(r'^\[E(\d{2,3})\]$', re.IGNORECASE), (None, 1)),
]

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

# -------------------- HELPERS --------------------


def extract_season_episode(filename: str):
    """
    Extract season and episode from filename.

    Special handling for manga-style chapters:
    [ Ch 31 ], [ Ch 31.5 ], Ch 36.6, etc.
    """
    # Remove text inside parentheses to reduce noise
    filename = re.sub(r'\(.*?\)', ' ', filename)

    # 1) Decimal chapter / episode patterns first (for things like "Ch 31.5", "[ Ch 36.6 ]")
    decimal_patterns = [
        re.compile(r'\[(?:\s*Ch(?:apter)?\s+(\d{1,3}(?:\.\d{1,2})?)\s*)\]', re.IGNORECASE),  # [ Ch 31.5 ]
        re.compile(r'\bCh(?:apter)?\s*(\d{1,3}(?:\.\d{1,2})?)\b', re.IGNORECASE),            # Ch 31.5
        re.compile(r'\b(\d{1,3}\.\d{1,2})\b', re.IGNORECASE),                                # 31.5 (plain)
    ]

    for pattern in decimal_patterns:
        m = pattern.search(filename)
        if m:
            episode = m.group(1)  # keep as string, don't zfill; preserves 31.5
            season = "01"
            return season, episode

    # 2) Fallback to standard SxxExx patterns etc.
    for pattern, group_info in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            season = None
            episode = None
            if isinstance(group_info, tuple):
                try:
                    # Season
                    if group_info[0] is not None:
                        g = int(group_info[0])
                        if match.lastindex and g <= match.lastindex:
                            # keep 2-digit season for neatness
                            season = match.group(g).zfill(2) if match.group(g) else "01"
                        else:
                            continue
                    else:
                        season = "01"

                    # Episode (keep as-is without zfill for template)
                    if group_info[1] is not None:
                        g = int(group_info[1])
                        if match.lastindex and g <= match.lastindex:
                            episode = match.group(g) if match.group(g) else None
                        else:
                            continue
                except (ValueError, IndexError, AttributeError):
                    continue

                if episode:
                    return season, episode

    # Default if nothing matched
    return "01", None


def extract_quality(filename: str) -> str:
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


def get_episode_sort_key(filename: str) -> float:
    """
    Convert extracted episode (e.g. '31', '31.5', '36.6') into a float for sorting.
    If not found or invalid, return +inf to push it to the end.
    """
    _, episode = extract_season_episode(filename)
    if not episode:
        return float('inf')
    try:
        return float(episode)
    except ValueError:
        return float('inf')


async def cleanup_files(*paths: str):
    """Safely remove files if they exist"""
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")


async def generate_pdf_thumbnail(pdf_path: str) -> str | None:
    """
    Generate a thumbnail from the first page of a PDF using pdftoppm.
    Returns path to JPEG thumbnail or None.
    """
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        logger.warning("pdftoppm not found; cannot generate PDF thumbnail.")
        return None

    thumb_dir = "thumbnails"
    os.makedirs(thumb_dir, exist_ok=True)

    stem = Path(pdf_path).stem
    output_prefix = os.path.join(thumb_dir, stem)

    cmd = [
        pdftoppm,
        "-jpeg",
        "-f", "1",
        "-singlefile",
        pdf_path,
        output_prefix
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        logger.error(f"pdftoppm error: {stderr.decode().strip()}")
        return None

    thumb_path = f"{output_prefix}.jpg"
    if not os.path.exists(thumb_path):
        return None

    return thumb_path


async def process_pdf_thumbnail(thumb_path: str) -> str | None:
    """
    Resize & pad thumbnail to 320x320, good for Telegram.
    """
    if not thumb_path or not os.path.exists(thumb_path):
        return None

    try:
        img = await asyncio.to_thread(Image.open, thumb_path)
        img = await asyncio.to_thread(lambda: img.convert("RGB"))
        # Maintain aspect ratio inside 320x320
        img = await asyncio.to_thread(lambda: ImageOps.contain(img, (320, 320), Image.LANCZOS))
        # Pad to exactly 320x320 with black background
        img = await asyncio.to_thread(lambda: ImageOps.pad(img, (320, 320), color=(0, 0, 0)))
        await asyncio.to_thread(img.save, thumb_path, "JPEG", quality=95)
        return thumb_path
    except Exception as e:
        logger.error(f"PDF thumbnail processing failed: {e}")
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
        return None


# -------------------- WORKER LOGIC (SORTED BY EPISODE) --------------------


async def user_worker(client: Client, user_id: int):
    """
    Per-user worker that takes jobs from user_jobs[user_id],
    sorts them by episode number, and processes them in that order.
    """
    # Small delay to allow "burst" of messages to queue up
    await asyncio.sleep(2)

    try:
        while True:
            jobs = user_jobs.get(user_id, [])
            if not jobs:
                break

            # Sort by numeric chapter (episode)
            jobs.sort(key=lambda item: item[0])  # item = (sort_key, Message)
            sort_key, msg = jobs.pop(0)

            # Process that PDF
            await process_single_pdf(client, msg)

    finally:
        # Cleanup when done
        user_workers.pop(user_id, None)
        user_jobs.pop(user_id, None)


# -------------------- MAIN HANDLER (PDF ONLY) --------------------


@Client.on_message(filters.private & filters.document)
async def auto_rename_pdfs(client: Client, message: Message):
    """Handle incoming PDF documents for auto rename, and queue them by chapter."""
    user_id = message.from_user.id
    document = message.document

    # Only allow PDFs
    if not document:
        return

    mime = document.mime_type or ""
    filename = document.file_name or ""

    if not (mime == "application/pdf" or filename.lower().endswith(".pdf")):
        return await message.reply_text("⚠️ This auto-rename currently supports **PDF files only**.")

    format_template = await codeflixbots.get_format_template(user_id)
    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename before sending PDFs.")

    # Compute sort key based on episode/chapter (e.g. 31, 31.5, 36.6)
    sort_key = get_episode_sort_key(filename)

    # Add job to user's queue
    jobs = user_jobs.setdefault(user_id, [])
    jobs.append((sort_key, message))

    # Start worker if not already running
    if user_id not in user_workers or user_workers[user_id].done():
        user_workers[user_id] = asyncio.create_task(user_worker(client, user_id))


# -------------------- SINGLE PDF PROCESSOR --------------------


async def process_single_pdf(client: Client, message: Message):
    """Download -> Rename -> Generate PDF thumb -> Upload (single file)."""
    user_id = message.from_user.id
    document = message.document

    file_unique_id = document.file_unique_id if document else None
    if not file_unique_id:
        return await message.reply_text("Unsupported file type.")

    # Prevent accidental double-processing of same Telegram file
    if file_unique_id in renaming_operations:
        if (datetime.now() - renaming_operations[file_unique_id]).seconds < 10:
            return
    renaming_operations[file_unique_id] = datetime.now()

    download_path = None
    thumb_path = None

    try:
        file_name = document.file_name or f"file_{file_unique_id}.pdf"

        # Extract season/episode/quality from original filename
        season, episode = extract_season_episode(file_name)
        quality = extract_quality(file_name)

        # For manga chapters, episode may be "31", "31.5", "36.6", etc.
        # We keep as-is (no zfill) to preserve your style.
        episode_val = episode or "XX"
        season_val = season or "01"
        quality_val = quality or "HD"

        # Build new filename from template
        format_template = await codeflixbots.get_format_template(user_id)
        replacements = {
            '{season}': season_val,
            '{episode}': episode_val,
            'Season': season_val,
            'Episode': episode_val,
            'QUALITY': quality_val,
        }
        for key, val in replacements.items():
            format_template = format_template.replace(key, val)

        ext = os.path.splitext(file_name)[1]
        if not ext:
            ext = ".pdf"
        new_filename = f"{format_template}{ext}"

        # Paths
        download_path = os.path.join("downloads", new_filename)
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Download PDF
        status_msg = await message.reply_text(f"📥 **Downloading PDF (Ch {episode_val})...**")
        file_path = await client.download_media(
            message,
            file_name=download_path,
            progress=progress_for_pyrogram,
            progress_args=("Downloading...", status_msg, time.time())
        )

        await status_msg.edit("🧾 **Preparing file...**")

        # Caption & thumbnail
        caption = await codeflixbots.get_caption(user_id) or f"**{new_filename}**"
        thumb_msg = await codeflixbots.get_thumbnail(user_id)

        if thumb_msg:
            # User's custom thumbnail
            raw_thumb_path = await client.download_media(thumb_msg)
            thumb_path = await process_pdf_thumbnail(raw_thumb_path)
        else:
            # Auto-generate PDF thumbnail
            raw_thumb_path = await generate_pdf_thumbnail(file_path)
            if raw_thumb_path:
                thumb_path = await process_pdf_thumbnail(raw_thumb_path)

        # Upload PDF back to user
        await status_msg.edit(f"📤 **Uploading Ch {episode_val}...**")

        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption=caption,
            thumb=thumb_path,
            progress=progress_for_pyrogram,
            progress_args=("Uploading...", status_msg, time.time())
        )

        await status_msg.delete()

    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.exception("Error while processing PDF:")
        await message.reply_text(f"Error: `{str(e)}`")
    finally:
        await cleanup_files(download_path, thumb_path)
        renaming_operations.pop(file_unique_id, None)