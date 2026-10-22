"""Turn the bulletin written by the LLM into what each channel needs.

The LLM returns Markdown. A phone notification wants short plain-ish text; an
email wants HTML. Both come from the same source, so the bulletin is written
once and never rewritten per channel.
"""

import html
import re
from datetime import datetime

from news.storage import project_root

TEMPLATE_PATH = project_root() / "templates" / "bulletin.html"

# ntfy sends anything over 4096 bytes as a file attachment instead of a message,
# which defeats the point of a notification. Staying under it keeps it readable.
PUSH_MAX_BYTES = 3800


def strip_comments(markdown: str) -> str:
    """
    Removes HTML comments, such as the marker left by the fake backend.

    Args:
        markdown (str): The bulletin.

    Returns:
        str: The bulletin without comments.
    """
    return re.sub(r"<!--.*?-->", "", markdown, flags=re.DOTALL).strip()


def render_push(bulletin: str, max_bytes: int = PUSH_MAX_BYTES) -> str:
    """
    Prepares the bulletin for the notification: the Markdown, as written.

    Nothing is reformatted and nothing is shortened — the headings and the line
    per news item are what the ntfy app and its web view display. The only cut
    is a safety net against ntfy's own 4000-byte ceiling, which a bulletin of a
    dozen articles does not reach.

    Args:
        bulletin (str): The bulletin in Markdown.
        max_bytes (int): Budget in bytes, not characters: accents count double.

    Returns:
        str: The text to push.
    """
    text = strip_comments(bulletin)
    if len(text.encode("utf-8")) <= max_bytes:
        return text

    suffix = "\n\n[…] bulletin tronqué, voir le fichier du jour."
    budget = max_bytes - len(suffix.encode("utf-8"))

    kept: list[str] = []
    used = 0
    for line in text.splitlines():
        cost = len(line.encode("utf-8")) + 1
        if used + cost > budget:
            break
        kept.append(line)
        used += cost

    return "\n".join(kept).rstrip() + suffix


def markdown_to_html(markdown: str) -> str:
    """
    Converts the small Markdown subset the prompt asks for into HTML.

    Headings, paragraphs, bold, italics and links — nothing else, because
    nothing else is expected. Everything is escaped before any tag is added, so
    a stray "<" in a headline cannot break the email.

    Args:
        markdown (str): The bulletin in Markdown.

    Returns:
        str: The body of the email, as HTML.
    """
    pieces: list[str] = []
    for block in re.split(r"\n\s*\n", strip_comments(markdown)):
        block = block.strip()
        if not block:
            continue

        heading = re.match(r"^(#{1,3})\s+(.*)$", block)
        if heading:
            level = len(heading.group(1))
            pieces.append(f"<h{level}>{inline_to_html(heading.group(2))}</h{level}>")
            continue

        lines = [inline_to_html(line.strip()) for line in block.splitlines() if line.strip()]
        pieces.append("<p>" + "<br>".join(lines) + "</p>")

    return "\n".join(pieces)


def inline_to_html(text: str) -> str:
    """
    Escapes one line and applies bold, italics and links.

    Args:
        text (str): One line of Markdown.

    Returns:
        str: The same line in HTML.
    """
    safe = html.escape(text, quote=False)
    safe = re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r'<a href="\2">\1</a>', safe)
    safe = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", safe)
    safe = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<em>\1</em>", safe)
    return safe


def render_email(bulletin: str, reference: datetime, article_count: int = 0) -> tuple[str, str]:
    """
    Builds the subject and the HTML body of the email.

    Args:
        bulletin (str): The bulletin in Markdown.
        reference (datetime): The run's logical date.
        article_count (int): Shown in the footer, when known.

    Returns:
        tuple[str, str]: The subject, then the HTML page.

    Raises:
        FileNotFoundError: If the template is missing.
    """
    day = reference.strftime("%d/%m/%Y")
    subject = f"Bulletin du {day}"

    footer = f"Généré le {reference.strftime('%d/%m/%Y à %H:%M UTC')}"
    if article_count:
        footer += f" — {article_count} articles retenus"

    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    page = (
        template.replace("{{title}}", html.escape(subject))
        .replace("{{body}}", markdown_to_html(bulletin))
        .replace("{{footer}}", html.escape(footer))
    )
    return subject, page
