import socket
import unicodedata
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser

# Identify politely
USER_AGENT = "BulletinNews/0.1 (personal learning project)"

# feedparser.parse() has no timeout argument: set it on the socket layer
REQUEST_TIMEOUT = 15  # seconds

# Query parameters that only exist for tracking: dropped when building an entry id
TRACKING_PARAMS = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid", "at_medium", "at_campaign")

SOURCES = [
    # France Info
    {"name": "France Info", "url": "https://www.franceinfo.fr/titres.rss"},
    {"name": "France Info Monde", "url": "https://www.franceinfo.fr/monde.rss"},
    {"name": "France Info Politique", "url": "https://www.franceinfo.fr/politique.rss"},
    {"name": "France Info Economie", "url": "https://www.franceinfo.fr/economie.rss"},
    {"name": "France Info Sciences", "url": "https://www.franceinfo.fr/sciences.rss"},
    {"name": "France Info Sante", "url": "https://www.franceinfo.fr/sante.rss"},
    {"name": "France Info Culture", "url": "https://www.franceinfo.fr/culture.rss"},
    {"name": "France Info Sports", "url": "https://www.franceinfo.fr/sports.rss"},
    {"name": "France Info Societe", "url": "https://www.franceinfo.fr/societe.rss"},
    {"name": "France Info Faits divers", "url": "https://www.franceinfo.fr/faits-divers.rss"},
    # 20 minutes
    {"name": "20 minutes Monde", "url": "https://www.20minutes.fr/feeds/rss-monde.xml"},
    {"name": "20 minutes France", "url": "https://www.20minutes.fr/feeds/rss-france.xml"},
    {"name": "20 minutes Economie", "url": "https://www.20minutes.fr/feeds/rss-economie.xml"},
    {"name": "20 minutes Sport", "url": "https://www.20minutes.fr/feeds/rss-sport.xml"},
    {"name": "20 minutes High-tech", "url": "https://www.20minutes.fr/feeds/rss-high-tech.xml"},
    {"name": "20 minutes Planete", "url": "https://www.20minutes.fr/feeds/rss-planete.xml"},
    {"name": "20 minutes Culture", "url": "https://www.20minutes.fr/feeds/rss-culture.xml"},
    # Tech
    {"name": "TechCrunch", "url": "https://techcrunch.com/tag/RSS/feed/"},
    {"name": "The Verge", "url": "https://www.theverge.com/rss/index.xml"},
    {"name": "L'Usine Digitale", "url": "https://www.usine-digitale.fr/arc/outboundfeeds/rss/"}
]


class FeedError(RuntimeError):
    """
    Raised when a feed cannot be fetched or parsed.

    Raising instead of returning an empty result keeps a broken source visible:
    the Airflow task fails, retries, and alerts, rather than silently producing
    a bulletin with one source missing.
    """


def normalize_text(text: str) -> str:
    """
    Normalizes the given text for display, keeping accents.

    NFC is the composed form: "é" stays a single character, which is what
    editors, terminals and LLMs expect.

    Args:
        text (str): The text to be normalized.

    Returns:
        str: The normalized text, accents included.
    """
    return unicodedata.normalize("NFC", text)


def comparison_key(text: str) -> str:
    """
    Builds a comparison key from the given text: lowercase, no accents, no punctuation.

    This is never displayed. It is meant to tell that "News : Anaïs's project"
    and "News, Anais's project" are the same story (deduplication).

    Args:
        text (str): The text to derive a key from.

    Returns:
        str: The comparison key.
    """
    decomposed = unicodedata.normalize("NFKD", text)
    without_accents = "".join(c for c in decomposed if not unicodedata.combining(c))
    letters_and_digits = "".join(c if c.isalnum() else " " for c in without_accents)
    return " ".join(letters_and_digits.lower().split())


def canonical_link(link: str) -> str:
    """
    Strips tracking parameters and the fragment from a link.

    Used as the entry id, so the same article shared with different tracking
    parameters is recognized as one article.

    Args:
        link (str): The raw link from the feed.

    Returns:
        str: The canonical link.
    """
    if not link:
        return ""
    parts = urlsplit(link)
    kept = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith(TRACKING_PARAMS)
    ]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), ""))


def to_iso_utc(published_parsed) -> str:
    """
    Converts feedparser's published_parsed into an ISO 8601 string in UTC.

    published_parsed is already expressed in UTC by feedparser, whatever the
    timezone written in the feed. Comparing these strings is safe; comparing the
    raw "published" strings of different feeds is not.

    Args:
        published_parsed: A time.struct_time from feedparser, or None.

    Returns:
        str: The date in ISO 8601 UTC, or "" when the feed gave no usable date.
    """
    if not published_parsed:
        return ""
    return datetime(*published_parsed[:6], tzinfo=timezone.utc).isoformat()


def get_rss_feed(url: str, etag: str = None, modified: str = None) -> feedparser.FeedParserDict:
    """
    Fetches and parses the RSS feed from the given URL.

    When etag or modified is given, the request is conditional: if the feed has
    not changed, the server answers 304 and sends no content at all.

    Args:
        url (str): The URL of the RSS feed.
        etag (str): The etag returned by the previous call, if any.
        modified (str): The Last-Modified value returned by the previous call, if any.

    Returns:
        feedparser.FeedParserDict: The parsed RSS feed.

    Raises:
        FeedError: If the feed could not be fetched or parsed.
    """
    # feedparser never raises on network errors: it reports them in d.bozo and d.status
    socket.setdefaulttimeout(REQUEST_TIMEOUT)
    d = feedparser.parse(url, agent=USER_AGENT, etag=etag, modified=modified)

    status = d.get("status")
    if status == 304:  # not modified since last run: no entries, and that is fine
        return d
    if status is not None and status >= 400:
        raise FeedError(f"{url} returned HTTP {status}")
    if d.bozo and not d.entries:
        raise FeedError(f"{url} could not be read: {d.get('bozo_exception')!r}")
    return d


def format_rss_feed(d: feedparser.FeedParserDict, source: str = None) -> dict:
    """
    Formats the parsed RSS feed into a structured dictionary.

    Args:
        d (feedparser.FeedParserDict): The parsed RSS feed.
        source (str): Name of the source, stored on every entry. Defaults to the feed title.

    Returns:
        dict: A dictionary containing the feed title, the HTTP metadata needed for
            the next conditional request, and a list of entries with their details.
    """
    feed = d.feed if 'feed' in d else {}
    title = normalize_text(feed.title if 'title' in feed else "")
    source_name = source or title
    entries = []

    result = {
        "title": title,
        "source": source_name,
        "status": d.get("status"),
        "not_modified": d.get("status") == 304,
        "etag": d.get("etag"),
        "modified": d.get("modified"),
        "href": d.get("href"),  # differs from the requested url after a redirect
        "entries": entries,
    }

    has_entries = hasattr(d, 'entries') and d.entries is not None
    if not has_entries:
        return result

    for entry in d.entries:
        entry_title = entry.get('title', "")
        entry_description = entry.get('description', "")
        entry_link = entry.get('link', "")
        if not entry_title or not entry_link:
            continue  # Skip entries we could neither read nor link to
        entries.append({
            "id": entry.get('id') or canonical_link(entry_link),
            "source": source_name,
            "title": normalize_text(entry_title),
            "description": normalize_text(entry_description),
            "link": entry_link,
            "canonical_link": canonical_link(entry_link),
            "published_iso": to_iso_utc(entry.get('published_parsed')),
            "title_key": comparison_key(entry_title),
        })

    return result


def get_feed(url: str, source: str = None, etag: str = None, modified: str = None) -> dict:
    """
    Fetches, parses, and formats the RSS feed from the given URL.

    Args:
        url (str): The URL of the RSS feed.
        source (str): Name of the source, stored on every entry.
        etag (str): The etag returned by the previous call, if any.
        modified (str): The Last-Modified value returned by the previous call, if any.

    Returns:
        dict: A dictionary containing the feed title and a list of entries with their details.

    Raises:
        FeedError: If the feed could not be fetched or parsed.
    """
    d = get_rss_feed(url, etag=etag, modified=modified)
    return format_rss_feed(d, source=source)


#----- Examples of usage -----

if __name__ == "__main__":
    for source in SOURCES[:3]:
        feed = get_feed(source["url"], source=source["name"])
        print(f"{feed['source']}: {len(feed['entries'])} entries (HTTP {feed['status']})")
        for entry in feed["entries"][:2]:
            print(f"   {entry['published_iso']}  {entry['title']}")
