"""Turn raw feed results into the short list of articles worth a bulletin.

The four steps of the pipeline, each usable on its own:

    merge_feeds   -> one flat list of articles out of several feed results
    filter_recent -> only what was published inside the time window
    deduplicate   -> one entry per article, sections merged
    select        -> the final short list, with a cap per publisher

`curate` runs the four in order. Nothing here touches the network: it works on
data already collected by `news.collect`, so it can be tested with hand-written
dictionaries and rerun without hitting any site.
"""

from datetime import datetime, timedelta
from urllib.parse import urlsplit

DEFAULT_WINDOW_HOURS = 24

# A dozen articles is what a LLM can summarize without being flooded, and what
# stays readable on a phone.
DEFAULT_LIMIT = 12

# France Info alone provides half of the feeds: without a cap per publisher the
# bulletin would be a France Info bulletin.
DEFAULT_PER_PUBLISHER = 4


def merge_feeds(feeds: list[dict]) -> list[dict]:
    """
    Flattens several feed results into a single list of articles.

    Feeds that answered 304 Not Modified simply carry no entry, which is not an
    error: there was nothing new to report.

    Args:
        feeds (list[dict]): Results of news.collect.get_feed.

    Returns:
        list[dict]: Every article of every feed, in the order the feeds were given.
    """
    articles = []
    for feed in feeds:
        if not feed:
            continue  # a task that failed may hand us nothing
        articles.extend(feed.get("entries") or [])
    return articles


def publisher_of(article: dict) -> str:
    """
    Returns the publisher of an article, derived from its link.

    "https://www.franceinfo.fr/politique/..." gives "franceinfo.fr". Deriving it
    from the host rather than from the feed name means a new feed of a known
    publisher is grouped with it automatically.

    Args:
        article (dict): An article as built by news.collect.

    Returns:
        str: The publisher's domain, or "" when the link is unusable.
    """
    link = article.get("canonical_link") or article.get("link") or ""
    host = urlsplit(link).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def filter_recent(
    articles: list[dict],
    reference: datetime,
    hours: int = DEFAULT_WINDOW_HOURS,
) -> list[dict]:
    """
    Keeps the articles published inside the window ending at `reference`.

    `reference` is a parameter on purpose: in a DAG it comes from the logical
    date, never from datetime.now(). That is what makes a rerun reproducible.

    Args:
        articles (list[dict]): The articles to filter.
        reference (datetime): End of the window. Must be timezone-aware.
        hours (int): Width of the window, in hours.

    Returns:
        list[dict]: The articles published between reference - hours and reference.
            Articles without a usable date are dropped: they cannot be placed.

    Raises:
        ValueError: If `reference` has no timezone.
    """
    if reference.tzinfo is None:
        raise ValueError(
            "reference must be timezone-aware: published_iso is in UTC, "
            "comparing it with a naive datetime would raise TypeError"
        )

    start = reference - timedelta(hours=hours)
    recent = []
    for article in articles:
        published = article.get("published_iso")
        if not published:
            continue
        moment = datetime.fromisoformat(published)
        if start <= moment <= reference:
            recent.append(article)
    return recent


def deduplicate(articles: list[dict]) -> list[dict]:
    """
    Keeps one entry per article, merging the sections it appeared in.

    The same France Info article shows up in titres.rss, politique.rss,
    economie.rss… so duplicates are detected on the canonical link. Rather than
    throwing the copies away, their feed names are collected in a "sources"
    field: knowing that an article is both economy and health is useful, both to
    select it and to write about it later.

    Args:
        articles (list[dict]): The articles to deduplicate.

    Returns:
        list[dict]: One copy of each article, in order of first appearance, with
            a "sources" field listing every feed it came from.
    """
    unique: dict[str, dict] = {}
    for article in articles:
        key = article.get("canonical_link") or article.get("id") or article.get("link")
        if not key:
            continue  # nothing to identify it by, nothing to link to either
        kept = unique.get(key)
        if kept is None:
            copy = dict(article)
            copy["sources"] = [article.get("source", "")]
            unique[key] = copy
        elif article.get("source") and article["source"] not in kept["sources"]:
            kept["sources"].append(article["source"])
    return list(unique.values())


def select(
    articles: list[dict],
    limit: int = DEFAULT_LIMIT,
    per_publisher: int = DEFAULT_PER_PUBLISHER,
) -> list[dict]:
    """
    Picks the final short list: most recent first, with a cap per publisher.

    The order is fully deterministic — date descending, then id — so the same
    input always gives the same bulletin, which is what makes a rerun safe.

    The cap is a target, not a wall: if it leaves the list short of `limit`, a
    second pass fills the remaining places with the next most recent articles.
    An under-filled bulletin is worse than a slightly unbalanced one.

    Args:
        articles (list[dict]): The deduplicated articles.
        limit (int): How many articles the bulletin holds.
        per_publisher (int): How many articles one publisher may take.

    Returns:
        list[dict]: At most `limit` articles, most recent first.
    """
    ordered = sorted(
        articles,
        key=lambda a: (a.get("published_iso", ""), a.get("id", "")),
        reverse=True,
    )

    chosen: list[dict] = []
    taken: dict[str, int] = {}
    for article in ordered:
        if len(chosen) >= limit:
            break
        publisher = publisher_of(article)
        if taken.get(publisher, 0) >= per_publisher:
            continue
        chosen.append(article)
        taken[publisher] = taken.get(publisher, 0) + 1

    if len(chosen) < limit:
        already = {id(a) for a in chosen}
        for article in ordered:
            if len(chosen) >= limit:
                break
            if id(article) not in already:
                chosen.append(article)

    return chosen


def curate(
    feeds: list[dict],
    reference: datetime,
    hours: int = DEFAULT_WINDOW_HOURS,
    limit: int = DEFAULT_LIMIT,
    per_publisher: int = DEFAULT_PER_PUBLISHER,
) -> list[dict]:
    """
    Runs the four steps in order: merge, filter, deduplicate, select.

    Args:
        feeds (list[dict]): Results of news.collect.get_feed.
        reference (datetime): End of the time window. Must be timezone-aware.
        hours (int): Width of the window, in hours.
        limit (int): How many articles the bulletin holds.
        per_publisher (int): How many articles one publisher may take.

    Returns:
        list[dict]: The selected articles, most recent first.
    """
    articles = merge_feeds(feeds)
    articles = filter_recent(articles, reference, hours=hours)
    articles = deduplicate(articles)
    return select(articles, limit=limit, per_publisher=per_publisher)


#----- Examples of usage -----

if __name__ == "__main__":
    from datetime import timezone

    from news.collect import SOURCES, FeedError, get_feed

    feeds, broken = [], []
    for source in SOURCES:
        try:
            feeds.append(get_feed(source["url"], source=source["name"]))
        except FeedError as error:
            broken.append((source["name"], str(error)))

    now = datetime.now(timezone.utc)
    merged = merge_feeds(feeds)
    recent = filter_recent(merged, now)
    unique = deduplicate(recent)
    final = select(unique)

    print(f"feeds: {len(feeds)} answered, {len(broken)} broken")
    print(f"merged: {len(merged)} -> recent: {len(recent)} -> unique: {len(unique)} -> selected: {len(final)}")
    for article in final:
        sections = ", ".join(article["sources"])
        print(f"   {article['published_iso']}  [{publisher_of(article)}]  {article['title'][:70]}")
        print(f"      sections: {sections}")
