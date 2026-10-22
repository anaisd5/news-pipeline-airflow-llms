"""Collect the RSS sources, then curate them into the day's short list.

Milestone 2: one task per source, a merge that survives a broken one, and a
JSON file named by the logical date. The LLM, the rendering and the sending
come later.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# The `news` package sits one level above this file, and Airflow only puts the
# dags folder itself on the import path. Two cleaner options for later: move
# `news/` inside dags/, or install the project as a package (pip install -e).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from airflow.sdk import dag, get_current_context, task  # noqa: E402
from airflow.sdk.exceptions import AirflowSkipException  # noqa: E402
from airflow.task.trigger_rule import TriggerRule  # noqa: E402

from news.collect import SOURCES, get_feed  # noqa: E402
from news.compose import compose_bulletin  # noqa: E402
from news.deliver import is_enabled, send_email, send_push  # noqa: E402
from news.curate import DEFAULT_LIMIT, DEFAULT_PER_PUBLISHER, DEFAULT_WINDOW_HOURS, curate  # noqa: E402
from news.render import render_email, render_push  # noqa: E402
from news.storage import read_json, read_text, run_dir, slugify, write_json, write_text  # noqa: E402


@dag(
    dag_id="bulletin_news",
    start_date=datetime(2026, 10, 1),
    schedule=None,  # manual while the pipeline is incomplete; "0 6 * * *" later
    catchup=False,
    max_active_runs=1,
    tags=["bulletin"],
)
def bulletin_news():
    @task(
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(seconds=60),
        max_active_tis_per_dag=4,  # four feeds at a time, not twenty
    )
    def collect_source(name: str, url: str) -> str:
        """Fetches one feed and writes it raw. Returns the file path, not the content."""
        context = get_current_context()
        feed = get_feed(url, source=name)
        path = run_dir(context["ds"], "raw") / f"{slugify(name)}.json"
        print(f"{name}: {len(feed['entries'])} entries (HTTP {feed['status']})")
        return write_json(path, feed)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def curate_articles(paths: list[str]) -> str:
        """Merges, filters, deduplicates and selects. Tolerates missing sources."""
        context = get_current_context()

        feeds = [read_json(path) for path in paths if path]
        print(f"{len(feeds)} sources out of {len(SOURCES)} answered")

        articles = curate(
            feeds,
            reference=context["logical_date"],
            hours=DEFAULT_WINDOW_HOURS,
            limit=DEFAULT_LIMIT,
            per_publisher=DEFAULT_PER_PUBLISHER,
        )
        if not articles:
            # Nothing to write and nothing to send: stop cleanly rather than
            # producing an empty bulletin.
            raise AirflowSkipException("no article in the time window")

        for article in articles:
            print(f"   {article['published_iso']}  {article['title'][:70]}")

        path = run_dir(context["ds"]) / "articles.json"
        return write_json(path, articles)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,  # a rate limit lifts on its own: 2, 4, 8 min
        max_retry_delay=timedelta(minutes=20),
        execution_timeout=timedelta(minutes=10),  # a free tier can be slow
    )
    def write_bulletin(articles_path: str) -> str:
        """Asks the LLM for the bulletin and keeps its raw answer on disk."""
        context = get_current_context()

        articles = read_json(articles_path)
        bulletin = compose_bulletin(articles, reference=context["logical_date"])

        print(bulletin[:800])
        path = run_dir(context["ds"]) / "bulletin.md"
        return write_text(path, bulletin)

    # ignore_downstream_trigger_rules=False: without it a short circuit skips the
    # WHOLE downstream subtree, journalise included, whatever its trigger rule says.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def push_enabled() -> bool:
        """Stops this branch when SEND_PUSH is off — the switch of course 05."""
        return is_enabled("push")

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def mail_enabled() -> bool:
        """Stops this branch when SEND_MAIL is off."""
        return is_enabled("mail")

    @task(retries=2, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True)
    def deliver_push(bulletin_path: str) -> str:
        """Pushes the shortened bulletin, unless it already went out today."""
        context = get_current_context()
        bulletin = read_text(bulletin_path)
        result = send_push(
            render_push(bulletin),
            title=f"Bulletin du {context['logical_date'].strftime('%d/%m')}",
            ds=context["ds"],
        )
        print(result)
        return result

    @task(retries=2, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True)
    def deliver_mail(bulletin_path: str, articles_path: str) -> str:
        """Sends the HTML bulletin, unless it already went out today."""
        context = get_current_context()
        bulletin = read_text(bulletin_path)
        subject, page = render_email(
            bulletin,
            reference=context["logical_date"],
            article_count=len(read_json(articles_path)),
        )
        result = send_email(subject, page, bulletin, ds=context["ds"])
        print(result)
        return result

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def journalise() -> str:
        """Closes the run, whichever channels were switched on (course 05)."""
        context = get_current_context()
        return f"bulletin of {context['ds']} done"

    articles = curate_articles(collect_source.expand_kwargs(SOURCES))
    bulletin = write_bulletin(articles)

    push = deliver_push(bulletin)
    mail = deliver_mail(bulletin, articles)
    push_enabled() >> push
    mail_enabled() >> mail
    [push, mail] >> journalise()


bulletin_news()
