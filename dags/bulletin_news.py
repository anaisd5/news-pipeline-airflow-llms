"""Collect the RSS sources, then curate them into the day's short list.

Milestone 2: one task per source, a merge that survives a broken one, and a
JSON file named by the logical date. The LLM, the rendering and the sending
come later.
"""

import os
from datetime import datetime, timedelta

# `news` is importable because it sits next to this file, inside the dags folder,
# which Airflow puts on the import path.
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.task.trigger_rule import TriggerRule

from news.collect import SOURCES, get_feed
from news.compose import compose_bulletin
from news.deliver import is_enabled, send_email, send_push
from news.curate import DEFAULT_LIMIT, DEFAULT_PER_PUBLISHER, curate
from news.render import render_email, render_push
from news.storage import read_json, read_text, run_dir, run_slug, slugify, write_json, write_text


def alert_failure(context) -> None:
    """
    Pushes a notification when a task has definitively failed.

    Called after the last retry, through default_args, so it covers every task
    of the DAG. It must never raise: a callback that fails is silent (course 08),
    and an alert that disappears is worse than no alert.

    Args:
        context: Airflow's task context.
    """
    instance = context.get("task_instance")
    try:
        send_push(
            f"{instance.task_id} a échoué.\n\nRun {context.get('run_id')}\n"
            f"Regarde les logs de ce DAG.",
            title="Bulletin en echec",
            slug=run_slug(context["logical_date"]),
            tags="warning",  # ntfy turns the tag into an emoji, which a header cannot carry
        )
    except Exception as error:  # noqa: BLE001 — an alert must not break the run
        print(f"alert could not be pushed: {error!r}")


@dag(
    dag_id="bulletin_news",
    start_date=datetime(2026, 10, 1),
    schedule="0 7,19 * * *",  # 07:00 and 19:00 UTC — the courses' reminder: not Paris time
    catchup=False,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=5,  # stop nagging after five failed runs in a row
    default_args={"on_failure_callback": alert_failure},
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
        path = run_dir(run_slug(context["logical_date"]), "raw") / f"{slugify(name)}.json"
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
            hours=int(os.environ.get("WINDOW_HOURS", 12)),  # two runs a day: 12 h avoids repeats
            limit=DEFAULT_LIMIT,
            per_publisher=DEFAULT_PER_PUBLISHER,
        )
        if not articles:
            # Nothing to write and nothing to send: stop cleanly rather than
            # producing an empty bulletin.
            raise AirflowSkipException("no article in the time window")

        for article in articles:
            print(f"   {article['published_iso']}  {article['title'][:70]}")

        path = run_dir(run_slug(context["logical_date"])) / "articles.json"
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
        path = run_dir(run_slug(context["logical_date"])) / "bulletin.md"
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
            slug=run_slug(context["logical_date"]),
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
        result = send_email(subject, page, bulletin, slug=run_slug(context["logical_date"]))
        print(result)
        return result

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def journalise() -> str:
        """Closes the run, whichever channels were switched on (course 05)."""
        context = get_current_context()
        return f"bulletin of {run_slug(context['logical_date'])} done"

    articles = curate_articles(collect_source.expand_kwargs(SOURCES))
    bulletin = write_bulletin(articles)

    push = deliver_push(bulletin)
    mail = deliver_mail(bulletin, articles)
    push_enabled() >> push
    mail_enabled() >> mail
    [push, mail] >> journalise()


bulletin_news()
