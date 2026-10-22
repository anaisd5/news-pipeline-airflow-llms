"""Send the bulletin, or write what would have been sent.

Two channels, each switched on or off on its own:

    SEND_PUSH=true   a notification on the phone, through ntfy
    SEND_MAIL=true   an email, through any SMTP server

And one switch above them both:

    DELIVERY_MODE=draft   (default) write the message to data/<ds>/ instead of sending
    DELIVERY_MODE=live    actually send

`draft` is there to let you work on the wording without filling your inbox, and
to let the pipeline run end to end before you own a topic or an SMTP account.

Sending is the one step that cannot be undone, so every channel writes a marker
once it has delivered: replaying a run sees the marker and skips, instead of
sending the same bulletin twice.

    NTFY_TOPIC     the topic name — treat it as a password: whoever knows it can
                   both read your bulletins and notify your phone
    NTFY_SERVER    https://ntfy.sh by default
    SMTP_HOST      e.g. smtp.gmail.com
    SMTP_PORT      587 by default (STARTTLS)
    SMTP_USER      the account that authenticates
    SMTP_PASSWORD  an app password, never your account password
    MAIL_FROM      defaults to SMTP_USER
    MAIL_TO        the recipient
"""

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

import requests

from news.storage import run_dir, write_text

DEFAULT_NTFY_SERVER = "https://ntfy.sh"
DEFAULT_SMTP_PORT = 587
DEFAULT_TIMEOUT = 30


class DeliverError(RuntimeError):
    """
    Raised when a channel could not deliver.

    The task then fails and retries, which is what you want for a notification
    service that answers 503 for a minute.
    """


def is_enabled(channel: str) -> bool:
    """
    Says whether a channel is switched on.

    Args:
        channel (str): "push" or "mail".

    Returns:
        bool: True when SEND_PUSH / SEND_MAIL is a true-ish value.
    """
    value = os.environ.get(f"SEND_{channel.upper()}", "false").strip().lower()
    return value in ("true", "1", "yes", "on")


def is_draft() -> bool:
    """
    Says whether nothing should actually leave the machine.

    Returns:
        bool: True unless DELIVERY_MODE is exactly "live".
    """
    return os.environ.get("DELIVERY_MODE", "draft").strip().lower() != "live"


def marker_path(ds: str, channel: str) -> Path:
    """
    Returns the file that records a successful delivery for one day and channel.

    Args:
        ds (str): The logical date, as YYYY-MM-DD.
        channel (str): "push" or "mail".

    Returns:
        Path: The marker's path, whether it exists or not.
    """
    return run_dir(ds) / f"sent-{channel}.marker"


def already_sent(ds: str, channel: str) -> bool:
    """
    Says whether this channel already delivered for this day.

    Args:
        ds (str): The logical date.
        channel (str): "push" or "mail".

    Returns:
        bool: True when the marker is there.
    """
    return marker_path(ds, channel).exists()


def mark_sent(ds: str, channel: str, detail: str = "") -> str:
    """
    Records that this channel delivered, so a replay does not send again.

    Args:
        ds (str): The logical date.
        channel (str): "push" or "mail".
        detail (str): Free text kept in the marker, e.g. the recipient.

    Returns:
        str: The marker's path.
    """
    return write_text(marker_path(ds, channel), f"{channel} delivered {detail}".strip() + "\n")


def send_push(text: str, title: str, ds: str) -> str:
    """
    Pushes the bulletin to the phone through ntfy.

    Args:
        text (str): The message body, already shortened by news.render.
        title (str): The notification's title.
        ds (str): The logical date, for the draft file and the marker.

    Returns:
        str: What happened, for the task log.

    Raises:
        DeliverError: If the topic is missing or the server refuses.
    """
    if already_sent(ds, "push"):
        return f"push already delivered for {ds}, skipped"

    if is_draft():
        path = write_text(run_dir(ds) / "push.txt", f"{title}\n\n{text}\n")
        return f"draft written to {path}"

    topic = os.environ.get("NTFY_TOPIC", "")
    if not topic:
        raise DeliverError("missing NTFY_TOPIC. Pick a long random topic, subscribe to it in the ntfy app, and keep it out of Git.")

    server = os.environ.get("NTFY_SERVER", DEFAULT_NTFY_SERVER).rstrip("/")
    url = f"{server}/{topic}"
    try:
        response = requests.post(
            url,
            data=text.encode("utf-8"),
            headers={
                "Title": title,
                # The app renders Markdown, so the headings survive.
                "Markdown": "true",
                "Tags": "newspaper",
                # The Android app crops long text in its own view (ntfy issue #1515);
                # its web view does not. Tapping the notification opens the topic there,
                # where the bulletin is complete and formatted.
                "Click": url,
            },
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as error:
        raise DeliverError(f"{server} unreachable: {error!r}") from error

    if response.status_code >= 400:
        raise DeliverError(f"{server} returned HTTP {response.status_code}: {response.text[:200]}")

    mark_sent(ds, "push", f"to {server}")
    return f"push sent through {server} ({len(text.encode('utf-8'))} bytes)"


def send_email(subject: str, html_body: str, text_body: str, ds: str) -> str:
    """
    Sends the bulletin by email through SMTP, with STARTTLS.

    The message carries both a plain-text and an HTML part, so a client that
    refuses HTML still shows something readable.

    Args:
        subject (str): The subject line.
        html_body (str): The HTML page from news.render.
        text_body (str): The Markdown bulletin, as the plain-text part.
        ds (str): The logical date, for the draft file and the marker.

    Returns:
        str: What happened, for the task log.

    Raises:
        DeliverError: If the configuration is incomplete or the server refuses.
    """
    if already_sent(ds, "mail"):
        return f"mail already delivered for {ds}, skipped"

    if is_draft():
        path = write_text(run_dir(ds) / "email.html", html_body)
        return f"draft written to {path}"

    host = os.environ.get("SMTP_HOST", "")
    user = os.environ.get("SMTP_USER", "")
    password = os.environ.get("SMTP_PASSWORD", "")
    recipient = os.environ.get("MAIL_TO", "")
    missing = [
        name
        for name, value in (("SMTP_HOST", host), ("SMTP_USER", user), ("SMTP_PASSWORD", password), ("MAIL_TO", recipient))
        if not value
    ]
    if missing:
        raise DeliverError(f"missing configuration: {', '.join(missing)}")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = os.environ.get("MAIL_FROM", user)
    message["To"] = recipient
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")

    port = int(os.environ.get("SMTP_PORT", DEFAULT_SMTP_PORT))
    try:
        with smtplib.SMTP(host, port, timeout=DEFAULT_TIMEOUT) as server:
            server.starttls()
            server.login(user, password)
            server.send_message(message)
    except smtplib.SMTPAuthenticationError as error:
        raise DeliverError(
            f"{host} refused the credentials: {error}. Personal Outlook.com accounts now "
            "require OAuth and reject app passwords (error 535 5.7.139); use another provider."
        ) from error
    except (smtplib.SMTPException, OSError) as error:
        raise DeliverError(f"sending through {host}:{port} failed: {error!r}") from error

    mark_sent(ds, "mail", f"to {recipient}")
    return f"mail sent to {recipient} through {host}"
