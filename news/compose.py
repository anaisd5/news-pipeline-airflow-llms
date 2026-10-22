"""Ask a LLM to turn the selected articles into the day's bulletin.

The provider is configuration, not code. Groq, Mistral, Ollama and many others
expose the same OpenAI-compatible route (`POST <base>/chat/completions`), so one
implementation covers them all: you give the base URL, the key and the model
name, and nothing here changes when you switch.

    LLM_BACKEND      "openai" to call a real provider, "fake" to run offline
    LLM_BASE_URL     e.g. https://api.groq.com/openai/v1
    LLM_API_KEY      the key from your provider's console — never commit it
    LLM_MODEL        the exact model name, copied from that same console
    LLM_TEMPERATURE  0.2 by default: a summary should be faithful, not creative
    LLM_MAX_TOKENS   3000 by default. A reasoning model spends part of this budget
                     thinking before it writes, so a value sized for the bulletin alone
                     gets the answer cut in the middle.
    LLM_TIMEOUT      120 seconds by default

Two knobs only sent when set, for providers that expose them (Groq's gpt-oss):

    LLM_REASONING_EFFORT     low | medium | high — lower it to leave room for the answer
    LLM_INCLUDE_REASONING    false to drop the reasoning from the answer altogether

The default backend is "fake": the pipeline runs end to end without a key, and
says loudly in its output that no model was involved.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import requests

PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "bulletin.md"

DEFAULT_BACKEND = "fake"
DEFAULT_TEMPERATURE = 0.2
DEFAULT_MAX_TOKENS = 3000
DEFAULT_TIMEOUT = 120

# Feed summaries are sometimes very long; the model does not need all of it.
MAX_DESCRIPTION = 400

FAKE_WARNING = "<!-- bulletin généré sans modèle : LLM_BACKEND=fake -->"


class ComposeError(RuntimeError):
    """
    Raised when the bulletin could not be written.

    Covers a missing configuration, an unreachable or refusing provider, and an
    answer that does not contain a bulletin. Raising keeps the failure visible:
    the task turns red and retries, instead of sending an empty bulletin.
    """


def format_articles(articles: list[dict], max_description: int = MAX_DESCRIPTION) -> str:
    """
    Lays the articles out as the numbered block the prompt expects.

    Args:
        articles (list[dict]): The selected articles.
        max_description (int): Where to cut a long summary.

    Returns:
        str: One numbered entry per article.
    """
    blocks = []
    for number, article in enumerate(articles, start=1):
        description = (article.get("description") or "").strip()
        if len(description) > max_description:
            description = description[:max_description].rstrip() + "…"
        sections = article.get("sources") or [article.get("source", "")]
        blocks.append(
            f"{number}. {article.get('title', '').strip()}\n"
            f"   rubriques : {', '.join(s for s in sections if s)}\n"
            f"   publié : {article.get('published_iso', '')}\n"
            f"   résumé : {description or '(aucun résumé fourni par le média)'}"
        )
    return "\n\n".join(blocks)


def build_prompt(articles: list[dict], reference: datetime) -> str:
    """
    Fills the prompt template with the day's articles.

    The template lives in prompts/bulletin.md, outside the code, because it is
    the part you will rewrite twenty times.

    Args:
        articles (list[dict]): The selected articles.
        reference (datetime): The run's logical date, used in the title.

    Returns:
        str: The prompt to send.

    Raises:
        ComposeError: If the template is missing.
    """
    try:
        template = PROMPT_PATH.read_text(encoding="utf-8")
    except OSError as error:
        raise ComposeError(f"prompt not readable at {PROMPT_PATH}: {error}") from error

    return (
        template.replace("{{date}}", reference.strftime("%d/%m/%Y"))
        .replace("{{count}}", str(len(articles)))
        .replace("{{articles}}", format_articles(articles))
    )


def explain_http_error(response: requests.Response, url: str, base_url: str, model: str) -> str:
    """
    Turns a provider's refusal into a message that says what to do about it.

    Args:
        response (requests.Response): The refused response.
        url (str): The URL called.
        base_url (str): The provider's base URL, used to point at its model list.
        model (str): The model asked for.

    Returns:
        str: The message for the ComposeError.
    """
    status = response.status_code
    body = response.text[:300]
    retry_after = response.headers.get("Retry-After")

    if status == 429:
        wait = f" The provider asks to wait {retry_after} seconds." if retry_after else ""
        hint = (
            "A free tier limits requests per second, tokens per minute and tokens per month: "
            "check which one in your provider's console. Space the calls out, shorten the "
            "selection, and let the task retry — the limit lifts on its own."
        )
        return f"{url} returned HTTP 429 (rate limited).{wait} {hint} Answer: {body}"
    if status in (401, 403):
        return (
            f"{url} refused the key (HTTP {status}). Check LLM_API_KEY, and that the key is "
            f"still active in the provider's console. Answer: {body}"
        )
    if status == 404:
        return (
            f"{url} returned HTTP 404. Either LLM_BASE_URL is wrong, or the model {model!r} "
            f"does not exist for this key — list them with GET {base_url.rstrip('/')}/models. "
            f"Answer: {body}"
        )
    if status >= 500:
        return f"{url} is failing on its side (HTTP {status}); retrying later should work. Answer: {body}"
    return f"{url} returned HTTP {status}: {body}"


def call_openai_compatible(prompt: str, articles: list[dict]) -> str:
    """
    Sends the prompt to any provider exposing the OpenAI chat route.

    Args:
        prompt (str): The prompt to send.
        articles (list[dict]): Unused here; kept so every backend has one signature.

    Returns:
        str: The bulletin, as written by the model.

    Raises:
        ComposeError: If the configuration is incomplete, the provider refuses,
            or the answer holds no text.
    """
    base_url = os.environ.get("LLM_BASE_URL", "")
    api_key = os.environ.get("LLM_API_KEY", "")
    model = os.environ.get("LLM_MODEL", "")

    missing = [
        name
        for name, value in (("LLM_BASE_URL", base_url), ("LLM_API_KEY", api_key), ("LLM_MODEL", model))
        if not value
    ]
    if missing:
        raise ComposeError(
            f"missing configuration: {', '.join(missing)}. "
            "Set LLM_BASE_URL to your provider's OpenAI-compatible endpoint "
            "(Groq: https://api.groq.com/openai/v1, Mistral: https://api.mistral.ai/v1), "
            "LLM_MODEL to a model name copied from its console, and LLM_API_KEY to your key. "
            "Or set LLM_BACKEND=fake to run without a provider."
        )

    url = f"{base_url.rstrip('/')}/chat/completions"
    payload = {
        "model": model,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", DEFAULT_TEMPERATURE)),
        "max_tokens": int(os.environ.get("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS)),
        "messages": [{"role": "user", "content": prompt}],
    }

    # Only sent when asked for: an unknown parameter is a 400 on some providers.
    effort = os.environ.get("LLM_REASONING_EFFORT")
    if effort:
        payload["reasoning_effort"] = effort
    if os.environ.get("LLM_INCLUDE_REASONING", "").lower() in ("false", "0", "no"):
        payload["include_reasoning"] = False

    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {api_key}"},  # never logged
            timeout=float(os.environ.get("LLM_TIMEOUT", DEFAULT_TIMEOUT)),
        )
    except requests.RequestException as error:
        raise ComposeError(f"{url} unreachable: {error!r}") from error

    if response.status_code != 200:
        raise ComposeError(explain_http_error(response, url, base_url, model))

    try:
        answer = response.json()
        choice = answer["choices"][0]
        text = choice["message"]["content"].strip()
    except (ValueError, KeyError, IndexError, TypeError) as error:
        raise ComposeError(f"unexpected answer from {url}: {response.text[:300]}") from error

    usage = answer.get("usage") or {}
    print(
        f"model={model} prompt_tokens={usage.get('prompt_tokens', '?')} "
        f"completion_tokens={usage.get('completion_tokens', '?')} "
        f"finish_reason={choice.get('finish_reason', '?')}"
    )

    if not text:
        raise ComposeError(
            f"{model} returned an empty bulletin. A reasoning model may have spent the whole "
            "token budget thinking: raise LLM_MAX_TOKENS, or lower LLM_REASONING_EFFORT."
        )

    if choice.get("finish_reason") == "length":
        # Sending half a bulletin is worse than failing: this one is a configuration bug.
        raise ComposeError(
            f"{model} hit the token ceiling and the bulletin is cut off "
            f"(completion_tokens={usage.get('completion_tokens', '?')}, "
            f"LLM_MAX_TOKENS={payload['max_tokens']}). Raise LLM_MAX_TOKENS, or set "
            "LLM_REASONING_EFFORT=low / LLM_INCLUDE_REASONING=false to leave the budget "
            "to the answer rather than to the thinking."
        )
    return text


def call_fake(prompt: str, articles: list[dict]) -> str:
    """
    Builds a bulletin without any model, to exercise the pipeline offline.

    It lists the selected articles, so the result is useful for testing the
    rendering and the sending, and says in the first line that no model wrote it.

    Args:
        prompt (str): The prompt that would have been sent; unused.
        articles (list[dict]): The selected articles.

    Returns:
        str: A deterministic stand-in bulletin.
    """
    body = "\n".join(
        f"{article.get('title', '').strip()} ({article.get('source', 'source inconnue')})."
        for article in articles
    )
    return f"{FAKE_WARNING}\n\n# Bulletin de test\n\n## Articles retenus\n\n{body}"


BACKENDS = {"openai": call_openai_compatible, "fake": call_fake}


def compose_bulletin(articles: list[dict], reference: datetime) -> str:
    """
    Writes the day's bulletin from the selected articles.

    Args:
        articles (list[dict]): The selected articles.
        reference (datetime): The run's logical date.

    Returns:
        str: The bulletin, in Markdown.

    Raises:
        ComposeError: If there is nothing to write about, or the backend failed.
    """
    if not articles:
        raise ComposeError("no article to write about")

    backend = os.environ.get("LLM_BACKEND", DEFAULT_BACKEND).lower()
    call = BACKENDS.get(backend)
    if call is None:
        raise ComposeError(f"unknown LLM_BACKEND {backend!r}, expected one of {sorted(BACKENDS)}")

    prompt = build_prompt(articles, reference)
    print(f"backend={backend} articles={len(articles)} prompt={len(prompt)} characters")
    return call(prompt, articles)


#----- Examples of usage -----

if __name__ == "__main__":
    import glob
    from datetime import timezone

    paths = sorted(glob.glob(str(Path(__file__).resolve().parents[1] / "data" / "*" / "articles.json")))
    if not paths:
        raise SystemExit("no data/<date>/articles.json yet: run the DAG first")

    with open(paths[-1], encoding="utf-8") as file:
        selection = json.load(file)

    print(compose_bulletin(selection, datetime.now(timezone.utc)))
