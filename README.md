# News pipeline with Airflow and LLMs

Twice a day, this pipeline collects the RSS feeds of a few news sites, keeps what
is worth reading, asks a LLM to write a short bulletin out of it, and pushes that
bulletin to a phone. Airflow runs the whole thing on a schedule and retries the
steps that fail.

## What it does, step by step

| Step | Task | What it produces |
|------|------|------------------|
| Collect | one `collect_source` per feed, in parallel | `data/<date>/<hour>h/raw/<source>.json` |
| Curate | `curate_articles` — merge, 12-hour window, deduplicate, select | `data/<date>/<hour>h/articles.json` |
| Write | `write_bulletin` — one LLM call | `data/<date>/<hour>h/bulletin.md` |
| Deliver | `deliver_push` and `deliver_mail`, each switched on its own | a notification, and `email.html` |

A broken feed fails its own task and the bulletin still goes out with the others.
Every delivery leaves a marker, so replaying a run never sends twice.

## Layout

```
app/
├── dags/
│   ├── bulletin_news.py     the DAG: orchestration only
│   └── news/                the logic, plain Python, testable without Airflow
│       ├── collect.py       read a feed, raise on failure
│       ├── curate.py        merge, filter, deduplicate, select
│       ├── compose.py       the LLM call, behind one function
│       ├── render.py        the notification text and the HTML email
│       ├── deliver.py       ntfy and SMTP, with draft mode and markers
│       └── storage.py       where files go, and atomic writes
├── prompts/bulletin.md      the prompt, versioned on its own
├── templates/bulletin.html  the email template
├── data/                    what each run produces — not in Git
└── airflow_home/            Airflow's own files — not in Git
```

`news` lives inside `dags/` on purpose: Airflow puts the dags folder on the
import path, so the DAG imports it with no path juggling.

## Install

Python 3.10, and a virtual environment outside the repository.

```bash
python3 -m venv ~/Envs/airflow
source ~/Envs/airflow/bin/activate

pip install -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.10.txt"
```

## Configure

```bash
cp .env.example .env     # then fill it in
```

`.env` holds every value that changes from one machine to the next, and every
secret. It is excluded from Git and recreated by hand on each machine. The LLM
provider is configuration too: any OpenAI-compatible endpoint works, so moving
from one to another is two variables.

To get a notification, install the [ntfy](https://ntfy.sh) app, subscribe to a
long random topic, and put that topic in `NTFY_TOPIC`. **The topic is both the
address and the password**: whoever knows it can read your bulletins and notify
your phone, so keep it out of Git.

Load the file in whatever shell is about to run something:

```bash
set -a; source .env; set +a
```

The variables must be in the environment of the process that runs the tasks:
exporting them after `airflow standalone` has started changes nothing for it.

## Run

```bash
export AIRFLOW_HOME=~/WorkProject/Airflow/app/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=~/WorkProject/Airflow/app/dags

airflow db migrate                  # once, and again after an Airflow upgrade
airflow dags test bulletin_news     # one full run, straight in the terminal
```

For the web interface and the scheduler:

```bash
airflow standalone                  # http://localhost:8080, user admin
airflow dags unpause bulletin_news  # a new DAG arrives paused
```

The admin password is printed at startup and kept in
`airflow_home/simple_auth_manager_passwords.json.generated`.

Nothing runs on a schedule unless `airflow standalone` is running at that moment.

## Schedule

`0 7,19 * * *` — **07:00 and 19:00 UTC**, which Airflow always counts in UTC:

| | Summer time (CEST) | Winter time (CET) |
|---|---|---|
| Morning run | 09:00 Paris | 08:00 Paris |
| Evening run | 21:00 Paris | 20:00 Paris |

The bulletins therefore shift by an hour at each daylight-saving change. The
12-hour window (`WINDOW_HOURS`) means the evening bulletin does not repeat the
morning one.

## Develop without sending anything

Each module runs on its own, which is the fast loop — Airflow only orchestrates:

```bash
cd dags
python -m news.collect      # fetch the first three feeds and print the titles
python -m news.curate       # the four curation steps on live feeds
python -m news.compose      # the bulletin, from the latest articles.json
```

Two switches keep a run harmless while you work on the wording:

```bash
LLM_BACKEND=fake      # no provider, no key: a stand-in bulletin marked as such
PUSH_MODE=draft       # writes push.txt instead of notifying
MAIL_MODE=draft       # writes email.html instead of sending
```

To send again on a run that already delivered, remove its marker:

```bash
rm data/2026-10-22/19h/sent-push.marker
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| `no such table: dag_bundle` | `airflow db migrate` has not run for this `AIRFLOW_HOME` |
| `airflow dags list` is empty | the database knows nothing yet: `airflow dags reserialize`, or start `standalone` |
| `dag_id could not be found` | `AIRFLOW__CORE__DAGS_FOLDER` does not point at `app/dags` |
| `ModuleNotFoundError: news` | running a module from the wrong folder: `cd dags` first |
| `missing NTFY_TOPIC` | `.env` not loaded in this shell |
| `already delivered … skipped` | the run's marker exists; remove it to resend |
| HTTP 429 from the LLM | the free tier's rate limit; the task retries with a growing delay |
| `finish_reason=length` | raise `LLM_MAX_TOKENS`, or lower `LLM_REASONING_EFFORT` |

## Author

Anaïs Dubois
