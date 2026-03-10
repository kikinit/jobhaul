# Jobhaul

Personal job market intelligence tool. Collects job listings from multiple sources (Platsbanken, Jooble, RemoteOK, LinkedIn, Indeed), analyzes them against your profile using an LLM, and stores results in SQLite. Includes a CLI and a web dashboard.

## Installation

```bash
pip install -e ".[dev]"
```

For LinkedIn/Indeed scraping support:
```bash
pip install -e ".[scraping]"
playwright install chromium
```

## Configuration

Initialize a profile from the example:

```bash
jobhaul config init
# or manually:
mkdir -p ~/.config/jobhaul
cp config/profile.example.yaml ~/.config/jobhaul/profile.yaml
```

Edit `~/.config/jobhaul/profile.yaml` to set:

- **name, roles, skills, learning** -- your candidate profile
- **search_terms** -- keywords used to query job boards
- **seniority, education, languages, summary** -- context for LLM scoring
- **location, remote_only, will_relocate** -- geographic preferences
- **flags** -- boost/warn/exclude keyword rules
- **prompt_context** -- (optional) override the default LLM skill-depth context. If not set, a default context for junior/academic-level candidates is used.
- **sources** -- enable/disable each job board and set API keys
- **llm** -- adapter and model selection
- **analysis.pre_screen_threshold** -- minimum keyword overlap to send a listing to the LLM

## Usage

### Scan (collect + analyze)

```bash
# Collect and analyze from all enabled sources
jobhaul scan

# Collect from a single source
jobhaul scan --source platsbanken

# Collect only, skip analysis
jobhaul scan --skip-analysis

# Analyze only (skip collection)
jobhaul scan --analyze-only

# Retry previously failed analyses
jobhaul scan --retry-failed
```

### List and inspect

```bash
# List recent listings
jobhaul list --top 10

# Filter by minimum score
jobhaul list --min-score 70

# Show full detail for a listing
jobhaul show 42
```

### Analyze

```bash
# Analyze a single listing
jobhaul analyze 42

# Analyze all unanalyzed listings
jobhaul analyze 0 --all

# Analyze up to 10 unanalyzed listings
jobhaul analyze 0 --all --limit 10
```

### Other commands

```bash
# View current configuration
jobhaul config show

# Show database statistics
jobhaul stats

# Run database maintenance (dedup merge)
jobhaul db maintenance

# Start the web dashboard
jobhaul serve
jobhaul serve --port 9000 --host 0.0.0.0
```

## Architecture

```
src/jobhaul/
  cli.py              CLI entrypoint (Typer)
  service.py           Orchestrates collection and analysis workflows
  models.py            Pydantic data models (Profile, JobListing, AnalysisResult, Stats, etc.)
  config.py            YAML profile loading and validation
  constants.py         Shared named constants
  flagging.py          Keyword flag matching (boost/warn/exclude)
  log.py               Centralized logging setup

  collectors/          Job board integrations
    base.py            Abstract Collector, ApifyCollectorMixin, request_with_retry
    registry.py        Name-to-class registration
    platsbanken.py     Swedish Arbetsformedlingen API
    jooble.py          Jooble API
    remoteok.py        RemoteOK JSON feed
    linkedin.py        LinkedIn via Apify
    indeed.py          Indeed via Apify
    stealth.py         Browser stealth utilities

  analysis/            LLM analysis pipeline
    adapter.py         Abstract LLMAdapter interface
    claude_cli.py      Claude CLI subprocess adapter
    matcher.py         Prompt building, response parsing, pre-screening

  db/                  SQLite persistence
    schema.py          Schema definition and migrations (v1-v8)
    queries.py         All SQL operations (upsert, query, analysis, stats)

  web/                 FastAPI web dashboard
    app.py             Routes (HTML + JSON API)
    templates/         Jinja2 HTML templates
    static/            CSS/JS assets
```

**Data flow:** `profile.yaml` -> `collectors` -> `db` (upsert + dedup) -> `matcher` (pre-screen + LLM) -> `db` (save analysis) -> `cli` / `web` (display)

## Authentication

Jobhaul uses the `claude` CLI for LLM analysis. Three authentication paths:

1. **OpenClaw auto-refresh** -- tokens read from `~/.openclaw/agents/main/agent/auth.json` and injected into Claude CLI credentials automatically.
2. **Claude CLI login** -- run `claude login` to authenticate via browser OAuth.
3. **API key** -- set `ANTHROPIC_API_KEY=sk-ant-...` environment variable.

## Development

```bash
pip install -e ".[dev]"
pytest --cov=jobhaul
```
