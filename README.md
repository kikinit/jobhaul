# Jobhaul

Personal job market intelligence CLI that collects job listings from multiple sources, analyzes them with LLM, and stores results in SQLite.

## Installation

```bash
pip install -e ".[dev]"
```

For LinkedIn/Indeed scraping support:
```bash
pip install -e ".[scraping]"
playwright install chromium
```

## Usage

```bash
# Collect and analyze from enabled sources
jobhaul scan

# Collect from a single source
jobhaul scan --source platsbanken

# Collect only, skip analysis
jobhaul scan --skip-analysis

# Analyze only (skip collection)
jobhaul scan --analyze-only

# Analyze all unanalyzed listings
jobhaul analyze 0 --all

# Analyze up to 10 unanalyzed listings
jobhaul analyze 0 --all --limit 10

# List recent listings
jobhaul list --top 10

# List by score
jobhaul list --min-score 70

# Show full detail for a listing
jobhaul show 42

# View configuration
jobhaul config show

# Start the web UI
jobhaul serve
```

## Configuration

Copy the example profile and customize:

```bash
mkdir -p ~/.config/jobhaul
cp config/profile.example.yaml ~/.config/jobhaul/profile.yaml
```

## Authentication

Jobhaul uses the `claude` CLI for LLM analysis. There are three ways to authenticate:

### Path 1: OpenClaw Auto-Refresh (Cortex Deployments)

If you're running Jobhaul through OpenClaw/Cortex, OAuth tokens are automatically refreshed from the OpenClaw agent credentials:

- OpenClaw stores credentials at `~/.openclaw/agents/main/agent/auth.json`
- Jobhaul reads the `anthropic.key` field from this file
- The token is injected into `~/.claude/.credentials.json` before each analysis run
- No manual token management is needed

### Path 2: Claude CLI Login (Standalone)

For standalone usage, authenticate directly with the Claude CLI:

```bash
claude login
```

This opens a browser for OAuth authentication and stores credentials at `~/.claude/.credentials.json`.

### Path 3: API Key (Direct API Access)

Set the `ANTHROPIC_API_KEY` environment variable to use the Anthropic API directly:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

This works with the `claude` CLI when configured to use the API key adapter.

## Development

```bash
pip install -e ".[dev]"
pytest --cov=jobhaul
```
