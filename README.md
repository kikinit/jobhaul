# Jobhaul

Personal job market intelligence CLI that collects job listings from multiple sources, analyzes them with LLM, and stores results in SQLite.

## Installation

```bash
pip install -e ".[dev]"
```

## Usage

```bash
# Collect and analyze from enabled sources
jobhaul scan

# Collect from a single source
jobhaul scan --source platsbanken

# List recent listings
jobhaul list --top 10

# Show full detail for a listing
jobhaul show 42

# View configuration
jobhaul config show
```

## Configuration

Copy the example profile and customize:

```bash
mkdir -p ~/.config/jobhaul
cp config/profile.example.yaml ~/.config/jobhaul/profile.yaml
```

## Development

```bash
pip install -e ".[dev]"
pytest --cov=jobhaul
```
