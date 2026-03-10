"""Shared constants used across the jobhaul codebase.

Centralizes magic numbers and configuration defaults so they are easy to find,
understand, and change in one place.
"""

# Maximum characters of a job description to send to the LLM for analysis
MAX_DESCRIPTION_CHARS = 5000

# Maximum display widths for table columns in the CLI
MAX_TITLE_DISPLAY_CHARS = 50
MAX_COMPANY_DISPLAY_CHARS = 25

# Maximum number of analysis retries before a listing is permanently marked failed
MAX_ANALYSIS_FAIL_RETRIES = 5

# Default page size for the web UI listings view
DEFAULT_PAGE_SIZE = 50

# Apify actor run defaults
APIFY_MAX_ITEMS = 50
APIFY_POLL_INTERVAL_SECS = 10
APIFY_TIMEOUT_SECS = 300
