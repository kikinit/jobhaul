"""FastAPI web application serving both HTML pages and a JSON API.

Provides a dashboard, paginated listing browser, detail views, scan triggers,
and a REST API under ``/api/``. All database access goes through FastAPI
dependency injection so connections are properly opened and closed per request.
"""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Generator
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from jobhaul.constants import DEFAULT_PAGE_SIZE
from jobhaul.db.queries import get_analysis, get_listing, get_stats, list_listings, list_listings_with_analysis, save_analysis

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

app = FastAPI(title="Jobhaul", description="Personal job market intelligence")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def get_db_dep() -> Generator[sqlite3.Connection, None, None]:
    """FastAPI dependency that provides a database connection for a request.

    Opens a new SQLite connection (initialising the schema if needed), yields
    it for the duration of the request, and closes it automatically when the
    request finishes.

    Yields:
        An open ``sqlite3.Connection`` instance.
    """
    from jobhaul.config import ensure_data_dir
    from jobhaul.db.schema import init_db

    conn = init_db(str(ensure_data_dir() / "jobhaul.db"))
    try:
        yield conn
    finally:
        conn.close()


def get_profile_dep():
    """FastAPI dependency that loads and returns the current user profile.

    Reads the profile YAML file on every request so that changes to the
    profile are picked up without restarting the server.

    Returns:
        A ``Profile`` instance parsed from the profile configuration file.
    """
    from jobhaul.config import load_profile

    return load_profile()


# --- HTML Routes ---


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, conn: sqlite3.Connection = Depends(get_db_dep)):
    """Render the main dashboard page with summary stats and top matches."""
    stats = get_stats(conn)
    all_analyzed = list_listings_with_analysis(
        conn, days=30, min_score=1, sort_by_score=True, limit=10,
    )
    top_matches = [
        {"listing": listing, "analysis": analysis}
        for listing, analysis in all_analyzed if analysis
    ]

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "top_matches": top_matches,
    })


def _parse_optional_int(value: str | None) -> int | None:
    """Parse optional int query param, treating empty string as None."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


@app.get("/listings", response_class=HTMLResponse)
async def listings_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db_dep),
    source: str | None = Query(None),
    min_score: str | None = Query(None),
    remote_only: bool = Query(False),
    days: str | None = Query("30"),
    sort: str = Query("date"),
    page: int = Query(1, ge=1),
    include_expired: bool = Query(False),
):
    """Render the paginated listings page with filtering and sorting options."""
    parsed_min_score = _parse_optional_int(min_score)
    parsed_days = _parse_optional_int(days) or 30

    per_page = DEFAULT_PAGE_SIZE
    all_pairs = list_listings_with_analysis(
        conn, days=parsed_days, source=source, min_score=parsed_min_score,
        sort_by_score=(sort == "score"),
        include_expired=include_expired,
    )

    listings_with_analysis = []
    for listing, analysis in all_pairs:
        if remote_only and not listing.is_remote:
            continue
        listings_with_analysis.append({"listing": listing, "analysis": analysis})

    if sort == "score":
        listings_with_analysis.sort(
            key=lambda x: (x["analysis"].match_score if x["analysis"] else -1),
            reverse=True,
        )
    elif sort == "company":
        listings_with_analysis.sort(
            key=lambda x: (x["listing"].company or "").lower()
        )

    total = len(listings_with_analysis)
    total_pages = max(1, math.ceil(total / per_page))
    page = min(page, total_pages)
    start = (page - 1) * per_page
    page_listings = listings_with_analysis[start:start + per_page]

    return templates.TemplateResponse("listings.html", {
        "request": request,
        "listings": page_listings,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "source": source or "",
        "min_score": parsed_min_score,
        "remote_only": remote_only,
        "days": parsed_days,
        "sort": sort,
        "include_expired": include_expired,
    })


@app.get("/listings/{listing_id}", response_class=HTMLResponse)
async def listing_detail(request: Request, listing_id: int, conn: sqlite3.Connection = Depends(get_db_dep)):
    """Render the detail page for a single listing with its analysis."""
    listing = get_listing(conn, listing_id)
    if not listing:
        return HTMLResponse("<h1>Listing not found</h1>", status_code=404)
    analysis = get_analysis(conn, listing.id)
    source_urls = [
        r["source_url"]
        for r in conn.execute(
            "SELECT source_url FROM listing_sources WHERE listing_id = ? AND source_url IS NOT NULL",
            (listing_id,),
        ).fetchall()
    ]

    return templates.TemplateResponse("listing_detail.html", {
        "request": request,
        "listing": listing,
        "analysis": analysis,
        "source_urls": source_urls,
    })


@app.post("/listings/{listing_id}/reanalyze")
async def reanalyze_listing(
    listing_id: int,
    conn: sqlite3.Connection = Depends(get_db_dep),
    profile=Depends(get_profile_dep),
):
    """Re-run LLM analysis for a listing and redirect back to its detail page."""
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash

    listing = get_listing(conn, listing_id)
    if not listing:
        return HTMLResponse("<h1>Listing not found</h1>", status_code=404)
    adapter = ClaudeCliAdapter(model=profile.llm.model)
    result = await analyze_listing(listing, profile, adapter)
    result.profile_hash = compute_profile_hash(profile)
    save_analysis(conn, result)

    return RedirectResponse(f"/listings/{listing_id}", status_code=303)


@app.get("/scan", response_class=HTMLResponse)
async def scan_page(request: Request):
    from jobhaul.collectors import ensure_collectors_registered
    from jobhaul.collectors.registry import _registry

    ensure_collectors_registered()
    available_sources = list(_registry.keys())

    return templates.TemplateResponse("scan.html", {
        "request": request,
        "available_sources": available_sources,
        "results": None,
    })


@app.post("/scan", response_class=HTMLResponse)
async def run_scan(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db_dep),
    profile=Depends(get_profile_dep),
):
    from jobhaul.collectors import ensure_collectors_registered
    from jobhaul.collectors.registry import _registry
    from jobhaul.service import collect_listings, run_analysis as svc_analyze

    ensure_collectors_registered()

    form = await request.form()
    selected_sources = form.getlist("sources")
    do_analysis = form.get("analyze") == "on"

    source = selected_sources[0] if len(selected_sources) == 1 else None
    total_collected, _, _ = await collect_listings(profile, conn, source=source)

    analysis_results = []
    if do_analysis:
        results = await svc_analyze(profile, conn, limit=20)
        for entry in results:
            listing = entry["listing"]
            result = entry.get("result")
            analysis_results.append({
                "title": listing.title,
                "score": result.match_score if result else None,
            })

    available_sources = list(_registry.keys())

    return templates.TemplateResponse("scan.html", {
        "request": request,
        "available_sources": available_sources,
        "results": {
            "scan": [],
            "total": total_collected,
            "analysis": analysis_results if do_analysis else None,
        },
    })


# --- JSON API ---


def _listing_to_dict(listing, analysis=None):
    """Convert a listing and optional analysis to a JSON-safe dict."""
    data = listing.model_dump()
    if analysis:
        data["analysis"] = analysis.model_dump()
    else:
        data["analysis"] = None
    return data


@app.get("/api/listings")
async def api_listings(
    conn: sqlite3.Connection = Depends(get_db_dep),
    source: str | None = Query(None),
    min_score: str | None = Query(None),
    remote_only: bool = Query(False),
    days: str | None = Query("30"),
    limit: str | None = Query(None),
    include_expired: bool = Query(False),
):
    parsed_min_score = _parse_optional_int(min_score)
    parsed_days = _parse_optional_int(days) or 30
    parsed_limit = _parse_optional_int(limit)

    all_pairs = list_listings_with_analysis(
        conn, days=parsed_days, source=source, min_score=parsed_min_score,
        include_expired=include_expired,
    )
    results = []
    for listing, analysis in all_pairs:
        if remote_only and not listing.is_remote:
            continue
        results.append(_listing_to_dict(listing, analysis))
        if parsed_limit and len(results) >= parsed_limit:
            break
    return results


@app.get("/api/listings/{listing_id}")
async def api_listing_detail(listing_id: int, conn: sqlite3.Connection = Depends(get_db_dep)):
    listing = get_listing(conn, listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
    analysis = get_analysis(conn, listing.id)
    return _listing_to_dict(listing, analysis)


@app.get("/api/stats")
async def api_stats(conn: sqlite3.Connection = Depends(get_db_dep)):
    return get_stats(conn)


@app.post("/api/scan")
async def api_scan(
    request: Request,
    conn: sqlite3.Connection = Depends(get_db_dep),
    profile=Depends(get_profile_dep),
):
    from jobhaul.service import collect_listings, run_analysis as svc_analyze

    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    selected_sources = body.get("sources", [])
    do_analysis = body.get("analyze", False)

    source = selected_sources[0] if len(selected_sources) == 1 else None
    total_collected, _, _ = await collect_listings(profile, conn, source=source)

    analysis_results = []
    if do_analysis:
        results = await svc_analyze(profile, conn, limit=20)
        for entry in results:
            listing = entry["listing"]
            result = entry.get("result")
            analysis_results.append({
                "title": listing.title,
                "score": result.match_score if result else None,
                **({"error": entry["error"]} if "error" in entry else {}),
            })

    return {
        "total_collected": total_collected,
        "sources": [],
        "analysis": analysis_results if do_analysis else None,
    }
