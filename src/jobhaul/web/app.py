"""FastAPI web application with HTML pages and JSON API."""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from jobhaul.config import ensure_data_dir
from jobhaul.db.queries import get_analysis, get_listing, get_stats, list_listings, save_analysis
from jobhaul.db.schema import init_db

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

app = FastAPI(title="Jobhaul", description="Personal job market intelligence")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _get_db() -> sqlite3.Connection:
    data_dir = ensure_data_dir()
    return init_db(str(data_dir / "jobhaul.db"))


# --- HTML Routes ---


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    conn = _get_db()
    try:
        stats = get_stats(conn)
        # Get analyzed listings sorted by score (not just recent 10)
        all_analyzed = list_listings(conn, days=30, min_score=1, sort_by_score=True, limit=10)
        top_matches = []
        for listing in all_analyzed:
            analysis = get_analysis(conn, listing.id)
            if analysis:
                top_matches.append({"listing": listing, "analysis": analysis})
    finally:
        conn.close()

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
    source: str | None = Query(None),
    min_score: str | None = Query(None),
    remote_only: bool = Query(False),
    days: str | None = Query("30"),
    sort: str = Query("date"),
    page: int = Query(1, ge=1),
):
    parsed_min_score = _parse_optional_int(min_score)
    parsed_days = _parse_optional_int(days) or 30

    per_page = 50
    conn = _get_db()
    try:
        all_listings = list_listings(
            conn, days=parsed_days, source=source, min_score=parsed_min_score,
            sort_by_score=(sort == "score"),
        )

        listings_with_analysis = []
        for listing in all_listings:
            if remote_only and not listing.is_remote:
                continue
            analysis = get_analysis(conn, listing.id)
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
    finally:
        conn.close()

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
    })


@app.get("/listings/{listing_id}", response_class=HTMLResponse)
async def listing_detail(request: Request, listing_id: int):
    conn = _get_db()
    try:
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
    finally:
        conn.close()

    return templates.TemplateResponse("listing_detail.html", {
        "request": request,
        "listing": listing,
        "analysis": analysis,
        "source_urls": source_urls,
    })


@app.post("/listings/{listing_id}/reanalyze")
async def reanalyze_listing(listing_id: int):
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
    from jobhaul.config import load_profile

    profile = load_profile()
    conn = _get_db()
    try:
        listing = get_listing(conn, listing_id)
        if not listing:
            return HTMLResponse("<h1>Listing not found</h1>", status_code=404)
        adapter = ClaudeCliAdapter(model=profile.llm.model)
        result = await analyze_listing(listing, profile, adapter)
        result.profile_hash = compute_profile_hash(profile)
        save_analysis(conn, result)
    finally:
        conn.close()

    return RedirectResponse(f"/listings/{listing_id}", status_code=303)


@app.get("/scan", response_class=HTMLResponse)
async def scan_page(request: Request):
    from jobhaul.collectors.registry import _registry

    # Import collectors to trigger registration
    import jobhaul.collectors.jooble  # noqa: F401
    import jobhaul.collectors.platsbanken  # noqa: F401
    import jobhaul.collectors.remoteok  # noqa: F401

    try:
        import jobhaul.collectors.linkedin  # noqa: F401
    except ImportError:
        pass
    try:
        import jobhaul.collectors.indeed  # noqa: F401
    except ImportError:
        pass

    available_sources = list(_registry.keys())

    return templates.TemplateResponse("scan.html", {
        "request": request,
        "available_sources": available_sources,
        "results": None,
    })


@app.post("/scan", response_class=HTMLResponse)
async def run_scan(request: Request):
    from jobhaul.collectors.registry import _registry, get_all_collectors, get_collector
    from jobhaul.config import load_profile
    from jobhaul.db.queries import upsert_listing

    # Import collectors to trigger registration
    import jobhaul.collectors.jooble  # noqa: F401
    import jobhaul.collectors.platsbanken  # noqa: F401
    import jobhaul.collectors.remoteok  # noqa: F401

    try:
        import jobhaul.collectors.linkedin  # noqa: F401
    except ImportError:
        pass
    try:
        import jobhaul.collectors.indeed  # noqa: F401
    except ImportError:
        pass

    form = await request.form()
    selected_sources = form.getlist("sources")
    run_analysis = form.get("analyze") == "on"

    profile = load_profile()
    conn = _get_db()
    scan_results = []

    try:
        if selected_sources:
            collectors = [get_collector(s) for s in selected_sources]
        else:
            collectors = get_all_collectors()

        total_collected = 0
        for collector in collectors:
            result = await collector.collect(profile)
            for raw in result.listings:
                upsert_listing(conn, raw)
            total_collected += len(result.listings)
            scan_results.append({
                "source": collector.name,
                "count": len(result.listings),
                "errors": result.errors,
            })

        analysis_results = []
        if run_analysis:
            from jobhaul.analysis.claude_cli import ClaudeCliAdapter
            from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
            from jobhaul.db.queries import get_unanalyzed_listings

            profile_hash = compute_profile_hash(profile)
            adapter = ClaudeCliAdapter(model=profile.llm.model)
            unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=20)

            for listing in unanalyzed:
                try:
                    result = await analyze_listing(listing, profile, adapter)
                    save_analysis(conn, result)
                    analysis_results.append({
                        "title": listing.title,
                        "score": result.match_score,
                    })
                except Exception:
                    analysis_results.append({
                        "title": listing.title,
                        "score": None,
                    })
    finally:
        conn.close()

    available_sources = list(_registry.keys())

    return templates.TemplateResponse("scan.html", {
        "request": request,
        "available_sources": available_sources,
        "results": {
            "scan": scan_results,
            "total": total_collected,
            "analysis": analysis_results if run_analysis else None,
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
    source: str | None = Query(None),
    min_score: str | None = Query(None),
    remote_only: bool = Query(False),
    days: str | None = Query("30"),
    limit: str | None = Query(None),
):
    parsed_min_score = _parse_optional_int(min_score)
    parsed_days = _parse_optional_int(days) or 30
    parsed_limit = _parse_optional_int(limit)

    conn = _get_db()
    try:
        all_listings = list_listings(conn, days=parsed_days, source=source, min_score=parsed_min_score)
        results = []
        for listing in all_listings:
            if remote_only and not listing.is_remote:
                continue
            analysis = get_analysis(conn, listing.id)
            results.append(_listing_to_dict(listing, analysis))
            if parsed_limit and len(results) >= parsed_limit:
                break
    finally:
        conn.close()
    return results


@app.get("/api/listings/{listing_id}")
async def api_listing_detail(listing_id: int):
    conn = _get_db()
    try:
        listing = get_listing(conn, listing_id)
        if not listing:
            raise HTTPException(status_code=404, detail="Listing not found")
        analysis = get_analysis(conn, listing.id)
    finally:
        conn.close()
    return _listing_to_dict(listing, analysis)


@app.get("/api/stats")
async def api_stats():
    conn = _get_db()
    try:
        stats = get_stats(conn)
    finally:
        conn.close()
    return stats


@app.post("/api/scan")
async def api_scan(
    request: Request,
):
    from jobhaul.collectors.registry import get_all_collectors, get_collector
    from jobhaul.config import load_profile
    from jobhaul.db.queries import upsert_listing

    # Import collectors to trigger registration
    import jobhaul.collectors.jooble  # noqa: F401
    import jobhaul.collectors.platsbanken  # noqa: F401
    import jobhaul.collectors.remoteok  # noqa: F401

    try:
        import jobhaul.collectors.linkedin  # noqa: F401
    except ImportError:
        pass
    try:
        import jobhaul.collectors.indeed  # noqa: F401
    except ImportError:
        pass

    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    selected_sources = body.get("sources", [])
    run_analysis = body.get("analyze", False)

    profile = load_profile()
    conn = _get_db()
    scan_results = []

    try:
        if selected_sources:
            collectors = [get_collector(s) for s in selected_sources]
        else:
            collectors = get_all_collectors()

        total_collected = 0
        for collector in collectors:
            result = await collector.collect(profile)
            for raw in result.listings:
                upsert_listing(conn, raw)
            total_collected += len(result.listings)
            scan_results.append({
                "source": collector.name,
                "count": len(result.listings),
                "errors": result.errors,
            })

        analysis_results = []
        if run_analysis:
            from jobhaul.analysis.claude_cli import ClaudeCliAdapter
            from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
            from jobhaul.db.queries import get_unanalyzed_listings

            profile_hash = compute_profile_hash(profile)
            adapter = ClaudeCliAdapter(model=profile.llm.model)
            unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=20)

            for listing in unanalyzed:
                try:
                    result = await analyze_listing(listing, profile, adapter)
                    save_analysis(conn, result)
                    analysis_results.append({
                        "title": listing.title,
                        "score": result.match_score,
                    })
                except Exception as e:
                    analysis_results.append({
                        "title": listing.title,
                        "score": None,
                        "error": str(e),
                    })
    finally:
        conn.close()

    return {
        "total_collected": total_collected,
        "sources": scan_results,
        "analysis": analysis_results if run_analysis else None,
    }
