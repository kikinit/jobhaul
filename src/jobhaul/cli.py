"""Typer CLI entrypoint."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from jobhaul.config import ensure_data_dir, init_profile, load_profile
from jobhaul.log import get_logger

logger = get_logger(__name__)
console = Console()

app = typer.Typer(name="jobhaul", help="Personal job market intelligence CLI.")
config_app = typer.Typer(help="Configuration commands.")
app.add_typer(config_app, name="config")


def _get_db():
    from jobhaul.db.schema import init_db

    data_dir = ensure_data_dir()
    return init_db(str(data_dir / "jobhaul.db"))


@app.command()
def scan(
    source: Optional[str] = typer.Option(None, help="Single source to collect from"),
    skip_analysis: bool = typer.Option(False, "--skip-analysis", help="Collect only, no analysis"),
    analyze_only: bool = typer.Option(
        False, "--analyze-only", help="Skip collection, only analyze unanalyzed listings"
    ),
    retry_failed: bool = typer.Option(
        False, "--retry-failed", help="Re-queue only listings with analysis errors"
    ),
    limit: Optional[int] = typer.Option(None, help="Max number of listings to analyze"),
):
    """Collect job listings and optionally analyze them."""
    asyncio.run(_scan(source, skip_analysis, analyze_only, retry_failed, limit))


async def _scan(
    source: str | None, skip_analysis: bool, analyze_only: bool,
    retry_failed: bool, limit: int | None,
):
    profile = load_profile()
    conn = _get_db()
    flags = profile.get_effective_flags()

    if retry_failed:
        # Only re-queue failed analyses, skip collection
        from jobhaul.analysis.claude_cli import ClaudeCliAdapter
        from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash, pre_screen
        from jobhaul.db.queries import get_failed_listings, save_analysis
        from jobhaul.flagging import flag_listing

        profile_hash = compute_profile_hash(profile)
        adapter = ClaudeCliAdapter(model=profile.llm.model)
        failed = get_failed_listings(conn, profile_hash, limit=limit)

        if not failed:
            console.print("[dim]No failed analyses to retry.[/dim]")
            conn.close()
            return

        console.print(f"\n[bold]Retrying {len(failed)} failed analyses...[/bold]")
        for listing in failed:
            try:
                result = await analyze_listing(listing, profile, adapter)
                save_analysis(conn, result)
                if result.analysis_error:
                    console.print(
                        f"  [yellow]Still failing: {listing.title} @ "
                        f"{listing.company or '?'}[/yellow]"
                    )
                else:
                    console.print(
                        f"  [green]Recovered: {listing.title} @ "
                        f"{listing.company or '?'}: score {result.match_score}[/green]"
                    )
            except Exception as e:
                console.print(f"  [red]Error analyzing {listing.title}: {e}[/red]")

        conn.close()
        return

    collected_external_ids: set[str] = set()

    if not analyze_only:
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

        from jobhaul.collectors.registry import get_all_collectors, get_collector
        from jobhaul.db.queries import upsert_listing
        from jobhaul.flagging import flag_listing
        from jobhaul.models import JobListing

        if source:
            collectors = [get_collector(source)]
        else:
            collectors = get_all_collectors()

        total_collected = 0
        skipped_excluded = 0
        for collector in collectors:
            console.print(f"[bold]Collecting from {collector.name}...[/bold]")
            result = await collector.collect(profile)
            for raw in result.listings:
                # Check exclusion before inserting
                temp_listing = JobListing(
                    title=raw.title,
                    company=raw.company,
                    description=raw.description,
                )
                flag_result = flag_listing(temp_listing, flags)
                if flag_result["excluded"]:
                    logger.info("Excluded listing: %s (matched exclude filter)", raw.title)
                    skipped_excluded += 1
                    continue
                upsert_listing(conn, raw)
                collected_external_ids.add(raw.external_id)
            total_collected += len(result.listings) - skipped_excluded

            if result.errors:
                for err in result.errors:
                    console.print(f"  [yellow]Warning: {err}[/yellow]")

            console.print(f"  Collected {len(result.listings)} listings")

        console.print(f"\n[green]Total: {total_collected} listings collected[/green]")
        if skipped_excluded:
            console.print(f"[dim]Skipped {skipped_excluded} excluded listings[/dim]")

        # Expiry check: mark listings with past deadline not seen in this scan
        _check_expiry(conn, collected_external_ids)

    if not skip_analysis:
        from jobhaul.analysis.claude_cli import ClaudeCliAdapter
        from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash, pre_screen
        from jobhaul.db.queries import get_unanalyzed_listings, save_analysis
        from jobhaul.flagging import flag_listing

        profile_hash = compute_profile_hash(profile)
        adapter = ClaudeCliAdapter(model=profile.llm.model)
        unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=limit)

        if not unanalyzed:
            console.print("[dim]No new listings to analyze.[/dim]")
            conn.close()
            return

        console.print(f"\n[bold]Analyzing {len(unanalyzed)} listings...[/bold]")
        threshold = profile.analysis.pre_screen_threshold
        for listing in unanalyzed:
            # Check exclusion
            flag_result = flag_listing(listing, flags)
            if flag_result["excluded"]:
                console.print(
                    f"  [dim]Skipping '{listing.title}' (excluded by flag filter)[/dim]"
                )
                continue

            # Pre-screen
            score = pre_screen(listing, profile)
            if score < threshold:
                console.print(
                    f"  [dim]Skipping '{listing.title}' (pre-screen: {score:.2f})[/dim]"
                )
                continue

            try:
                result = await analyze_listing(listing, profile, adapter)
                save_analysis(conn, result)
                emoji = "+" if result.match_score >= 50 else "-"

                # Show flags
                flag_info = ""
                if flag_result["boost"]:
                    flag_info += " \u2705 " + ", ".join(flag_result["boost"])
                if flag_result["warn"]:
                    flag_info += " \u26a0\ufe0f " + ", ".join(flag_result["warn"])

                console.print(
                    f"  [{emoji}] {listing.title} @ {listing.company or '?'}: "
                    f"score {result.match_score}{flag_info}"
                )
            except Exception as e:
                console.print(f"  [red]Error analyzing {listing.title}: {e}[/red]")

    conn.close()


def _check_expiry(conn, collected_external_ids: set[str]) -> None:
    """Mark listings as likely_expired if deadline is past and not seen in scan."""
    from jobhaul.db.queries import mark_likely_expired

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT l.id, l.title, l.application_deadline
           FROM listings l
           WHERE l.listing_status = 'active'
             AND l.application_deadline IS NOT NULL
             AND l.application_deadline < ?""",
        (now,),
    ).fetchall()

    # Filter to those not seen in latest scan
    marked = 0
    for row in rows:
        # Check if this listing has any source with an external_id in the collected set
        source_ids = conn.execute(
            "SELECT external_id FROM listing_sources WHERE listing_id = ?",
            (row["id"],),
        ).fetchall()
        seen = any(r["external_id"] in collected_external_ids for r in source_ids)
        if not seen:
            mark_likely_expired(conn, row["id"])
            marked += 1

    if marked:
        console.print(f"[dim]Marked {marked} listing(s) as likely expired[/dim]")


@app.command("list")
def list_listings(
    top: Optional[int] = typer.Option(None, help="Show top N listings"),
    source: Optional[str] = typer.Option(None, help="Filter by source"),
    min_score: Optional[int] = typer.Option(None, "--min-score", help="Minimum match score"),
    days: int = typer.Option(7, help="Listings from last N days"),
):
    """List recent job listings."""
    from jobhaul.db.queries import get_analysis
    from jobhaul.db.queries import list_listings as db_list
    from jobhaul.flagging import flag_listing

    profile = load_profile()
    flags = profile.get_effective_flags()

    sort_by_score = top is not None or min_score is not None
    conn = _get_db()
    listings = db_list(
        conn, days=days, source=source, min_score=min_score,
        limit=top if not sort_by_score else None,
        sort_by_score=sort_by_score,
    )

    if not listings:
        console.print("[dim]No listings found.[/dim]")
        conn.close()
        return

    # Apply limit after score sorting if needed
    if sort_by_score and top:
        listings = listings[:top]

    table = Table(title="Job Listings")
    table.add_column("ID", style="cyan", width=5)
    table.add_column("Title", style="bold")
    table.add_column("Company")
    table.add_column("Location")
    table.add_column("Remote")
    table.add_column("Sources")
    table.add_column("Score", justify="right")
    table.add_column("Flags")

    for listing in listings:
        analysis = get_analysis(conn, listing.id)
        score = str(analysis.match_score) if analysis else "-"

        # Flag display
        flag_result = flag_listing(listing, flags)
        flag_parts = []
        if flag_result["boost"]:
            flag_parts.append("\u2705 " + ",".join(flag_result["boost"]))
        if flag_result["warn"]:
            flag_parts.append("\u26a0\ufe0f " + ",".join(flag_result["warn"]))
        flag_str = " ".join(flag_parts)

        table.add_row(
            str(listing.id),
            listing.title[:50],
            (listing.company or "?")[:25],
            (listing.location or "?")[:20],
            "Yes" if listing.is_remote else "No",
            ", ".join(listing.sources),
            score,
            flag_str,
        )

    console.print(table)
    conn.close()


@app.command()
def show(listing_id: int = typer.Argument(..., help="Listing ID to show")):
    """Show full detail for a listing including analysis."""
    from jobhaul.db.queries import get_analysis, get_listing
    from jobhaul.flagging import flag_listing

    profile = load_profile()
    flags = profile.get_effective_flags()

    conn = _get_db()
    listing = get_listing(conn, listing_id)

    if not listing:
        console.print(f"[red]Listing {listing_id} not found.[/red]")
        conn.close()
        raise typer.Exit(1)

    console.print(f"\n[bold]{listing.title}[/bold]")
    console.print(f"Company: {listing.company or '?'}")
    console.print(f"Location: {listing.location or '?'}")
    console.print(f"Remote: {'Yes' if listing.is_remote else 'No'}")
    console.print(f"Employment: {listing.employment_type or '?'}")
    console.print(f"Sources: {', '.join(listing.sources)}")
    console.print(f"Published: {listing.published_at or '?'}")

    # Show flags
    flag_result = flag_listing(listing, flags)
    if flag_result["boost"]:
        console.print(f"\u2705 Boost: {', '.join(flag_result['boost'])}")
    if flag_result["warn"]:
        console.print(f"\u26a0\ufe0f Warn: {', '.join(flag_result['warn'])}")

    if listing.url:
        console.print(f"URL: {listing.url}")

    if listing.description:
        console.print(f"\n[bold]Description:[/bold]\n{listing.description[:2000]}")

    analysis = get_analysis(conn, listing.id)
    if analysis:
        console.print(f"\n[bold]Analysis (score: {analysis.match_score}/100):[/bold]")
        if analysis.summary:
            console.print(f"Summary: {analysis.summary}")
        if analysis.match_reasons:
            console.print("Match reasons:")
            for reason in analysis.match_reasons:
                console.print(f"  - {reason}")
        if analysis.strengths:
            console.print("Strengths:")
            for strength in analysis.strengths:
                console.print(f"  - {strength}")
        if analysis.missing_skills:
            console.print("Missing skills:")
            for skill in analysis.missing_skills:
                console.print(f"  - {skill}")
        if analysis.concerns:
            console.print("Concerns:")
            for concern in analysis.concerns:
                console.print(f"  - {concern}")
        if analysis.application_notes:
            console.print(f"Application notes: {analysis.application_notes}")
    else:
        console.print("\n[dim]No analysis yet. Run 'jobhaul analyze {listing_id}'.[/dim]")

    conn.close()


@app.command()
def analyze(
    listing_id: int = typer.Argument(0, help="Listing ID to analyze"),
    all_: bool = typer.Option(False, "--all", help="Analyze all unanalyzed listings"),
    limit: Optional[int] = typer.Option(None, help="Max listings to analyze (with --all)"),
):
    """(Re-)analyze a single listing, or all unanalyzed listings with --all."""
    asyncio.run(_analyze(listing_id, all_, limit))


async def _analyze(listing_id: int, all_: bool, limit: int | None):
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
    from jobhaul.db.queries import get_listing, get_unanalyzed_listings, save_analysis

    profile = load_profile()
    conn = _get_db()
    adapter = ClaudeCliAdapter(model=profile.llm.model)
    profile_hash = compute_profile_hash(profile)

    if all_:
        unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=limit)
        if not unanalyzed:
            console.print("[dim]No unanalyzed listings.[/dim]")
            conn.close()
            return

        console.print(f"[bold]Analyzing {len(unanalyzed)} listings...[/bold]")
        for listing in unanalyzed:
            try:
                result = await analyze_listing(listing, profile, adapter)
                result.profile_hash = profile_hash
                save_analysis(conn, result)
                console.print(
                    f"  {listing.title} @ {listing.company or '?'}: "
                    f"score {result.match_score}"
                )
            except Exception as e:
                console.print(f"  [red]Error analyzing {listing.title}: {e}[/red]")
    else:
        if listing_id == 0:
            console.print("[red]Provide a listing ID or use --all.[/red]")
            conn.close()
            raise typer.Exit(1)

        listing = get_listing(conn, listing_id)
        if not listing:
            console.print(f"[red]Listing {listing_id} not found.[/red]")
            conn.close()
            raise typer.Exit(1)

        console.print(f"[bold]Analyzing: {listing.title}...[/bold]")
        result = await analyze_listing(listing, profile, adapter)
        result.profile_hash = profile_hash
        save_analysis(conn, result)

        console.print(f"[green]Score: {result.match_score}/100[/green]")
        if result.summary:
            console.print(f"Summary: {result.summary}")

    conn.close()


@app.command()
def stats():
    """Show summary statistics."""
    from jobhaul.db.queries import get_stats

    conn = _get_db()
    s = get_stats(conn)

    console.print("\n[bold]Jobhaul Statistics[/bold]")
    console.print(f"Total unique listings: {s['total_listings']}")
    console.print(f"Total source entries: {s['total_source_entries']}")
    console.print(f"Dedup savings: {s['dedup_savings']} duplicates merged")
    console.print(f"Total analyses: {s['total_analyses']}")
    console.print(f"Average score: {s['avg_score']}")
    console.print("\nListings by source:")
    for src, count in s["source_counts"].items():
        console.print(f"  {src}: {count}")

    conn.close()


@config_app.command("show")
def config_show():
    """Print the current profile configuration."""
    import yaml

    profile = load_profile()
    console.print(yaml.dump(profile.model_dump(), default_flow_style=False, allow_unicode=True))


@config_app.command("init")
def config_init():
    """Create a default profile.yaml from the example."""
    try:
        path = init_profile()
        console.print(f"[green]Profile created at {path}[/green]")
        console.print("Edit it to customize your job search preferences.")
    except FileExistsError as e:
        console.print(f"[yellow]{e}[/yellow]")
        raise typer.Exit(1)


@app.command()
def serve(
    port: int = typer.Option(8080, help="Port to serve on"),
    host: str = typer.Option("127.0.0.1", help="Host to bind to"),
):
    """Start the web dashboard."""
    import uvicorn

    from jobhaul.web.app import app as web_app

    console.print(f"[bold]Starting Jobhaul web UI at http://{host}:{port}[/bold]")
    uvicorn.run(web_app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    app()
