"""Command-line interface for Jobhaul.

Defines all Typer commands that a user can run from the terminal, including
scanning for new listings, listing/showing results, triggering analysis, and
launching the web dashboard. This module is the main entrypoint for the CLI.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from jobhaul.config import init_profile, load_profile
from jobhaul.constants import MAX_COMPANY_DISPLAY_CHARS, MAX_TITLE_DISPLAY_CHARS
from jobhaul.db import async_get_db, get_db
from jobhaul.log import get_logger

logger = get_logger(__name__)
console = Console()

app = typer.Typer(name="jobhaul", help="Personal job market intelligence CLI.")
config_app = typer.Typer(help="Configuration commands.")
app.add_typer(config_app, name="config")
db_app = typer.Typer(help="Database maintenance commands.")
app.add_typer(db_app, name="db")


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
    """Collect job listings from configured sources and optionally analyze them.

    By default this command collects new listings and then runs LLM analysis on
    any that have not been analyzed yet. Use the flags to run only collection,
    only analysis, or to retry previously failed analyses.
    """
    asyncio.run(_scan(source, skip_analysis, analyze_only, retry_failed, limit))


async def _scan(
    source: str | None, skip_analysis: bool, analyze_only: bool,
    retry_failed: bool, limit: int | None,
):
    profile = load_profile()
    flags = profile.get_effective_flags()

    async with async_get_db() as conn:
        if retry_failed:
            await _handle_retry_failed(profile, conn, limit)
            return

        if not analyze_only:
            await _collect_listings(profile, conn, source, flags)

        if not skip_analysis:
            await _run_analysis(profile, conn, flags, limit)


async def _handle_retry_failed(profile, conn, limit):
    """Retry failed analyses."""
    from jobhaul.service import retry_failed_analyses

    results = await retry_failed_analyses(profile, conn, limit=limit)

    if not results:
        console.print("[dim]No failed analyses to retry.[/dim]")
        return

    console.print(f"\n[bold]Retrying {len(results)} failed analyses...[/bold]")
    for entry in results:
        listing = entry["listing"]
        result = entry.get("result")
        if "error" in entry:
            console.print(f"  [red]Error analyzing {listing.title}: {entry['error']}[/red]")
        elif result and result.analysis_error:
            console.print(
                f"  [yellow]Still failing: {listing.title} @ "
                f"{listing.company or '?'}[/yellow]"
            )
        elif result:
            console.print(
                f"  [green]Recovered: {listing.title} @ "
                f"{listing.company or '?'}: score {result.match_score}[/green]"
            )


async def _collect_listings(profile, conn, source, flags):
    """Run collection and report results."""
    from jobhaul.db.queries import check_and_mark_expired
    from jobhaul.service import collect_listings

    total_collected, skipped_excluded, collected_ids = await collect_listings(
        profile, conn, source=source, flags=flags,
    )
    console.print(f"\n[green]Total: {total_collected} listings collected[/green]")
    if skipped_excluded:
        console.print(f"[dim]Skipped {skipped_excluded} excluded listings[/dim]")

    marked = check_and_mark_expired(conn, collected_ids)
    if marked:
        console.print(f"[dim]Marked {marked} listing(s) as likely expired[/dim]")


async def _run_analysis(profile, conn, flags, limit):
    """Run analysis on unanalyzed listings and print results."""
    from jobhaul.service import run_analysis

    results = await run_analysis(profile, conn, flags=flags, limit=limit)

    if not results:
        console.print("[dim]No new listings to analyze.[/dim]")
        return

    console.print(f"\n[bold]Analyzing {len(results)} listings...[/bold]")
    for entry in results:
        listing = entry["listing"]
        result = entry.get("result")
        if "error" in entry:
            console.print(f"  [red]Error analyzing {listing.title}: {entry['error']}[/red]")
        elif result:
            emoji = "+" if result.match_score >= 50 else "-"
            flag_info = ""
            flag_data = entry.get("flags")
            if flag_data:
                if flag_data["boost"]:
                    flag_info += " \u2705 " + ", ".join(flag_data["boost"])
                if flag_data["warn"]:
                    flag_info += " \u26a0\ufe0f " + ", ".join(flag_data["warn"])
            console.print(
                f"  [{emoji}] {listing.title} @ {listing.company or '?'}: "
                f"score {result.match_score}{flag_info}"
            )


@app.command("list")
def list_listings(
    top: Optional[int] = typer.Option(None, help="Show top N listings"),
    source: Optional[str] = typer.Option(None, help="Filter by source"),
    min_score: Optional[int] = typer.Option(None, "--min-score", help="Minimum match score"),
    days: int = typer.Option(7, help="Listings from last N days"),
):
    """List recent job listings in a Rich table.

    Supports filtering by source, minimum match score, and time window. When
    ``--top`` or ``--min-score`` is provided, results are sorted by score.
    """
    from jobhaul.db.queries import get_analysis
    from jobhaul.db.queries import list_listings as db_list
    from jobhaul.flagging import flag_listing

    profile = load_profile()
    flags = profile.get_effective_flags()

    sort_by_score = top is not None or min_score is not None
    with get_db() as conn:
        listings = db_list(
            conn, days=days, source=source, min_score=min_score,
            limit=top if not sort_by_score else None,
            sort_by_score=sort_by_score,
        )

        if not listings:
            console.print("[dim]No listings found.[/dim]")
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
                listing.title[:MAX_TITLE_DISPLAY_CHARS],
                (listing.company or "?")[:MAX_COMPANY_DISPLAY_CHARS],
                (listing.location or "?")[:20],
                "Yes" if listing.is_remote else "No",
                ", ".join(listing.sources),
                score,
                flag_str,
            )

        console.print(table)


@app.command()
def show(listing_id: int = typer.Argument(..., help="Listing ID to show")):
    """Show full detail for a single listing including its analysis results.

    Prints the listing metadata, description (truncated), flag information,
    and the complete LLM analysis breakdown (score, reasons, strengths,
    missing skills, concerns, and application notes).
    """
    from jobhaul.db.queries import get_analysis, get_listing
    from jobhaul.flagging import flag_listing

    profile = load_profile()
    flags = profile.get_effective_flags()

    with get_db() as conn:
        listing = get_listing(conn, listing_id)

        if not listing:
            console.print(f"[red]Listing {listing_id} not found.[/red]")
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


@app.command()
def analyze(
    listing_id: int = typer.Argument(0, help="Listing ID to analyze"),
    all_: bool = typer.Option(False, "--all", help="Analyze all unanalyzed listings"),
    limit: Optional[int] = typer.Option(None, help="Max listings to analyze (with --all)"),
):
    """Run LLM analysis on a single listing or all unanalyzed listings.

    Pass a listing ID to analyze (or re-analyze) one specific listing. Use
    ``--all`` to process every listing that has not been analyzed yet, with an
    optional ``--limit`` to cap the number of listings processed in one run.
    """
    asyncio.run(_analyze(listing_id, all_, limit))


async def _analyze(listing_id: int, all_: bool, limit: int | None):
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
    from jobhaul.db.queries import get_listing, get_unanalyzed_listings, save_analysis

    profile = load_profile()
    adapter = ClaudeCliAdapter(model=profile.llm.model)
    profile_hash = compute_profile_hash(profile)

    async with async_get_db() as conn:
        if all_:
            unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=limit)
            if not unanalyzed:
                console.print("[dim]No unanalyzed listings.[/dim]")
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
                raise typer.Exit(1)

            listing = get_listing(conn, listing_id)
            if not listing:
                console.print(f"[red]Listing {listing_id} not found.[/red]")
                raise typer.Exit(1)

            console.print(f"[bold]Analyzing: {listing.title}...[/bold]")
            result = await analyze_listing(listing, profile, adapter)
            result.profile_hash = profile_hash
            save_analysis(conn, result)

            console.print(f"[green]Score: {result.match_score}/100[/green]")
            if result.summary:
                console.print(f"Summary: {result.summary}")


@app.command()
def stats():
    """Show summary statistics about the local database.

    Displays total listings, source entry counts, dedup savings, analysis
    counts, average score, and a per-source breakdown.
    """
    from jobhaul.db.queries import get_stats

    with get_db() as conn:
        s = get_stats(conn)

    console.print("\n[bold]Jobhaul Statistics[/bold]")
    console.print(f"Total unique listings: {s.total_listings}")
    console.print(f"Total source entries: {s.total_source_entries}")
    console.print(f"Dedup savings: {s.dedup_savings} duplicates merged")
    console.print(f"Total analyses: {s.total_analyses}")
    console.print(f"Average score: {s.avg_score}")
    console.print("\nListings by source:")
    for src, count in s.source_counts.items():
        console.print(f"  {src}: {count}")


@config_app.command("show")
def config_show():
    """Print the current profile configuration as YAML."""
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


@db_app.command("maintenance")
def db_maintenance():
    """Run database maintenance tasks.

    Scans for duplicate listings that share the same dedup key and merges
    them, preserving all source information on the surviving record.
    """
    from jobhaul.db.queries import merge_existing_duplicates

    with get_db() as conn:
        merged = merge_existing_duplicates(conn)
    if merged:
        console.print(f"[green]Merged {merged} duplicate(s)[/green]")
    else:
        console.print("[dim]No duplicates found.[/dim]")


@app.command()
def serve(
    port: int = typer.Option(8080, help="Port to serve on"),
    host: str = typer.Option("127.0.0.1", help="Host to bind to"),
):
    """Start the web dashboard.

    Launches a Uvicorn server hosting the FastAPI web application, which
    provides an HTML UI and a JSON API for browsing listings and results.
    """
    import uvicorn

    from jobhaul.web.app import app as web_app

    console.print(f"[bold]Starting Jobhaul web UI at http://{host}:{port}[/bold]")
    uvicorn.run(web_app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    app()
