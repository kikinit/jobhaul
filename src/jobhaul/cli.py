"""Typer CLI entrypoint."""

from __future__ import annotations

import asyncio
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
    limit: Optional[int] = typer.Option(None, help="Max number of listings to analyze"),
):
    """Collect job listings and optionally analyze them."""
    asyncio.run(_scan(source, skip_analysis, limit))


async def _scan(source: str | None, skip_analysis: bool, limit: int | None):
    profile = load_profile()
    conn = _get_db()

    # Import collectors to trigger registration
    import jobhaul.collectors.jooble  # noqa: F401
    import jobhaul.collectors.platsbanken  # noqa: F401
    import jobhaul.collectors.remoteok  # noqa: F401
    from jobhaul.collectors.registry import get_all_collectors, get_collector
    from jobhaul.db.queries import upsert_listing

    if source:
        collectors = [get_collector(source)]
    else:
        collectors = get_all_collectors()

    total_collected = 0
    for collector in collectors:
        console.print(f"[bold]Collecting from {collector.name}...[/bold]")
        result = await collector.collect(profile)
        for raw in result.listings:
            upsert_listing(conn, raw)
        total_collected += len(result.listings)

        if result.errors:
            for err in result.errors:
                console.print(f"  [yellow]Warning: {err}[/yellow]")

        console.print(f"  Collected {len(result.listings)} listings")

    console.print(f"\n[green]Total: {total_collected} listings collected[/green]")

    if not skip_analysis:
        from jobhaul.analysis.claude_cli import ClaudeCliAdapter
        from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
        from jobhaul.db.queries import get_unanalyzed_listings, save_analysis

        profile_hash = compute_profile_hash(profile)
        adapter = ClaudeCliAdapter(model=profile.llm.model)
        unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=limit)

        if not unanalyzed:
            console.print("[dim]No new listings to analyze.[/dim]")
            return

        console.print(f"\n[bold]Analyzing {len(unanalyzed)} listings...[/bold]")
        for listing in unanalyzed:
            try:
                result = await analyze_listing(listing, profile, adapter)
                save_analysis(conn, result)
                emoji = "+" if result.match_score >= 50 else "-"
                console.print(
                    f"  [{emoji}] {listing.title} @ {listing.company or '?'}: "
                    f"score {result.match_score}"
                )
            except Exception as e:
                console.print(f"  [red]Error analyzing {listing.title}: {e}[/red]")

    conn.close()


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

    conn = _get_db()
    listings = db_list(conn, days=days, source=source, min_score=min_score, limit=top)

    if not listings:
        console.print("[dim]No listings found.[/dim]")
        conn.close()
        return

    table = Table(title="Job Listings")
    table.add_column("ID", style="cyan", width=5)
    table.add_column("Title", style="bold")
    table.add_column("Company")
    table.add_column("Location")
    table.add_column("Remote")
    table.add_column("Sources")
    table.add_column("Score", justify="right")

    for listing in listings:
        analysis = get_analysis(conn, listing.id)
        score = str(analysis.match_score) if analysis else "-"
        table.add_row(
            str(listing.id),
            listing.title[:50],
            (listing.company or "?")[:25],
            (listing.location or "?")[:20],
            "Yes" if listing.is_remote else "No",
            ", ".join(listing.sources),
            score,
        )

    console.print(table)
    conn.close()


@app.command()
def show(listing_id: int = typer.Argument(..., help="Listing ID to show")):
    """Show full detail for a listing including analysis."""
    from jobhaul.db.queries import get_analysis, get_listing

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
            console.print(f"Match reasons: {analysis.match_reasons}")
        if analysis.strengths:
            console.print(f"Strengths: {analysis.strengths}")
        if analysis.missing_skills:
            console.print(f"Missing skills: {analysis.missing_skills}")
        if analysis.concerns:
            console.print(f"Concerns: {analysis.concerns}")
        if analysis.application_notes:
            console.print(f"Application notes: {analysis.application_notes}")
    else:
        console.print("\n[dim]No analysis yet. Run 'jobhaul analyze {listing_id}'.[/dim]")

    conn.close()


@app.command()
def analyze(listing_id: int = typer.Argument(..., help="Listing ID to analyze")):
    """(Re-)analyze a single listing."""
    asyncio.run(_analyze(listing_id))


async def _analyze(listing_id: int):
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
    from jobhaul.db.queries import get_listing, save_analysis

    profile = load_profile()
    conn = _get_db()

    listing = get_listing(conn, listing_id)
    if not listing:
        console.print(f"[red]Listing {listing_id} not found.[/red]")
        raise typer.Exit(1)

    adapter = ClaudeCliAdapter(model=profile.llm.model)
    console.print(f"[bold]Analyzing: {listing.title}...[/bold]")

    result = await analyze_listing(listing, profile, adapter)
    result.profile_hash = compute_profile_hash(profile)
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


if __name__ == "__main__":
    app()
