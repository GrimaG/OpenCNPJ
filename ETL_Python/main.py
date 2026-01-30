#!/usr/bin/env python3
"""
OpenCNPJ ETL Processor - Main entry point
"""
import asyncio
import sys
from typing import Optional

import click
from rich.console import Console

from .config import AppConfig, set_config
from .commands import PipelineCommand, SingleCommand, TestCommand, ZipCommand

console = Console()


@click.group()
@click.option(
    "--config",
    type=click.Path(exists=True),
    help="Path to configuration file (config.json)",
)
def cli(config: Optional[str]):
    """OpenCNPJ ETL Processor - Process CNPJ data from Receita Federal."""
    console.print("[bold blue]🚀 OpenCNPJ ETL Processor[/]")
    
    # Load configuration
    app_config = AppConfig.load(config)
    set_config(app_config)


@cli.command()
@click.option(
    "--cnpj",
    "-c",
    required=True,
    help="CNPJ (14 dígitos)",
)
def single(cnpj: str):
    """Process a specific CNPJ."""
    try:
        command = SingleCommand(cnpj)
        exit_code = asyncio.run(command.execute_async())
        sys.exit(exit_code)
    except ValueError as ex:
        console.print(f"[red]❌ {ex}[/]")
        sys.exit(1)


@cli.command()
def test():
    """Test data integrity with sampling."""
    command = TestCommand()
    exit_code = asyncio.run(command.execute_async())
    sys.exit(exit_code)


@cli.command()
def zip():
    """Generate consolidated ZIP file."""
    command = ZipCommand()
    exit_code = asyncio.run(command.execute_async())
    sys.exit(exit_code)


@cli.command()
@click.option(
    "--month",
    "-m",
    help="Month (YYYY-MM). Default: previous month",
)
def pipeline(month: Optional[str]):
    """Run full pipeline (download → ingest → upload → test → zip)."""
    command = PipelineCommand(month)
    exit_code = asyncio.run(command.execute_async())
    sys.exit(exit_code)


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
