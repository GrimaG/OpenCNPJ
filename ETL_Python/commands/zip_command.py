"""ZIP command - export all data to ZIP."""
from rich.console import Console

from ..processors.parquet_ingestor import ParquetIngestor

console = Console()


class ZipCommand:
    """Export all data to a ZIP file."""
    
    async def execute_async(self) -> int:
        """Execute the ZIP command."""
        try:
            with ParquetIngestor() as ingestor:
                console.print("[yellow]📦 Exportando para ZIP[/]")
                await ingestor.export_jsons_to_zip("cnpj_json_export")
                console.print("[green]✅ ZIP criado[/]")
            return 0
        
        except Exception as ex:
            console.print(f"[red]❌ Erro ao gerar ZIP: {ex}[/]")
            console.print_exception()
            return 1
