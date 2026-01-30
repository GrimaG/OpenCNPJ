"""Pipeline command - full ETL pipeline."""
import asyncio
from datetime import datetime
from typing import Optional

from rich.console import Console

from ..config import get_config
from ..downloaders.web_downloader import WebDownloader
from ..processors.parquet_ingestor import ParquetIngestor
from ..processors.integrity_tester import IntegrityTester

console = Console()


class PipelineCommand:
    """Execute the full ETL pipeline."""
    
    def __init__(self, month: Optional[str] = None):
        self.month = month or datetime.now().strftime("%Y-%m")
    
    async def execute_async(self) -> int:
        """Execute the pipeline."""
        config = get_config()
        
        try:
            console.print(f"[cyan]1/6 Baixando dados de {self.month}...[/]")
            downloader = WebDownloader(config.paths.download_dir, config.paths.data_dir)
            await downloader.download_and_extract_async(self.month)
            
            with ParquetIngestor() as ingestor:
                console.print("[cyan]2/6 Convertendo CSVs para Parquet...[/]")
                await ingestor.convert_csvs_to_parquet()
                
                console.print("[cyan]3/6 Export + Upload integrado para Storage...[/]")
                await ingestor.export_and_upload_to_storage(config.paths.output_dir)
                
                console.print("[cyan]4/6 Testando integridade por amostragem...[/]")
                tester = IntegrityTester()
                await tester.run_async()
                
                console.print("[cyan]5/6 Gerando ZIP consolidado...[/]")
                zip_path = await ingestor.export_jsons_to_zip("cnpj_json_export")
                
                console.print("[cyan]6/6 Gerando e enviando estatística final...[/]")
                await ingestor.generate_and_upload_final_info_json_async(zip_path)
            
            console.print("[green]✅ Pipeline completo concluído![/]")
            return 0
        
        except Exception as ex:
            console.print(f"[red]❌ Erro no pipeline: {ex}[/]")
            console.print_exception()
            return 1
