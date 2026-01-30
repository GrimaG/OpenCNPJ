"""Single CNPJ command - process a specific CNPJ."""
from rich.console import Console

from ..config import get_config
from ..processors.parquet_ingestor import ParquetIngestor
from ..utils.cnpj_utils import CnpjUtils

console = Console()


class SingleCommand:
    """Process a single CNPJ."""
    
    def __init__(self, cnpj: str):
        if not CnpjUtils.is_valid_format(cnpj):
            raise ValueError("CNPJ inválido. Informe um CNPJ com 14 dígitos.")
        self.cnpj = cnpj
    
    async def execute_async(self) -> int:
        """Execute the single CNPJ command."""
        config = get_config()
        
        try:
            with ParquetIngestor() as ingestor:
                console.print(f"[yellow]🎯 Processando CNPJ {self.cnpj}[/]")
                await ingestor.export_single_cnpj_async(self.cnpj, config.paths.output_dir)
                console.print("[green]✅ CNPJ processado[/]")
            return 0
        
        except Exception as ex:
            console.print(f"[red]❌ Erro ao processar CNPJ: {ex}[/]")
            console.print_exception()
            return 1
