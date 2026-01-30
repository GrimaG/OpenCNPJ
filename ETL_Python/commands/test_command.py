"""Test command - run integrity tests."""
from rich.console import Console

from ..processors.integrity_tester import IntegrityTester

console = Console()


class TestCommand:
    """Run integrity tests."""
    
    async def execute_async(self) -> int:
        """Execute the test command."""
        try:
            tester = IntegrityTester()
            console.print("[yellow]🧪 Rodando teste de integridade[/]")
            await tester.run_async()
            console.print("[green]✅ Teste de integridade finalizado[/]")
            return 0
        
        except Exception as ex:
            console.print(f"[red]❌ Erro no teste: {ex}[/]")
            console.print_exception()
            return 1
