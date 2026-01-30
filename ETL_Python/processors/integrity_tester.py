import os
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

import xxhash
from rich.console import Console
from rich.progress import Progress

from ..config import get_config
from ..exporters.rclone_client import RcloneClient
from ..utils.json_cleanup_utils import JsonCleanupUtils
from .parquet_ingestor import ParquetIngestor

console = Console()


class IntegrityTester:
    """Test integrity of exported JSON data."""
    
    @staticmethod
    def _compute_hash(json_str: str) -> str:
        """Compute xxHash3 hash of JSON string."""
        h = xxhash.xxh3_64()
        h.update(json_str.encode("utf-8"))
        return h.hexdigest()
    
    async def run_async(self, total: int = 10) -> None:
        """Run integrity test on sample CNPJs."""
        import duckdb
        
        config = get_config()
        
        # Initialize DuckDB connection
        data_source = ":memory:" if config.duckdb.use_in_memory else "./cnpj.duckdb"
        conn = duckdb.connect(data_source)
        
        # Load Parquet views
        await self._load_parquet_views_async(conn)
        
        # Pick sample CNPJs
        sample = await self._pick_sample_async(conn, total)
        
        if not sample:
            console.print("[red]❌ Não foi possível selecionar CNPJs para o teste[/]")
            conn.close()
            return
        
        # Create temp directories
        temp_root = tempfile.mkdtemp(prefix="opencnpj_test_")
        local_json_dir = os.path.join(temp_root, "local")
        remote_json_dir = os.path.join(temp_root, "remote")
        os.makedirs(local_json_dir, exist_ok=True)
        os.makedirs(remote_json_dir, exist_ok=True)
        
        ingestor = ParquetIngestor()
        results = []
        
        try:
            with Progress() as progress:
                task = progress.add_task("[green]Comparando hashes[/]", total=len(sample))
                
                for cnpj in sample:
                    progress.update(task, description=f"[cyan]Processando {cnpj}[/]")
                    
                    note: Optional[str] = None
                    local_path = os.path.join(local_json_dir, f"{cnpj}.json")
                    remote_path = os.path.join(remote_json_dir, f"{cnpj}.json")
                    
                    try:
                        # Generate local JSON
                        await ingestor.export_single_cnpj_async(cnpj, local_json_dir)
                        
                        if not os.path.exists(local_path):
                            raise RuntimeError("JSON local não gerado")
                        
                        with open(local_path, "r", encoding="utf-8") as f:
                            local_json = f.read()
                        
                        local_json = JsonCleanupUtils.clean_json_spaces(local_json)
                        local_hash = self._compute_hash(local_json)
                        
                        # Download from storage
                        ok = await RcloneClient.download_file_async(f"{cnpj}.json", remote_path)
                        if not ok or not os.path.exists(remote_path):
                            raise RuntimeError("Download via rclone falhou ou arquivo não existe no Storage")
                        
                        with open(remote_path, "r", encoding="utf-8") as f:
                            remote_json = f.read()
                        
                        remote_json = JsonCleanupUtils.clean_json_spaces(remote_json)
                        remote_hash = self._compute_hash(remote_json)
                        
                        equal = local_hash.lower() == remote_hash.lower()
                        results.append((cnpj, local_hash, remote_hash, equal, note))
                    
                    except Exception as ex:
                        note = str(ex)
                        results.append((cnpj, "-", "-", False, note))
                    
                    progress.advance(task)
        
        finally:
            ingestor.dispose()
            conn.close()
        
        # Report
        success_count = sum(1 for r in results if r[3])
        
        for cnpj, local_hash, remote_hash, ok, note in results:
            if ok:
                console.print(f"[green]✓ {cnpj}[/] [grey](hash {local_hash})[/]")
            else:
                error_msg = note or "Hashes divergentes ou indisponíveis"
                console.print(f"[red]✗ {cnpj}[/] {error_msg}")
        
        if success_count == len(results):
            console.print(f"[green]✅ {success_count}/{len(results)} CNPJs válidos: hashes idênticos[/]")
        else:
            console.print(f"[red]❌ {success_count}/{len(results)} CNPJs válidos; verifique divergências acima[/]")
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_root, ignore_errors=True)
    
    async def _load_parquet_views_async(self, conn) -> None:
        """Load Parquet files as views in DuckDB."""
        config = get_config()
        base_dir = config.paths.parquet_dir
        
        views = {
            "empresa": "empresa/**/*.parquet",
            "estabelecimento": "estabelecimento/**/*.parquet",
            "socio": "socio/**/*.parquet",
            "simples": "simples/**/*.parquet",
            "cnae": "cnae.parquet",
            "motivo": "motivo.parquet",
            "municipio": "municipio.parquet",
            "natureza": "natureza.parquet",
            "pais": "pais.parquet",
            "qualificacao": "qualificacao.parquet"
        }
        
        for name, pattern in views.items():
            full_path = os.path.join(base_dir, pattern)
            sql = f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{full_path}')"
            conn.execute(sql)
    
    async def _pick_sample_async(self, conn, total: int) -> List[str]:
        """Pick a sample of CNPJs for testing."""
        sample_set = set()
        
        # At least 1 with SIMPLES
        try:
            result = conn.execute("""
                SELECT e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj
                FROM estabelecimento e
                INNER JOIN simples s ON e.cnpj_basico = s.cnpj_basico
                ORDER BY random() LIMIT 1
            """).fetchone()
            if result and result[0]:
                sample_set.add(result[0])
        except Exception:
            pass
        
        # At least 1 with SOCIO
        try:
            result = conn.execute("""
                SELECT e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj
                FROM estabelecimento e
                INNER JOIN socio so ON e.cnpj_basico = so.cnpj_basico
                ORDER BY random() LIMIT 1
            """).fetchone()
            if result and result[0]:
                sample_set.add(result[0])
        except Exception:
            pass
        
        # Fill the rest with random CNPJs
        while len(sample_set) < total:
            remaining = total - len(sample_set)
            try:
                results = conn.execute(f"""
                    SELECT DISTINCT e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj
                    FROM estabelecimento e
                    ORDER BY random() LIMIT {max(remaining * 2, 8)}
                """).fetchall()
                
                for row in results:
                    if row and row[0]:
                        sample_set.add(row[0])
                        if len(sample_set) >= total:
                            break
                
                if not results:
                    break
            except Exception:
                break
        
        return list(sample_set)[:total]
