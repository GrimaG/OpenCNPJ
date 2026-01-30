import asyncio
import os
from pathlib import Path
from typing import Optional, List

from rich.console import Console
from rich.progress import Progress, TaskID
import xxhash

from ..exporters.rclone_client import RcloneClient
from ..utils.hash_cache_manager import HashCacheManager, ProcessedItem
from ..utils.json_cleanup_utils import JsonCleanupUtils

console = Console()


class NdjsonProcessor:
    """Process NDJSON files and upload to storage."""
    
    @staticmethod
    def _compute_hash(json_str: str) -> str:
        """Compute xxHash3 hash of JSON string."""
        h = xxhash.xxh3_64()
        h.update(json_str.encode("utf-8"))
        return h.hexdigest()
    
    def _read_and_process_ndjson(
        self,
        ndjson_file_path: str,
        progress: Progress,
        task_id: TaskID
    ) -> List[ProcessedItem]:
        """Read and process NDJSON file."""
        import json
        
        processed_data = []
        
        try:
            with open(ndjson_file_path, "r", encoding="utf-8", buffering=1024*1024) as f:
                lines = f.readlines()
            
            # Process lines in parallel using thread pool
            def process_line(line: str) -> Optional[ProcessedItem]:
                line = line.strip()
                if not line:
                    return None
                
                cnpj, clean_json = self._extract_cnpj_and_json(line)
                if not cnpj:
                    return None
                
                hash_value = self._compute_hash(clean_json)
                return ProcessedItem(cnpj, clean_json, hash_value)
            
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor() as executor:
                results = list(executor.map(process_line, lines))
            
            processed_data = [item for item in results if item is not None]
        
        except Exception as ex:
            console.print(f"[red]Erro processando {ndjson_file_path}: {ex}[/]")
        
        return processed_data
    
    async def process_ndjson_file_to_storage(
        self,
        ndjson_file_path: str,
        progress: Progress,
        task_id: TaskID
    ) -> None:
        """Process NDJSON file and upload to storage."""
        processed_data = self._read_and_process_ndjson(ndjson_file_path, progress, task_id)
        
        if not processed_data:
            console.print(f"[yellow]Arquivo {os.path.basename(ndjson_file_path)} está vazio[/]")
            return
        
        temp_dir = os.path.join(
            os.path.dirname(ndjson_file_path) or ".",
            Path(ndjson_file_path).stem
        )
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # Get items that need processing
            all_processed_items = []
            async for item in HashCacheManager.get_items_to_process_async(processed_data):
                all_processed_items.append(item)
            
            if not all_processed_items:
                progress.update(task_id, total=1, completed=1)
                console.print(f"[green]Nenhuma alteração em {os.path.basename(ndjson_file_path)}[/]")
                return
            
            progress.update(task_id, total=len(all_processed_items), completed=0)
            
            # Write JSON files
            processed_count = 0
            for item in all_processed_items:
                processed_count += 1
                progress.update(
                    task_id,
                    description=f"[cyan]Escrevendo {processed_count}/{len(all_processed_items)}: {item.cnpj}.json[/]",
                    completed=processed_count
                )
                
                json_path = os.path.join(temp_dir, f"{item.cnpj}.json")
                with open(json_path, "w", encoding="utf-8") as f:
                    f.write(item.json)
            
            # Upload
            progress.update(
                task_id,
                description=f"[yellow]📤 Preparando upload de {len(all_processed_items)} arquivos...[/]",
                total=100,
                completed=0
            )
            
            success = await RcloneClient.upload_folder_async(
                temp_dir,
                progress_task=(progress, task_id)
            )
            
            if success:
                await HashCacheManager.add_batch_async(all_processed_items)
            else:
                raise RuntimeError("Falha no upload dos arquivos")
            
            console.print(f"[green]✓ {len(all_processed_items)} arquivos enviados com sucesso[/]")
        
        finally:
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
    
    def _extract_cnpj_and_json(self, json_line: str) -> tuple[Optional[str], str]:
        """Extract CNPJ and JSON from a line."""
        import json
        
        try:
            data = json.loads(json_line)
            
            # Check if wrapped in json_output
            if "json_output" in data:
                data_element = data["json_output"]
                raw_json = json.dumps(data_element, ensure_ascii=False, separators=(",", ":"))
            else:
                data_element = data
                raw_json = json_line
            
            cnpj = data_element.get("cnpj")
            if not cnpj:
                return None, json_line
            
            clean_json = JsonCleanupUtils.clean_json_spaces(raw_json)
            return cnpj, clean_json
        
        except Exception:
            return None, json_line
