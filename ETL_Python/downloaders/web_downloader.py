import asyncio
import os
import re
from pathlib import Path
from typing import List
from zipfile import ZipFile

import aiohttp
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TimeRemainingColumn,
    TaskID,
)

console = Console()


class WebDownloader:
    """Download and extract CNPJ data files from Receita Federal."""
    
    BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    
    def __init__(self, download_dir: str, extract_dir: str):
        self._download_dir = download_dir
        self._extract_dir = extract_dir
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        Path(extract_dir).mkdir(parents=True, exist_ok=True)
    
    async def download_and_extract_async(self, year_month: str) -> None:
        """Download and extract files for a given year-month."""
        page_url = f"{self.BASE_URL}{year_month.strip('/')}/"
        console.print(f"[blue]Acessando:[/] [white]{page_url}[/]")
        
        zip_urls = await self._list_zip_urls_async(page_url)
        if not zip_urls:
            console.print("[yellow]Nenhum arquivo ZIP encontrado nessa página.[/]")
            return
        
        local_zips = await self._download_all_async(zip_urls)
        await self._extract_all_async(local_zips, self._extract_dir)
    
    async def _list_zip_urls_async(self, page_url: str) -> List[str]:
        """List all ZIP URLs from the page."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(page_url, headers={"User-Agent": "OpenCNPJ/1.0"}) as resp:
                    resp.raise_for_status()
                    html = await resp.text()
            
            urls = []
            pattern = re.compile(r'href="([^"]+?\.zip)"', re.IGNORECASE)
            for match in pattern.finditer(html):
                href = match.group(1)
                if not href.strip():
                    continue
                
                if href.lower().startswith("http"):
                    url = href
                else:
                    # Join relative URL
                    url = page_url.rstrip("/") + "/" + href.lstrip("/")
                urls.append(url)
            
            unique_urls = list(dict.fromkeys(urls))  # Remove duplicates preserving order
            console.print(f"[green]Encontrados {len(unique_urls)} ZIP(s).[/]")
            return unique_urls
        
        except aiohttp.ClientError as ex:
            console.print(f"[red]Erro ao obter lista de arquivos: {ex}[/]")
            return []
    
    async def _download_all_async(self, urls: List[str]) -> List[str]:
        """Download all files with progress tracking."""
        from ..config import get_config
        config = get_config()
        
        results = []
        files_info = []
        
        for url in urls:
            filename = os.path.basename(url.split("?")[0])
            filepath = os.path.join(self._download_dir, filename)
            files_info.append({
                "url": url,
                "filename": filename,
                "filepath": filepath
            })
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TimeRemainingColumn(),
            SpinnerColumn(),
        ) as progress:
            tasks = {}
            
            # Mark already downloaded files
            for info in files_info:
                if os.path.exists(info["filepath"]):
                    task_id = progress.add_task(
                        f"[green]✓ {info['filename']} (já existe)[/]",
                        total=1,
                        completed=1
                    )
                    results.append(info["filepath"])
                else:
                    task_id = progress.add_task(f"[cyan]{info['filename']}[/]", total=None)
                    tasks[info["filepath"]] = task_id
            
            # Download files in parallel
            to_download = [info for info in files_info if not os.path.exists(info["filepath"])]
            
            parallel_downloads = max(1, config.downloader.parallel_downloads)
            sem = asyncio.Semaphore(parallel_downloads)
            
            async def download_with_sem(info):
                async with sem:
                    task_id = tasks[info["filepath"]]
                    local_path = await self._download_one_async(
                        info["url"],
                        info["filepath"],
                        progress,
                        task_id
                    )
                    results.append(local_path)
            
            await asyncio.gather(*[download_with_sem(info) for info in to_download])
        
        return results
    
    async def _download_one_async(
        self,
        url: str,
        filepath: str,
        progress: Progress,
        task_id: TaskID,
        max_retries: int = 3
    ) -> str:
        """Download a single file with retry logic."""
        retry = 0
        
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers={"User-Agent": "OpenCNPJ/1.0"}) as resp:
                        resp.raise_for_status()
                        
                        total = resp.content_length or 0
                        if total > 0:
                            progress.update(task_id, total=total)
                        else:
                            progress.update(task_id, total=1_000_000)  # Fallback
                        
                        with open(filepath, "wb") as f:
                            read_total = 0
                            async for chunk in resp.content.iter_chunked(65536):
                                f.write(chunk)
                                read_total += len(chunk)
                                if total > 0:
                                    progress.update(task_id, completed=read_total)
                                else:
                                    progress.update(task_id, advance=len(chunk))
                        
                        progress.update(
                            task_id,
                            description=f"[green]✓ {os.path.basename(filepath)}[/]",
                            completed=total if total > 0 else read_total
                        )
                        return filepath
            
            except Exception:
                retry += 1
                progress.update(
                    task_id,
                    description=f"[red]✗ {os.path.basename(filepath)} (tentativa {retry})[/]"
                )
                if retry >= max_retries:
                    raise
                await asyncio.sleep(1 * retry)
        
        raise RuntimeError("Falha no download após múltiplas tentativas")
    
    async def _extract_all_async(self, zip_files: List[str], target_dir: str) -> None:
        """Extract all ZIP files to the target directory."""
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        
        # Check if files are already extracted
        extracted_patterns = [
            "*EMPRECSV*", "*ESTABELE*", "*SOCIOCSV*", "*SIMPLES*",
            "*CNAECSV*", "*MOTICSV*", "*MUNICCSV*", "*NATJUCSV*",
            "*PAISCSV*", "*QUALSCSV*"
        ]
        
        has_extracted = any(
            list(Path(target_dir).rglob(pattern))
            for pattern in extracted_patterns
        )
        
        if has_extracted:
            console.print("[blue]ℹ️ Arquivos já extraídos encontrados; pulando extração.[/]")
            return
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
        ) as progress:
            for zip_path in zip_files:
                filename = os.path.basename(zip_path)
                task_id = progress.add_task(f"[yellow]Extraindo {filename}[/]", total=1)
                
                try:
                    with ZipFile(zip_path, "r") as zip_ref:
                        zip_ref.extractall(target_dir)
                    progress.update(task_id, completed=1)
                except Exception as ex:
                    progress.update(
                        task_id,
                        description=f"[red]Erro em {filename}: {ex}[/]"
                    )
        
        console.print(f"[green]✓ Extração concluída em {os.path.abspath(target_dir)}[/]")
