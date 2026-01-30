import asyncio
import os
import re
from typing import Optional

from rich.console import Console
from rich.progress import Progress, TaskID

console = Console()


class RcloneClient:
    """Client for rclone operations."""
    
    _upload_semaphore: Optional[asyncio.Semaphore] = None
    _transfer_regex = re.compile(r"Transferred:\s+\d+\s*/\s*\d+,\s*(\d+)%", re.IGNORECASE)
    
    @classmethod
    def _get_remote_base(cls) -> str:
        """Get the rclone remote base path."""
        from ..config import get_config
        config = get_config()
        remote = os.environ.get("RCLONE_REMOTE", config.rclone.remote_base)
        return remote.rstrip("/")
    
    @classmethod
    def _get_transfers(cls) -> int:
        """Get the number of transfers."""
        from ..config import get_config
        config = get_config()
        return max(1, config.rclone.transfers)
    
    @classmethod
    def _get_upload_semaphore(cls) -> asyncio.Semaphore:
        """Get or create the upload semaphore."""
        if cls._upload_semaphore is None:
            from ..config import get_config
            config = get_config()
            cls._upload_semaphore = asyncio.Semaphore(config.rclone.max_concurrent_uploads)
        return cls._upload_semaphore
    
    @classmethod
    async def upload_folder_async(
        cls,
        local_folder_path: str,
        progress_task: Optional[tuple[Progress, TaskID]] = None
    ) -> bool:
        """Upload a folder to rclone remote."""
        sem = cls._get_upload_semaphore()
        async with sem:
            try:
                remote = cls._get_remote_base() + "/"
                transfers = cls._get_transfers()
                
                args = [
                    "rclone", "copy", local_folder_path, remote,
                    "--progress", "--stats=1s", f"--transfers={transfers}",
                    "--no-traverse", "--no-check-dest", "--fast-list=false",
                    "--ignore-times", "--ignore-size", "--ignore-checksum",
                    "--no-update-modtime",
                    "--buffer-size=128M", "--checkers=1",
                    "--bwlimit=off",
                    "--retries=-1", "--retries-sleep=60s", "--low-level-retries=10"
                ]
                
                process = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                error_buffer = []
                
                async def read_output():
                    """Read stdout for progress updates."""
                    if process.stdout:
                        async for line in process.stdout:
                            try:
                                line_str = line.decode("utf-8", errors="ignore")
                                match = cls._transfer_regex.search(line_str)
                                if match and progress_task:
                                    progress, task_id = progress_task
                                    percentage = int(match.group(1))
                                    progress.update(
                                        task_id,
                                        completed=percentage,
                                        description=f"[cyan]Upload: {percentage}%[/]"
                                    )
                            except Exception:
                                pass
                
                async def read_errors():
                    """Read stderr for errors."""
                    if process.stderr:
                        async for line in process.stderr:
                            try:
                                line_str = line.decode("utf-8", errors="ignore")
                                error_buffer.append(line_str)
                                if "ERROR" in line_str.upper():
                                    console.print(f"[red]rclone: {line_str.strip()}[/]")
                            except Exception:
                                pass
                
                await asyncio.gather(read_output(), read_errors())
                await process.wait()
                
                ok = process.returncode == 0
                if not ok and error_buffer:
                    console.print(f"[red]Erro no rclone upload: {''.join(error_buffer)}[/]")
                
                return ok
            
            except Exception as ex:
                console.print(f"[red]Erro no rclone upload: {ex}[/]")
                return False
    
    @classmethod
    async def download_file_async(cls, remote_relative_path: str, local_file_path: str) -> bool:
        """Download a file from rclone remote."""
        remote = cls._get_remote_base() + "/" + remote_relative_path.lstrip("/")
        return await cls.copy_to_async(remote, local_file_path)
    
    @classmethod
    async def copy_to_async(cls, remote_path: str, local_file_path: str) -> bool:
        """Copy a file from remote to local using rclone copyto."""
        try:
            args = [
                "rclone", "copyto", remote_path, local_file_path,
                "--retries=-1", "--retries-sleep=60s", "--low-level-retries=10",
                "--bwlimit=off"
            ]
            
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            ok = process.returncode == 0 and os.path.exists(local_file_path)
            if not ok and stderr:
                console.print(f"[yellow]⚠️ rclone copyto falhou: {stderr.decode('utf-8', errors='ignore')}[/]")
            
            return ok
        
        except Exception as ex:
            console.print(f"[yellow]⚠️ Erro no rclone copyto: {ex}[/]")
            return False
    
    @classmethod
    async def upload_file_async(cls, local_file_path: str, remote_relative_path: str) -> bool:
        """Upload a single file to rclone remote."""
        try:
            remote_path = cls._get_remote_base() + "/" + remote_relative_path.lstrip("/")
            
            args = [
                "rclone", "copyto", local_file_path, remote_path,
                "--retries=-1", "--retries-sleep=60s", "--low-level-retries=10",
                "--bwlimit=off", "--no-update-modtime"
            ]
            
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            _, stderr = await process.communicate()
            
            ok = process.returncode == 0
            if not ok and stderr:
                console.print(f"[yellow]⚠️ rclone upload file falhou: {stderr.decode('utf-8', errors='ignore')}[/]")
            
            return ok
        
        except Exception as ex:
            console.print(f"[yellow]⚠️ Erro no rclone upload file: {ex}[/]")
            return False
