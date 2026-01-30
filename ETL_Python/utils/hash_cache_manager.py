import os
import sqlite3
import zipfile
from pathlib import Path
from typing import AsyncIterator, List, Optional, Any
import asyncio

from rich.console import Console

console = Console()


class ProcessedItem:
    """Represents a processed CNPJ item."""
    def __init__(self, cnpj: str, json_data: str, hash_value: str):
        self.cnpj = cnpj
        self.json = json_data
        self.hash = hash_value


class HashCacheManager:
    """Manager for hash cache database."""
    
    _connection: Optional[sqlite3.Connection] = None
    _db_path: Optional[str] = None
    _current_transaction: Optional[Any] = None
    _pending_inserts: int = 0
    _initialized: bool = False
    BATCH_SIZE: int = 10000
    _lock = asyncio.Lock()
    
    @classmethod
    async def initialize_database(cls, hash_cache_dir: str) -> None:
        """Initialize the hash cache database."""
        if cls._initialized:
            return
        
        cls._db_path = os.path.join(hash_cache_dir, "hashes.db")
        os.makedirs(os.path.dirname(cls._db_path), exist_ok=True)
        
        await cls._ensure_database_exists(hash_cache_dir)
        
        cls._connection = sqlite3.connect(cls._db_path)
        cursor = cls._connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hashes (
                cnpj TEXT PRIMARY KEY NOT NULL,
                hash TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cls._connection.commit()
        
        cls._optimize_database()
        cls._initialized = True
    
    @classmethod
    async def _ensure_database_exists(cls, hash_cache_dir: str) -> bool:
        """Ensure database exists, download from storage if needed."""
        if os.path.exists(cls._db_path):
            console.print("[green]✓ Banco de hashes local encontrado[/green]")
            return True
        
        console.print("[yellow]Banco de hashes não encontrado localmente, baixando do Storage...[/yellow]")
        
        temp_dir = os.path.join("/tmp", f"hash_download_{os.urandom(8).hex()}")
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            from ..exporters.rclone_client import RcloneClient
            
            zip_file_name = "hashes.zip"
            temp_zip_path = os.path.join(temp_dir, zip_file_name)
            
            success = await RcloneClient.download_file_async(zip_file_name, temp_zip_path)
            
            if success and os.path.exists(temp_zip_path):
                async with cls._lock:
                    with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(os.path.dirname(cls._db_path))
                    
                    if os.path.exists(cls._db_path):
                        console.print("[green]✓ Banco de hashes baixado e descompactado com sucesso[/green]")
                        return True
        finally:
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
        
        console.print("[yellow]⚠️ Banco de hashes não encontrado no Storage, criando novo...[/yellow]")
        return False
    
    @classmethod
    def _optimize_database(cls) -> None:
        """Optimize database settings."""
        cursor = cls._connection.cursor()
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA cache_size = -84000")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA mmap_size = 30000000000")
        cls._connection.commit()
    
    @classmethod
    async def get_items_to_process_async(cls, items: List[ProcessedItem]) -> AsyncIterator[ProcessedItem]:
        """Get items that need processing (new or updated)."""
        if not cls._initialized or cls._connection is None:
            raise RuntimeError("Database not initialized")
        
        new_count = 0
        update_count = 0
        
        check_batch_size = 500
        
        async with cls._lock:
            cursor = cls._connection.cursor()
            
            for i in range(0, len(items), check_batch_size):
                batch = items[i:i + check_batch_size]
                placeholders = ','.join(['?'] * len(batch))
                cnpjs = [item.cnpj for item in batch]
                
                cursor.execute(
                    f"SELECT cnpj, hash FROM hashes WHERE cnpj IN ({placeholders})",
                    cnpjs
                )
                
                existing_hashes = {row[0]: row[1] for row in cursor.fetchall()}
                
                for item in batch:
                    if item.cnpj not in existing_hashes:
                        new_count += 1
                        yield item
                    elif existing_hashes[item.cnpj] != item.hash:
                        update_count += 1
                        yield item
            
            if new_count > 0 or update_count > 0:
                console.print(f"[cyan]📊 {new_count} novos CNPJs para inserir, {update_count} CNPJs para atualizar[/cyan]")
    
    @classmethod
    async def add_async(cls, cnpj: str, hash_value: str) -> None:
        """Add a hash to the cache."""
        await cls._lock.acquire()
        try:
            if cls._current_transaction is None:
                cls._current_transaction = True
            
            cursor = cls._connection.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO hashes (cnpj, hash) VALUES (?, ?)",
                (cnpj, hash_value)
            )
            
            cls._pending_inserts += 1
            
            if cls._pending_inserts >= cls.BATCH_SIZE:
                await cls._commit_batch_async()
        finally:
            cls._lock.release()
    
    @classmethod
    async def add_batch_async(cls, items: List[ProcessedItem]) -> None:
        """Add a batch of items."""
        async with cls._lock:
            for item in items:
                await cls.add_async(item.cnpj, item.hash)
            await cls._commit_batch_async()
    
    @classmethod
    async def _commit_batch_async(cls) -> None:
        """Commit the current batch."""
        if cls._current_transaction is not None:
            cls._connection.commit()
            cls._current_transaction = None
            cls._pending_inserts = 0
    
    @classmethod
    async def upload_database_async(cls) -> bool:
        """Upload database to storage."""
        from ..config import get_config
        config = get_config()
        hash_cache_dir = config.paths.hash_cache_dir
        
        async with cls._lock:
            await cls._commit_batch_async()
            
            console.print("[cyan]📤 Fazendo upload do banco de hashes...[/cyan]")
            
            temp_dir = os.path.join("/tmp", f"hash_upload_{os.urandom(8).hex()}")
            os.makedirs(temp_dir, exist_ok=True)
            
            try:
                zip_file_name = "hashes.zip"
                zip_path = os.path.join(temp_dir, zip_file_name)
                temp_db_copy_path = os.path.join(temp_dir, os.path.basename(cls._db_path))
                
                cls.close_connections()
                
                console.print("[cyan]🗃️ Compactando banco de dados...[/cyan]")
                
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                
                import shutil
                shutil.copy2(cls._db_path, temp_db_copy_path)
                
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                    zip_ref.write(temp_db_copy_path, os.path.basename(cls._db_path))
                
                zip_size_mb = os.path.getsize(zip_path) / 1024 / 1024
                console.print(f"[cyan]📦 Banco compactado: {zip_size_mb:.1f} MB[/cyan]")
                
                from ..exporters.rclone_client import RcloneClient
                success = await RcloneClient.upload_file_async(zip_path, zip_file_name)
                
                if success:
                    console.print("[green]✓ Banco de hashes enviado para Storage[/green]")
                else:
                    console.print("[yellow]⚠️ Falha ao enviar banco de hashes[/yellow]")
                
                return success
            finally:
                if os.path.exists(temp_dir):
                    import shutil
                    shutil.rmtree(temp_dir, ignore_errors=True)
    
    @classmethod
    def close_connections(cls) -> None:
        """Close database connections."""
        if cls._connection:
            cls._connection.close()
            cls._connection = None
        cls._initialized = False
