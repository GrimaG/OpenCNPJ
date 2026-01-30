import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile, ZIP_DEFLATED

import duckdb
from rich.console import Console
from rich.progress import Progress, track

from ..config import get_config
from ..exporters.rclone_client import RcloneClient
from ..utils.cnpj_utils import CnpjUtils
from ..utils.hash_cache_manager import HashCacheManager
from ..utils.json_cleanup_utils import JsonCleanupUtils
from .ndjson_processor import NdjsonProcessor

console = Console()


class ParquetIngestor:
    """Process CSV data to Parquet and export to NDJSON/ZIP."""
    
    def __init__(self):
        config = get_config()
        self._data_dir = config.paths.data_dir
        self._parquet_dir = config.paths.parquet_dir
        Path(self._parquet_dir).mkdir(parents=True, exist_ok=True)
        
        data_source = ":memory:" if config.duckdb.use_in_memory else "./cnpj.duckdb"
        self._connection = duckdb.connect(data_source)
        
        self._ndjson_processor = NdjsonProcessor()
        self._connection_lock = asyncio.Lock()
        self._initialized_hash_cache = False
        
        self._configure_duckdb()
        console.print("[green]ParquetIngestor inicializado com DuckDB (otimizado)[/]")
    
    async def _ensure_hash_cache_initialized(self):
        """Ensure hash cache is initialized."""
        if not self._initialized_hash_cache:
            config = get_config()
            await HashCacheManager.initialize_database(config.paths.hash_cache_dir)
            self._initialized_hash_cache = True
    
    def _configure_duckdb(self) -> None:
        """Configure DuckDB for optimal performance."""
        config = get_config()
        
        try:
            self._connection.execute(f"""
                PRAGMA threads = {max(config.duckdb.threads_pragma, os.cpu_count() or 2)};
                SET memory_limit = '{config.duckdb.memory_limit}';
                SET threads = {max(config.duckdb.engine_threads, os.cpu_count() or 2)};
                PRAGMA temp_directory='./temp';
                PRAGMA enable_progress_bar=false;
                PRAGMA force_index_join=true;
                PRAGMA enable_object_cache=true;
                SET parallel_csv_read=true;
                SET preserve_insertion_order={'true' if config.duckdb.preserve_insertion_order else 'false'};
            """)
            console.print("[green]✓ Configurações de performance aplicadas[/]")
        except Exception as ex:
            console.print(f"[yellow]Aviso ao configurar performance: {ex}[/]")
    
    async def convert_csvs_to_parquet(self) -> None:
        """Convert CSV files to Parquet format."""
        table_configs = {
            "empresa": ("*EMPRECSV*", [
                "cnpj_basico", "razao_social", "natureza_juridica",
                "qualificacao_responsavel", "capital_social", "porte_empresa", "ente_federativo"
            ]),
            "estabelecimento": ("*ESTABELE*", [
                "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
                "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
                "motivo_situacao_cadastral", "nome_cidade_exterior", "codigo_pais",
                "data_inicio_atividade", "cnae_principal", "cnaes_secundarios",
                "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
                "cep", "uf", "codigo_municipio", "ddd1", "telefone1", "ddd2",
                "telefone2", "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
                "data_situacao_especial"
            ]),
            "socio": ("*SOCIOCSV*", [
                "cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio",
                "qualificacao_socio", "data_entrada_sociedade", "codigo_pais",
                "representante_legal", "nome_representante", "qualificacao_representante",
                "faixa_etaria"
            ]),
            "simples": ("*SIMPLES*", [
                "cnpj_basico", "opcao_simples", "data_opcao_simples",
                "data_exclusao_simples", "opcao_mei", "data_opcao_mei",
                "data_exclusao_mei"
            ]),
            "cnae": ("*CNAECSV*", ["codigo", "descricao"]),
            "motivo": ("*MOTICSV*", ["codigo", "descricao"]),
            "municipio": ("*MUNICCSV*", ["codigo", "descricao"]),
            "natureza": ("*NATJUCSV*", ["codigo", "descricao"]),
            "pais": ("*PAISCSV*", ["codigo", "descricao"]),
            "qualificacao": ("*QUALSCSV*", ["codigo", "descricao"])
        }
        
        with Progress() as progress:
            for table_name, (pattern, columns) in table_configs.items():
                task = progress.add_task(f"[green]Processando {table_name}[/]", total=None)
                
                # Find matching files
                files = list(Path(self._data_dir).rglob(pattern))
                
                if not files:
                    console.print(f"[yellow]Nenhum arquivo encontrado para {table_name} ({pattern})[/]")
                    progress.update(task, total=1, completed=1)
                    continue
                
                # Check if Parquet already exists
                parquet_path = os.path.join(self._parquet_dir, f"{table_name}.parquet")
                partitioned_dir = os.path.join(self._parquet_dir, table_name)
                
                is_partitioned = table_name in ["estabelecimento", "empresa", "simples", "socio"]
                parquet_exists = (
                    (is_partitioned and os.path.exists(partitioned_dir) and 
                     list(Path(partitioned_dir).rglob("*.parquet")))
                    or (not is_partitioned and os.path.exists(parquet_path))
                )
                
                if parquet_exists:
                    progress.update(
                        task,
                        description=f"[blue]Pulando {table_name}: Parquet já existe[/]",
                        total=1,
                        completed=1
                    )
                    continue
                
                progress.update(task, total=len(files))
                await self._convert_table_to_parquet(table_name, files, columns, progress, task)
    
    async def _convert_table_to_parquet(
        self,
        table_name: str,
        csv_files: List[Path],
        columns: List[str],
        progress: Progress,
        task_id: int
    ) -> None:
        """Convert a table's CSV files to Parquet."""
        config = get_config()
        parquet_path = os.path.join(self._parquet_dir, f"{table_name}.parquet")
        partitioned_dir = os.path.join(self._parquet_dir, table_name)
        
        is_partitioned = table_name in ["estabelecimento", "empresa", "simples", "socio"]
        
        temp_table_name = f"temp_{table_name}_{os.urandom(8).hex()}"
        
        try:
            # Create temporary table
            columns_def = ", ".join([f"{col} VARCHAR" for col in columns])
            create_sql = f"CREATE TEMPORARY TABLE {temp_table_name} ({columns_def})"
            self._connection.execute(create_sql)
            
            # Insert data in batches
            batch_size = config.ndjson.batch_upload_size
            
            for i in range(0, len(csv_files), batch_size):
                batch = csv_files[i:i + batch_size]
                
                for csv_file in batch:
                    try:
                        columns_dict = {col: "VARCHAR" for col in columns}
                        columns_str = ", ".join([f"'{col}': 'VARCHAR'" for col in columns])
                        
                        insert_sql = f"""
                            INSERT INTO {temp_table_name}
                            SELECT * FROM read_csv('{csv_file}',
                                sep=';',
                                header=false,
                                encoding='CP1252',
                                ignore_errors=true,
                                quote='"',
                                escape='"',
                                max_line_size=10000000,
                                columns={{{columns_str}}})
                        """
                        self._connection.execute(insert_sql)
                    except Exception as ex:
                        console.print(f"[red]Erro processando {csv_file}: {ex}[/]")
                    
                    progress.advance(task_id)
            
            # Export to Parquet
            if is_partitioned:
                os.makedirs(partitioned_dir, exist_ok=True)
                
                export_sql = f"""
                    COPY (
                        SELECT *,
                               SUBSTRING(cnpj_basico, 1, 2) as cnpj_prefix
                        FROM {temp_table_name}
                    )
                    TO '{partitioned_dir}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (cnpj_prefix), OVERWRITE)
                """
                self._connection.execute(export_sql)
                console.print(f"[green]✓ {table_name} particionado por CNPJ prefix criado[/]")
            else:
                export_sql = f"""
                    COPY (SELECT * FROM {temp_table_name})
                    TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE)
                """
                self._connection.execute(export_sql)
                console.print(f"[green]✓ {table_name}.parquet criado[/]")
        
        finally:
            try:
                self._connection.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            except Exception:
                pass
    
    async def export_and_upload_to_storage(self, output_dir: str = "cnpj_ndjson") -> None:
        """Export to NDJSON and upload to storage."""
        await self._ensure_hash_cache_initialized()
        
        os.makedirs(output_dir, exist_ok=True)
        
        console.print("[cyan]Carregando tabelas Parquet para memória...[/]")
        await self._load_parquet_tables()
        
        console.print("[cyan]🚀 Iniciando export + upload integrado para Storage...[/]")
        await self._export_to_ndjson_partitioned(output_dir)
        
        console.print("[green]🎉 Export + upload integrado concluído![/]")
    
    async def _load_parquet_tables(self) -> None:
        """Load Parquet tables as views in DuckDB."""
        table_patterns = {
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
        
        for table_name, pattern in table_patterns.items():
            try:
                full_path = os.path.join(self._parquet_dir, pattern)
                create_view_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{full_path}')"
                self._connection.execute(create_view_sql)
                console.print(f"[green]✓ Tabela {table_name} carregada[/]")
            except Exception as ex:
                console.print(f"[yellow]Aviso ao carregar {table_name}: {ex}[/]")
    
    async def _export_to_ndjson_partitioned(self, output_dir: str) -> None:
        """Export to NDJSON files partitioned by prefix."""
        config = get_config()
        prefixes = [f"{i:02d}" for i in range(100)]
        
        with Progress() as progress:
            main_task = progress.add_task("[green]Progresso geral[/]", total=len(prefixes))
            task_dict = {}
            
            max_parallel = (
                config.ndjson.max_parallel_processing
                if config.ndjson.max_parallel_processing > 0
                else os.cpu_count() or 8
            )
            sem = asyncio.Semaphore(max_parallel)
            
            async def process_prefix(prefix: str):
                async with sem:
                    prefix_task = progress.add_task(f"[yellow]Prefixo {prefix}[/]", total=None)
                    task_dict[prefix] = prefix_task
                    
                    try:
                        # Export to NDJSON
                        progress.update(prefix_task, description=f"[cyan]Gerando {prefix}.ndjson...[/]")
                        await self._export_single_prefix(prefix, output_dir)
                        
                        ndjson_file = os.path.join(output_dir, f"{prefix}.ndjson")
                        if os.path.exists(ndjson_file):
                            progress.update(prefix_task, description=f"[blue]Processando {prefix}.ndjson...[/]")
                            await self._ndjson_processor.process_ndjson_file_to_storage(
                                ndjson_file,
                                progress,
                                prefix_task
                            )
                            
                            os.remove(ndjson_file)
                            progress.update(prefix_task, description=f"[green]✓ {prefix}.ndjson concluído[/]")
                        
                        progress.advance(main_task)
                    
                    except Exception as ex:
                        progress.update(
                            prefix_task,
                            description=f"[red]❌ Erro em {prefix}: {ex}[/]"
                        )
                        console.print_exception()
                        progress.advance(main_task)
            
            await asyncio.gather(*[process_prefix(prefix) for prefix in prefixes])
        
        await HashCacheManager.upload_database_async()
    
    async def _export_single_prefix(self, prefix: str, output_dir: str) -> None:
        """Export a single prefix to NDJSON file."""
        try:
            output_file = os.path.join(output_dir, f"{prefix}.ndjson")
            
            export_query = self._build_json_query_for_prefix(
                prefix,
                include_cnpj_column=False,
                json_alias="json_output"
            )
            
            copy_query = f"COPY ({export_query}) TO '{output_file}'"
            
            # Use lock to serialize DuckDB access
            async with self._connection_lock:
                self._connection.execute(copy_query)
            
            if os.path.exists(output_file):
                file_size_mb = os.path.getsize(output_file) / 1024 / 1024
                console.print(f"[green]✓ {prefix}.ndjson criado ({file_size_mb:.1f} MB)[/]")
        
        except Exception as ex:
            console.print(f"[red]Erro exportando prefixo {prefix}: {ex}[/]")
    
    async def export_single_cnpj_async(self, cnpj: str, output_dir: str) -> None:
        """Export a single CNPJ to JSON file."""
        os.makedirs(output_dir, exist_ok=True)
        
        console.print("[cyan]Carregando tabelas Parquet para memória...[/]")
        await self._load_parquet_tables()
        
        cnpj_basico, cnpj_ordem, cnpj_dv = CnpjUtils.parse_cnpj(cnpj)
        prefix = cnpj_basico[:2]
        
        console.print(f"[yellow]🎯 Buscando CNPJ {cnpj} (prefixo {prefix})...[/]")
        
        try:
            output_file = os.path.join(output_dir, f"{cnpj}.json")
            export_query = self._build_json_query_for_cnpj(
                cnpj_basico,
                cnpj_ordem,
                cnpj_dv,
                json_alias="json_output"
            )
            
            result = self._connection.execute(export_query).fetchone()
            
            if result and result[0]:
                json_content = JsonCleanupUtils.clean_json_spaces(str(result[0]))
                
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(json_content)
                
                file_size = os.path.getsize(output_file)
                console.print(f"[green]✓ {cnpj}.json criado ({file_size} bytes)[/]")
            else:
                console.print(f"[red]❌ CNPJ {cnpj} não encontrado na base de dados[/]")
        
        except Exception as ex:
            console.print(f"[red]Erro exportando CNPJ {cnpj}: {ex}[/]")
    
    async def export_jsons_to_zip(self, output_dir: str) -> str:
        """Export all JSONs directly to ZIP file."""
        await self._load_parquet_tables()
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = os.path.join(output_dir, f"cnpj_jsons_{timestamp}.zip")
        
        with ZipFile(zip_path, "w", ZIP_DEFLATED) as archive:
            console.print("[cyan]Exportando JSONs para ZIP...[/]")
            
            prefixes = [f"{i:02d}" for i in range(100)]
            
            for prefix in track(prefixes, description="Exportando prefixos..."):
                await self._export_prefix_to_zip_directly(prefix, archive)
        
        zip_size_gb = os.path.getsize(zip_path) / 1024 / 1024 / 1024
        console.print(f"[green]✓ ZIP criado: {zip_path} ({zip_size_gb:.2f} GB)[/]")
        return zip_path
    
    async def generate_and_upload_final_info_json_async(self, zip_path: str) -> None:
        """Generate and upload info.json with statistics."""
        try:
            await self._load_parquet_tables()
            
            result = self._connection.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()
            total = result[0] if result else 0
            
            last_updated = datetime.utcnow().isoformat()
            
            zip_size = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
            
            zip_md5_base64 = ""
            if os.path.exists(zip_path):
                with open(zip_path, "rb") as f:
                    file_hash = hashlib.md5()
                    for chunk in iter(lambda: f.read(65536), b""):
                        file_hash.update(chunk)
                    import base64
                    zip_md5_base64 = base64.b64encode(file_hash.digest()).decode("ascii")
            
            payload = {
                "total": total,
                "last_updated": last_updated,
                "zip_size": zip_size,
                "zip_url": "https://file.opencnpj.org/cnpjs.zip",
                "zip_md5checksum": zip_md5_base64
            }
            
            json_str = json.dumps(payload, ensure_ascii=False)
            
            temp_dir = os.path.join("/tmp", "opencnpj_info")
            os.makedirs(temp_dir, exist_ok=True)
            local_info_path = os.path.join(temp_dir, "info.json")
            
            with open(local_info_path, "w", encoding="utf-8") as f:
                f.write(json_str)
            
            console.print("[cyan]📤 Enviando info.json para Storage...[/]")
            ok = await RcloneClient.upload_file_async(local_info_path, "info.json")
            
            if ok:
                console.print("[green]✓ info.json enviado para Storage[/]")
            else:
                console.print("[red]❌ Falha ao enviar info.json para Storage[/]")
        
        except Exception as ex:
            console.print(f"[red]Erro ao gerar/enviar info.json: {ex}[/]")
    
    async def _export_prefix_to_zip_directly(self, prefix: str, archive: ZipFile) -> None:
        """Export a prefix directly to ZIP archive."""
        query = self._build_json_query_for_prefix(
            prefix,
            include_cnpj_column=True,
            json_alias="json_data"
        )
        
        cursor = self._connection.execute(query)
        
        for row in cursor.fetchall():
            cnpj = row[0]
            json_data = row[1]
            
            archive.writestr(f"{cnpj}.json", json_data)
    
    @staticmethod
    def _get_json_struct_fields() -> str:
        """Get the JSON struct fields for DuckDB query."""
        return """cnpj := e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv,
                    razao_social := COALESCE(emp.razao_social, ''),
                    nome_fantasia := COALESCE(e.nome_fantasia, ''),
                    situacao_cadastral := CASE LPAD(e.situacao_cadastral, 2, '0')
                        WHEN '01' THEN 'Nula'
                        WHEN '02' THEN 'Ativa'
                        WHEN '03' THEN 'Suspensa'
                        WHEN '04' THEN 'Inapta'
                        WHEN '08' THEN 'Baixada'
                        ELSE e.situacao_cadastral
                    END,
                    data_situacao_cadastral := CASE 
                        WHEN e.data_situacao_cadastral ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(e.data_situacao_cadastral, 1, 4) || '-' || 
                             SUBSTRING(e.data_situacao_cadastral, 5, 2) || '-' || 
                             SUBSTRING(e.data_situacao_cadastral, 7, 2)
                        ELSE COALESCE(e.data_situacao_cadastral, '')
                    END,
                    matriz_filial := CASE e.identificador_matriz_filial
                        WHEN '1' THEN 'Matriz'
                        WHEN '2' THEN 'Filial'
                        ELSE e.identificador_matriz_filial
                    END,
                    data_inicio_atividade := CASE 
                        WHEN e.data_inicio_atividade ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(e.data_inicio_atividade, 1, 4) || '-' || 
                             SUBSTRING(e.data_inicio_atividade, 5, 2) || '-' || 
                             SUBSTRING(e.data_inicio_atividade, 7, 2)
                        ELSE COALESCE(e.data_inicio_atividade, '')
                    END,
                    cnae_principal := COALESCE(e.cnae_principal, ''),
                    cnaes_secundarios := CASE 
                        WHEN e.cnaes_secundarios IS NOT NULL AND e.cnaes_secundarios != ''
                        THEN string_split(e.cnaes_secundarios, ',')
                        ELSE []
                    END,
                    natureza_juridica := COALESCE(nat.descricao, ''),
                    tipo_logradouro := COALESCE(e.tipo_logradouro, ''),
                    logradouro := COALESCE(e.logradouro, ''),
                    numero := COALESCE(e.numero, ''),
                    complemento := COALESCE(e.complemento, ''),
                    bairro := COALESCE(e.bairro, ''),
                    cep := COALESCE(e.cep, ''),
                    uf := COALESCE(e.uf, ''),
                    municipio := COALESCE(mun.descricao, ''),
                    email := COALESCE(e.correio_eletronico, ''),
                    telefones := list_filter([
                        CASE WHEN e.ddd1 IS NOT NULL OR e.telefone1 IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd1, ''), numero := COALESCE(e.telefone1, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd2 IS NOT NULL OR e.telefone2 IS NOT NULL  
                             THEN struct_pack(ddd := COALESCE(e.ddd2, ''), numero := COALESCE(e.telefone2, ''), is_fax := false)
                             ELSE NULL
                        END,
                        CASE WHEN e.ddd_fax IS NOT NULL OR e.fax IS NOT NULL
                             THEN struct_pack(ddd := COALESCE(e.ddd_fax, ''), numero := COALESCE(e.fax, ''), is_fax := true)
                             ELSE NULL
                        END
                    ], x -> x IS NOT NULL),
                    capital_social := COALESCE(emp.capital_social, ''),
                    porte_empresa := CASE emp.porte_empresa
                        WHEN '00' THEN 'Não informado'
                        WHEN '01' THEN 'Microempresa (ME)'
                        WHEN '03' THEN 'Empresa de Pequeno Porte (EPP)'
                        WHEN '05' THEN 'Demais'
                        ELSE COALESCE(emp.porte_empresa, '')
                    END,
                    opcao_simples := COALESCE(s.opcao_simples, ''),
                    data_opcao_simples := CASE 
                        WHEN s.data_opcao_simples ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(s.data_opcao_simples, 1, 4) || '-' || 
                             SUBSTRING(s.data_opcao_simples, 5, 2) || '-' || 
                             SUBSTRING(s.data_opcao_simples, 7, 2)
                        ELSE COALESCE(s.data_opcao_simples, '')
                    END,
                    opcao_mei := COALESCE(s.opcao_mei, ''),
                    data_opcao_mei := CASE 
                        WHEN s.data_opcao_mei ~ '^[0-9]{8}$' 
                        THEN SUBSTRING(s.data_opcao_mei, 1, 4) || '-' || 
                             SUBSTRING(s.data_opcao_mei, 5, 2) || '-' || 
                             SUBSTRING(s.data_opcao_mei, 7, 2)
                        ELSE COALESCE(s.data_opcao_mei, '')
                    END,
                    QSA := COALESCE(sd.qsa_data, [])"""
    
    def _build_json_query_for_prefix(
        self,
        prefix: str,
        include_cnpj_column: bool,
        json_alias: str
    ) -> str:
        """Build JSON query for a CNPJ prefix."""
        json_fields = self._get_json_struct_fields()
        
        if include_cnpj_column:
            select_cols = f"e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, to_json(struct_pack(\n{json_fields}\n)) as {json_alias}"
        else:
            select_cols = f"to_json(struct_pack(\n{json_fields}\n)) as {json_alias}"
        
        return f"""WITH socios_data AS (
                SELECT 
                    s.cnpj_basico,
                    array_agg(struct_pack(
                        nome_socio := COALESCE(s.nome_socio, ''),
                        cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                        qualificacao_socio := COALESCE(qs.descricao, ''),
                        data_entrada_sociedade := CASE 
                            WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$' 
                            THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 7, 2)
                            ELSE COALESCE(s.data_entrada_sociedade, '')
                        END,
                        identificador_socio := CASE s.identificador_socio
                            WHEN '1' THEN 'Pessoa Jurídica'
                            WHEN '2' THEN 'Pessoa Física'
                            WHEN '3' THEN 'Estrangeiro'
                            ELSE COALESCE(s.identificador_socio, '')
                        END,
                        faixa_etaria := CASE s.faixa_etaria
                            WHEN '0' THEN 'Não se aplica'
                            WHEN '1' THEN '0 a 12 anos'
                            WHEN '2' THEN '13 a 20 anos'
                            WHEN '3' THEN '21 a 30 anos'
                            WHEN '4' THEN '31 a 40 anos'
                            WHEN '5' THEN '41 a 50 anos'
                            WHEN '6' THEN '51 a 60 anos'
                            WHEN '7' THEN '61 a 70 anos'
                            WHEN '8' THEN '71 a 80 anos'
                            WHEN '9' THEN 'Mais de 80 anos'
                            ELSE COALESCE(s.faixa_etaria, '')
                        END
                    )) as qsa_data
                FROM socio s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                WHERE s.cnpj_prefix = '{prefix}'
                GROUP BY s.cnpj_basico
            )
            SELECT {select_cols}
            FROM estabelecimento e
            LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_prefix = '{prefix}'"""
    
    def _build_json_query_for_cnpj(
        self,
        cnpj_basico: str,
        cnpj_ordem: str,
        cnpj_dv: str,
        json_alias: str
    ) -> str:
        """Build JSON query for a specific CNPJ."""
        json_fields = self._get_json_struct_fields()
        select_cols = f"to_json(struct_pack(\n{json_fields}\n)) as {json_alias}"
        
        return f"""WITH socios_data AS (
                SELECT 
                    s.cnpj_basico,
                    array_agg(struct_pack(
                        nome_socio := COALESCE(s.nome_socio, ''),
                        cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                        qualificacao_socio := COALESCE(qs.descricao, ''),
                        data_entrada_sociedade := CASE 
                            WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$' 
                            THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' || 
                                 SUBSTRING(s.data_entrada_sociedade, 7, 2)
                            ELSE COALESCE(s.data_entrada_sociedade, '')
                        END,
                        identificador_socio := CASE s.identificador_socio
                            WHEN '1' THEN 'Pessoa Jurídica'
                            WHEN '2' THEN 'Pessoa Física'
                            WHEN '3' THEN 'Estrangeiro'
                            ELSE COALESCE(s.identificador_socio, '')
                        END,
                        faixa_etaria := CASE s.faixa_etaria
                            WHEN '0' THEN 'Não se aplica'
                            WHEN '1' THEN '0 a 12 anos'
                            WHEN '2' THEN '13 a 20 anos'
                            WHEN '3' THEN '21 a 30 anos'
                            WHEN '4' THEN '31 a 40 anos'
                            WHEN '5' THEN '41 a 50 anos'
                            WHEN '6' THEN '51 a 60 anos'
                            WHEN '7' THEN '61 a 70 anos'
                            WHEN '8' THEN '71 a 80 anos'
                            WHEN '9' THEN 'Mais de 80 anos'
                            ELSE COALESCE(s.faixa_etaria, '')
                        END
                    )) as qsa_data
                FROM socio s
                LEFT JOIN qualificacao qs ON s.qualificacao_socio = qs.codigo
                WHERE s.cnpj_basico = '{cnpj_basico}'
                GROUP BY s.cnpj_basico
            )
            SELECT {select_cols}
            FROM estabelecimento e
            LEFT JOIN empresa emp ON e.cnpj_basico = emp.cnpj_basico
            LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN natureza nat ON emp.natureza_juridica = nat.codigo
            LEFT JOIN municipio mun ON e.codigo_municipio = mun.codigo
            LEFT JOIN socios_data sd ON e.cnpj_basico = sd.cnpj_basico
            WHERE e.cnpj_basico = '{cnpj_basico}' 
              AND e.cnpj_ordem = '{cnpj_ordem}' 
              AND e.cnpj_dv = '{cnpj_dv}'"""
    
    def dispose(self) -> None:
        """Clean up resources."""
        if self._connection:
            self._connection.close()
        HashCacheManager.close_connections()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
