<img src="./Page/assets/logo.svg" alt="OpenCNPJ" height="64" />

Projeto aberto para baixar, processar e publicar dados públicos das empresas do Brasil.

## Pastas

- `ETL`: ETL em C# (.NET 9.0) que baixa, processa e publica dados do CNPJ.
- `ETL_Python`: ETL em Python 3.10+ (conversão completa do ETL em C#).
- `Page`: página/SPA estática para consulta dos dados publicados.

## 🐍 Versão Python (Recomendada)

### Requisitos

- `Python 3.10+`
- `rclone` instalado e autenticado no seu storage (ex.: Backblaze, R2, S3, Azure Storage, ...).
- Espaço em disco e boa conexão (a primeira execução pode levar tempo -- dias até).

### Instalação

```bash
cd ETL_Python
pip install -r requirements.txt
```

### Configuração

- Ajuste `ETL_Python/config.json` ou use `ETL/config.json` se desejar mudar pastas locais, destino do storage, memória, paralelismo...
- No `config.json`, aponte para o Storage que deseja passando a configuração do rclone.

### Execução

```bash
cd ETL_Python

# Pipeline completo
python -m ETL_Python.main pipeline
python -m ETL_Python.main pipeline --month 2024-01  # (opcional)

# Outros comandos úteis
python -m ETL_Python.main zip                        # gera um ZIP consolidado local
python -m ETL_Python.main test                       # roda teste simples de integridade
python -m ETL_Python.main single --cnpj 00000000000191  # processa um CNPJ específico
```

## 🔷 Versão C# (.NET)

### Requisitos

- `.NET SDK 9.0+`
- `rclone` instalado e autenticado no seu storage (ex.: Backblaze, R2, S3, Azure Storage, ...).
- Espaço em disco e boa conexão (a primeira execução pode levar tempo -- dias até).

### Configuração

- Ajuste `ETL/config.json` se desejar mudar pastas locais, destino do storage, memória, paralelismo... 
- No `config.json`, aponte para o Storage que deseja passando a configuração do rclone.

### Execução

- Dentro de `ETL`:
  - `dotnet run pipeline`
  - `dotnet run pipeline -m YYYY-MM` (opcional)

Outros comandos úteis (opcionais):

- `dotnet run zip`: gera um ZIP consolidado local.
- `dotnet run test`: roda teste simples de integridade.
- `dotnet run single --cnpj 00000000000191`: processa um CNPJ específico.

## Contribuição

- Abra issues para discutir mudanças.
- Faça fork, crie uma branch descritiva e envie PR.
- Mantenha commits pequenos e o projeto compilando (`dotnet build`).
