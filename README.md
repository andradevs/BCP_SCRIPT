# BCP Script Toolkit

Scripts para exportar consultas SQL via `bcp`, compactar em gzip e enviar para o S3, alem de baixar/importar arquivos `.bcp` diretamente no banco.

## Requisitos

- Python 3.10+
- Ferramentas CLI `bcp` e `sqlcmd` acessiveis no `PATH` (ou configure via `.env`).
- Credenciais AWS com permissao de leitura/escrita no bucket definido (apenas para o fluxo S3).

## Instalacao

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuracao

Copie o arquivo de exemplo e preencha as variaveis obrigatorias:

```bash
cp .env.example .env
```

Principais variaveis:

- Banco: `DB_SERVER`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`.
- Ferramentas: `BCP_PATH` (bcp), `SQLCMD_PATH` (sqlcmd), `FIELD_TERMINATOR` (padrao `;`), `BCP_KEEP_IDENTITY` (padrao `true`), `BCP_MAX_ERRORS` (padrao `1` para interromper no primeiro erro) e `BCP_ERROR_FILE` (arquivo de erros do bcp; se vazio cria um `.err` em `LOG_DIR`).
- Diretorios: `SCRIPTS_DIR`, `OUTPUT_DIR`, `DOWNLOAD_DIR`, `LOG_DIR`, `LOCAL_IMPORT_DIR` (padrao `./Subida`).
- S3: `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_BUCKET`, `S3_KEY` (prefixo para exportar/importar) e opcional `S3_OBJECT_KEY` para importar um arquivo especifico.
- Importacao local: opcional `LOCAL_IMPORT_FILE` para apontar um arquivo especifico dentro de `LOCAL_IMPORT_DIR`.

Veja o `.env.example` para a lista completa.

## Uso

### Exportar queries para BCP e enviar ao S3

1. Coloque os arquivos `.sql` no diretorio configurado em `SCRIPTS_DIR` (padrao `./scripts`).
2. Execute o script de exportacao:

```bash
python main.py                     # executa todos os .sql do diretorio
python main.py --scripts a.sql b.sql  # executa apenas os arquivos indicados
```

O script gera arquivos `.bcp`, compacta em `.gz` e envia para `s3://$S3_BUCKET/$S3_KEY/<arquivo>.bcp.gz`. Logs ficam em `LOG_DIR`.

### Importar do S3 para o banco

1. Confirme as variaveis de conexao (`DB_*`) e de S3 (`S3_*`).
2. Opcional: defina `S3_OBJECT_KEY` para usar um arquivo especifico; caso contrario, o script buscara o `.bcp/.bcp.gz` mais recente dentro de `S3_KEY`.
3. Execute:

```bash
python import_from_s3.py
```

Durante a execucao sera exibida uma confirmacao antes de `TRUNCATE TABLE` na tabela inferida a partir do nome do arquivo. O arquivo e baixado para `DOWNLOAD_DIR`, descompactado se necessario e importado via `bcp`.

### Importar arquivo local (pasta Subida)

1. Coloque o arquivo `.bcp` (ou `.bcp.gz`) na pasta definida em `LOCAL_IMPORT_DIR` (padrao `./Subida`). O nome do arquivo deve comecar com o nome da tabela, por exemplo `dbo.SUA_TABELA_20240101_120000.bcp`.
2. Se houver mais de um arquivo na pasta, defina `LOCAL_IMPORT_FILE` para escolher um especifico; caso contrario o arquivo mais recente sera usado.
3. Execute:

```bash
python import_local.py
```

O script confirma o `TRUNCATE TABLE` antes de importar.

## Dicas

- Utilize nomes de arquivo `.bcp` no formato `tabela_YYYYMMDD_HHMMSS.bcp.gz` para que a importacao automatica identifique a tabela correta.
- Verifique os arquivos de log em `LOG_DIR` para diagnosticar falhas de BCP ou de upload/download no S3.
- Se precisar ajustar o delimitador de campos, configure `FIELD_TERMINATOR` no `.env` para corresponder ao usado na exportacao/importacao.
