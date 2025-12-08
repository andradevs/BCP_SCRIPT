# BCP Script Toolkit

Scripts para exportar consultas SQL via `bcp`, compactar em gzip e enviar para o S3, além de baixar e reimportar arquivos `.bcp` diretamente no banco.

## Requisitos

- Python 3.10+
- Ferramentas de linha de comando `bcp` e `sqlcmd` acessíveis no `PATH` (ou configure os caminhos via `.env`).
- Credenciais AWS com permissão de leitura/escrita no bucket definido.

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuração

Copie o arquivo de exemplo e preencha as variáveis obrigatórias:

```bash
cp .env.example .env
```

Principais variáveis:

- **Banco**: `DB_SERVER`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`.
- **Ferramentas**: `BCP_PATH` (bcp), `SQLCMD_PATH` (sqlcmd), `FIELD_TERMINATOR` (padrão `;`), `BCP_KEEP_IDENTITY` (padrão `true` para manter valores de colunas IDENTITY nos imports), `BCP_MAX_ERRORS` (padrão `1` para interromper o bcp no primeiro erro e evitar cargas parciais) e `BCP_ERROR_FILE` (caminho para registrar linhas que falharam; se vazio cria um `.err` no diretório de logs e o script exibe as primeiras linhas em caso de falha).
- **Diretórios**: `SCRIPTS_DIR`, `OUTPUT_DIR`, `DOWNLOAD_DIR`, `LOG_DIR`.
- **S3**: `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_BUCKET`, `S3_KEY` (prefixo para exportar/importar) e opcional `S3_OBJECT_KEY` para importar um arquivo específico.

Veja o `.env.example` para a lista completa de variáveis suportadas.

## Uso

### Exportar queries para BCP e enviar ao S3

1. Coloque os arquivos `.sql` no diretório configurado em `SCRIPTS_DIR` (padrão `./scripts`).
2. Execute o script de exportação:

```bash
python main.py            # executa todos os .sql do diretório
python main.py --scripts script1.sql outro.sql  # executa apenas arquivos específicos
```

O script gera arquivos `.bcp`, compacta em `.gz` e envia para `s3://$S3_BUCKET/$S3_KEY/<arquivo>.bcp.gz`. Logs ficam em `LOG_DIR`.

### Importar do S3 para o banco

1. Confirme as variáveis de conexão (`DB_*`) e de S3 (`S3_*`).
2. Opcional: defina `S3_OBJECT_KEY` para usar um arquivo específico; caso contrário, o script buscará o `.bcp/.bcp.gz` mais recente dentro de `S3_KEY`.
3. Execute o script de importação:

```bash
python import_from_s3.py
```

Durante a execução será exibida uma confirmação antes de executar `TRUNCATE TABLE` na tabela inferida a partir do nome do arquivo. O arquivo é baixado para `DOWNLOAD_DIR`, descompactado se necessário e importado via `bcp`.

## Dicas

- Utilize nomes de arquivo `.bcp` no formato `tabela_YYYYMMDD_HHMMSS.bcp.gz` para que a importação automática identifique a tabela correta.
- Verifique os arquivos de log em `LOG_DIR` para diagnosticar falhas de BCP ou de upload/download no S3.
- Se precisar ajustar o delimitador de campos, configure `FIELD_TERMINATOR` no `.env` para corresponder ao usado na exportação/importação.
