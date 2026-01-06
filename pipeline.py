import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List
import logging
from tqdm import tqdm

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CNPJDataPipeline:
    """
    Pipeline para processar dados abertos do CNPJ:
    - Extrai ZIPs para uma pasta temporária
    - Renomeia arquivos extraídos adicionando ".csv" ao final do nome
    - Agrupa por tipo (empresas, estabelecimentos, etc.)
    - Converte CSVs (em chunks) para Parquet (1 arquivo por tipo)
    - Gera relatório de saída
    (Sem qualquer deduplicação)
    """

    def __init__(self, pasta_zip: str, pasta_parquet: str):
        self.pasta_zip = Path(pasta_zip)
        self.pasta_parquet = Path(pasta_parquet)
        self.pasta_temp = Path("temp_extraidos")

        # Criar pastas
        self.pasta_parquet.mkdir(exist_ok=True, parents=True)
        self.pasta_temp.mkdir(exist_ok=True, parents=True)

        # Schemas das tabelas conforme metadados
        self.schemas = self._definir_schemas()

    def _definir_schemas(self) -> Dict:
        """Define os schemas de cada tipo de arquivo"""
        return {
            "empresas": {
                "colunas": [
                    "cnpj_basico",
                    "razao_social",
                    "natureza_juridica",
                    "qualificacao_responsavel",
                    "capital_social",
                    "porte_empresa",
                    "ente_federativo_responsavel",
                ],
                "dtypes": {
                    "cnpj_basico": str,
                    "razao_social": str,
                    "natureza_juridica": str,
                    "qualificacao_responsavel": str,
                    "capital_social": str,
                    "porte_empresa": str,
                    "ente_federativo_responsavel": str,
                },
            },
            "estabelecimentos": {
                "colunas": [
                    "cnpj_basico",
                    "cnpj_ordem",
                    "cnpj_dv",
                    "identificador_matriz_filial",
                    "nome_fantasia",
                    "situacao_cadastral",
                    "data_situacao_cadastral",
                    "motivo_situacao_cadastral",
                    "nome_cidade_exterior",
                    "pais",
                    "data_inicio_atividade",
                    "cnae_fiscal_principal",
                    "cnae_fiscal_secundaria",
                    "tipo_logradouro",
                    "logradouro",
                    "numero",
                    "complemento",
                    "bairro",
                    "cep",
                    "uf",
                    "municipio",
                    "ddd_1",
                    "telefone_1",
                    "ddd_2",
                    "telefone_2",
                    "ddd_fax",
                    "fax",
                    "correio_eletronico",
                    "situacao_especial",
                    "data_situacao_especial",
                ],
                "dtypes": {
                    "cnpj_basico": str,
                    "cnpj_ordem": str,
                    "cnpj_dv": str,
                    "cep": str,
                    "ddd_1": str,
                    "telefone_1": str,
                    "ddd_2": str,
                    "telefone_2": str,
                },
            },
            "socios": {
                "colunas": [
                    "cnpj_basico",
                    "identificador_socio",
                    "nome_socio",
                    "cnpj_cpf_socio",
                    "qualificacao_socio",
                    "data_entrada_sociedade",
                    "pais",
                    "representante_legal",
                    "nome_representante",
                    "qualificacao_representante_legal",
                    "faixa_etaria",
                ],
                "dtypes": {
                    "cnpj_basico": str,
                    "cnpj_cpf_socio": str,
                    "representante_legal": str,
                },
            },
            "simples": {
                "colunas": [
                    "cnpj_basico",
                    "opcao_simples",
                    "data_opcao_simples",
                    "data_exclusao_simples",
                    "opcao_mei",
                    "data_opcao_mei",
                    "data_exclusao_mei",
                ],
                "dtypes": {"cnpj_basico": str},
            },
            "paises": {"colunas": ["codigo", "descricao"], "dtypes": {"codigo": str}},
            "municipios": {"colunas": ["codigo", "descricao"], "dtypes": {"codigo": str}},
            "qualificacoes": {"colunas": ["codigo", "descricao"], "dtypes": {"codigo": str}},
            "naturezas": {"colunas": ["codigo", "descricao"], "dtypes": {"codigo": str}},
            "cnaes": {"colunas": ["codigo", "descricao"], "dtypes": {"codigo": str}},
            "motivos": {"colunas": ["codigo", "descricao"], "dtypes": {"codigo": str}},
        }

    def identificar_tipo_arquivo(self, nome_arquivo: str) -> str:
        """Identifica o tipo de arquivo pelo nome"""
        nome_lower = nome_arquivo.lower()

        if "empre" in nome_lower:
            return "empresas"
        elif "estabe" in nome_lower or "estable" in nome_lower:
            return "estabelecimentos"
        elif "socio" in nome_lower:
            return "socios"
        elif "simples" in nome_lower:
            return "simples"
        elif "pais" in nome_lower:
            return "paises"
        elif "munic" in nome_lower:
            return "municipios"
        elif "quals" in nome_lower:
            return "qualificacoes"
        elif "natju" in nome_lower:
            return "naturezas"
        elif "cnae" in nome_lower:
            return "cnaes"
        elif "moti" in nome_lower:
            return "motivos"
        else:
            return "desconhecido"

    def extrair_zips(self):
        """Extrai todos os arquivos ZIP para pasta temporária"""
        logger.info("Iniciando extração dos arquivos ZIP...")

        arquivos_zip = list(self.pasta_zip.glob("*.zip"))
        if not arquivos_zip:
            logger.warning(f"Nenhum arquivo ZIP encontrado em {self.pasta_zip}")
            return

        for arquivo_zip in tqdm(arquivos_zip, desc="Extraindo ZIPs"):
            try:
                with zipfile.ZipFile(arquivo_zip, "r") as zip_ref:
                    zip_ref.extractall(self.pasta_temp)
                logger.info(f"Extraído: {arquivo_zip.name}")
            except Exception as e:
                logger.error(f"Erro ao extrair {arquivo_zip.name}: {e}")

        # Renomear todos os arquivos para terminar com .csv
        self.renomear_todos_para_csv()

    def renomear_todos_para_csv(self):
        """Adiciona .csv no final de TODOS os arquivos dentro de temp_extraidos"""
        logger.info("Adicionando .csv no final de todos os arquivos extraídos...")

        contador = 0
        for arquivo in self.pasta_temp.rglob("*"):
            if arquivo.is_file():
                novo_nome = arquivo.parent / f"{arquivo.name}.csv"
                try:
                    arquivo.rename(novo_nome)
                    contador += 1
                except Exception as e:
                    logger.error(f"Erro ao renomear {arquivo.name}: {e}")

        logger.info(f"✓ {contador} arquivo(s) renomeado(s) adicionando .csv no final")

    def agrupar_arquivos_por_tipo(self) -> Dict[str, List[Path]]:
        """Agrupa arquivos CSV extraídos por tipo"""
        arquivos_por_tipo: Dict[str, List[Path]] = {}

        arquivos_csv = list(self.pasta_temp.glob("*.csv")) + list(self.pasta_temp.glob("**/*.csv"))
        logger.info(f"Total de arquivos CSV encontrados: {len(arquivos_csv)}")

        for arquivo in arquivos_csv:
            tipo = self.identificar_tipo_arquivo(arquivo.name)
            if tipo != "desconhecido":
                arquivos_por_tipo.setdefault(tipo, []).append(arquivo)

        return arquivos_por_tipo

    def processar_csv_para_parquet(self, tipo: str, arquivos: List[Path]):
        """
        Processa múltiplos CSVs do mesmo tipo e gera um Parquet (um arquivo por tipo).
        Sem deduplicação.
        """
        logger.info(f"Processando {len(arquivos)} arquivo(s) do tipo: {tipo}")

        schema_info = self.schemas.get(tipo)
        if not schema_info:
            logger.warning(f"Schema não definido para tipo: {tipo}")
            return

        chunk_size = 100000
        writer = None
        arquivo_parquet = self.pasta_parquet / f"{tipo}.parquet"

        for arquivo_csv in tqdm(arquivos, desc=f"Processando {tipo}"):
            try:
                for chunk in pd.read_csv(
                    arquivo_csv,
                    sep=";",
                    encoding="latin1",
                    header=None,
                    names=schema_info["colunas"],
                    dtype=schema_info.get("dtypes", None),
                    chunksize=chunk_size,
                    low_memory=False,
                    na_values=["", "NULL", "null"],
                    on_bad_lines="skip",
                ):
                    table = pa.Table.from_pandas(chunk, preserve_index=False)

                    if writer is None:
                        writer = pq.ParquetWriter(
                            arquivo_parquet,
                            table.schema,
                            compression="snappy",
                        )

                    writer.write_table(table)

            except Exception as e:
                logger.error(f"Erro ao processar {arquivo_csv.name}: {e}")

        if writer:
            writer.close()
            logger.info(f"✓ Parquet criado: {arquivo_parquet}")

    def gerar_relatorio(self):
        """Gera relatório dos arquivos Parquet criados"""
        logger.info("\n" + "=" * 60)
        logger.info("RELATÓRIO DE ARQUIVOS PARQUET")
        logger.info("=" * 60)

        arquivos_parquet = list(self.pasta_parquet.glob("*.parquet"))
        if not arquivos_parquet:
            logger.warning("Nenhum arquivo Parquet foi gerado!")
            return

        for arquivo in arquivos_parquet:
            tamanho_mb = arquivo.stat().st_size / (1024 * 1024)

            try:
                parquet_file = pq.ParquetFile(arquivo)
                num_linhas = parquet_file.metadata.num_rows

                logger.info(f"\n{arquivo.name}")
                logger.info(f"   Tamanho: {tamanho_mb:.2f} MB")
                logger.info(f"   Registros: {num_linhas:,}")
            except Exception as e:
                logger.error(f"Erro ao ler metadados de {arquivo.name}: {e}")

    def executar_pipeline(self):
        """Executa o pipeline completo (sem deduplicação)"""
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE DE DADOS CNPJ (SEM DEDUPLICAÇÃO)")
        logger.info("=" * 60)

        # Passo 1: Extrair ZIPs
        self.extrair_zips()

        # Passo 2: Agrupar arquivos por tipo
        logger.info("\nAgrupando arquivos por tipo...")
        arquivos_por_tipo = self.agrupar_arquivos_por_tipo()

        if not arquivos_por_tipo:
            logger.error("Nenhum arquivo foi encontrado para processar!")
            return

        for tipo, arquivos in arquivos_por_tipo.items():
            logger.info(f"  - {tipo}: {len(arquivos)} arquivo(s)")

        # Passo 3: Processar cada tipo para Parquet
        logger.info("\nProcessando arquivos para Parquet...")
        for tipo, arquivos in arquivos_por_tipo.items():
            self.processar_csv_para_parquet(tipo, arquivos)

        # Passo 4: Relatório
        self.gerar_relatorio()

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 60)

    def limpar_temporarios(self):
        """Remove arquivos temporários"""
        import shutil

        if self.pasta_temp.exists():
            shutil.rmtree(self.pasta_temp)
            logger.info("Arquivos temporários removidos")


# Script principal
if __name__ == "__main__":
    PASTA_ZIP = r"./data/dados_cnpj"       # Onde estão os ZIPs baixados
    PASTA_PARQUET = r"./data/cnpj_parquet" # Onde serão salvos os Parquets

    pipeline = CNPJDataPipeline(PASTA_ZIP, PASTA_PARQUET)
    pipeline.executar_pipeline()

    # Opcional: limpar arquivos temporários
    pipeline.limpar_temporarios()
