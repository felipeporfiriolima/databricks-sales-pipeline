#----------------------#
# configuração inicial #
#----------------------#

import sys
import os

cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.append(cwd)

# Se o config.py estiver na pasta raiz do seu repositório Git no Databricks, 
# adicione o caminho do repositório/workspace atual:
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))
    if repo_root not in sys.path:
        sys.path.append(repo_root)
except Exception:
    pass

from pyspark.sql.functions import current_timestamp, col
from pyspark import pipelines as dp
from config import CATALOGO, BRONZE, SCHEMA_LOJA, LOJA, LOJA_QUARENTENA, DATASET

catalogo = CATALOGO
schema = BRONZE
tabela = LOJA
quarentena = LOJA_QUARENTENA
tabela_bronze = f"{catalogo}.{schema}.{tabela}"
tabela_quarentena = f"{catalogo}.{schema}.{quarentena}"
schema = SCHEMA_LOJA
arquivo = f"{DATASET}/{LOJA}/"

#--------------------------------------#
# Carrega os arquivos de loja pendente #
#--------------------------------------#

@dp.table(name=tabela_bronze)
def lojas():
    df = (
        spark.readStream.format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("mode", "PERMISSIVE")
        .load(arquivo)
    )
    df = (
        df
        .withColumn("dtCarga", current_timestamp())
        .withColumn("arquivo_origem", col("_metadata.file_path"))
    )
    return df 

#--------------------------------#
# Carrega a tabela de quarentena #
#--------------------------------#

@dp.table(name=tabela_quarentena)
def lojas_quarentena():
    return spark.readStream.table(f"{tabela_bronze}").filter(
        col("_rescued_data").isNotNull()
        | col("id_loja").isNull()
        | col("nome").isNull()
    )
