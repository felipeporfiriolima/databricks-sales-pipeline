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
from config import CATALOGO, BRONZE, SCHEMA_STATUS, STATUS_VENDA, DATASET 

catalogo = CATALOGO
schema = BRONZE
tabela = STATUS_VENDA
tabela_bronze = f"{catalogo}.{schema}.{tabela}"
schema = SCHEMA_STATUS 
arquivo = f"{DATASET}/{STATUS_VENDA}/"

#----------------------------------------#
# Carrega os arquivos de status pendente #
#----------------------------------------#

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
