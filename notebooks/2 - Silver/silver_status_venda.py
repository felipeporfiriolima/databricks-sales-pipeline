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

from pyspark.sql.functions import col, current_timestamp, upper
from pyspark import pipelines as dp
from config import CATALOGO, BRONZE, SILVER, STATUS_VENDA

catalogo = CATALOGO
schema_silver = SILVER
schema_bronze = BRONZE
tabela = STATUS_VENDA
key = ["id_status"]

tabela_bronze = f"{catalogo}.{schema_bronze}.{tabela}"
tabela_silver = f"{catalogo}.{schema_silver}.{tabela}"
view = "vw_status"

@dp.view(name=view)
def vw_status():
    return (
        spark.readStream
        .table(tabela_bronze)
        .filter(col("_rescued_data").isNull())
        .filter(col("id_status").isNotNull())
        .filter(col("status").isNotNull())
        .select("id_status","status","dtCarga")
        .drop("_rescued_data")
        .drop("arquivo_origem")
    )

dp.create_streaming_table(name=tabela_silver)

dp.create_auto_cdc_flow(
    target=tabela_silver,
    source=view,
    keys=key,
    sequence_by="dtCarga",
    stored_as_scd_type=1,
    except_column_list=["dtCarga"]
)
        