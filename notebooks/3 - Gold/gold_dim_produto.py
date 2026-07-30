#----------------------#
# configuração inicial #
#----------------------#

import sys
import os

cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.append(cwd)

# Se o config.py estiver na pasta raiz do seu repositório Git no Databricks, adicione o caminho do repositório/workspace atual:
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))
    if repo_root not in sys.path:
        sys.path.append(repo_root)
except Exception:
    pass

from pyspark.sql.functions import col, current_timestamp, upper
from pyspark import pipelines as dp
from config import CATALOGO, SILVER, GOLD, PRODUTO, PROUTO_GOLD

catalogo = CATALOGO
schema_silver = SILVER
schema_gold = GOLD
tabela = PRODUTO
gold = PROUTO_GOLD
tabela_silver = f"{catalogo}.{schema_silver}.{tabela}"
tabela_gold = f"{catalogo}.{schema_gold}.{gold}"

@dp.table(name=tabela_gold)
def dim_loja():
    return (
        spark.read
        .table(tabela_silver)
        .select(
            col("id_produto").alias("id_produto"),
            col("produto").alias("nm_produto"),
        )
    )