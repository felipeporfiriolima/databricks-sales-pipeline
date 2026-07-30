CATALOGO = "workspace"

# camadas #

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

# dataset #

DATASET = "/Volumes/workspace/default/dados/vendas/"
VENDAS_ITEM = "/Volumes/workspace/default/dados/vendas/vendas_item/" 

# tabelas #

CLIENTE = "clientes"
CLIENTE_QUARENTENA = "clientes_quarentena"
CLIENTE_GOLD = "dim_cliente"
LOJA = "lojas"
LOJA_QUARENTENA = "lojas_quarentena"
LOJA_GOLD = "dim_loja"
PRODUTO = "produtos"
PRODUTO_QUARENTENA = "produtos_quarentena"
PRODUTO_GOLD = "dim_PRODUTO"
STATUS_VENDA = "status_venda"
STATUS_VENDA_GOLD = "dim_status_venda"
VENDA = "vendas"
VENDA_QUARENTENA = "vendas_quarentena"
VENDA_GOLD = "fato_venda"

# schemas #

SCHEMA_CLIENTE = """
id_cliente INT,
nome STRING
"""

SCHEMA_PRODUTO = """
id_produto INT,
produto STRING
"""

SCHEMA_LOJA = """
id_loja INT,
nome STRING,
estado STRING
"""

SCHEMA_STATUS = """
id_status INT,
status STRING
"""

SCHEMA_VENDA = """
id_venda BIGINT,
data_venda DATE,
id_cliente BIGINT,
id_loja BIGINT,
id_status BIGINT,
id_produto BIGINT,
quantidade INT,
vl_unitario DECIMAL(10,2)
"""