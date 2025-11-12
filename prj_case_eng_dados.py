# Databricks notebook source
# MAGIC %md
# MAGIC ### Case Engenharia de Dados

# COMMAND ----------


import io, requests, pandas as pd
from pyspark.sql import functions as F


spark.sql("""
DROP TABLE IF EXISTS LOG
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS log (
  id INT,
  etapa STRING,
  data_hora_ini timestamp,
  data_hora_fim timestamp,
  status STRING,
  qtd_reg int
)
""")

spark.sql("""
INSERT INTO LOG
VALUES (1,"ROTINA - CASE",CURRENT_TIMESTAMP(),null,"",0)
""")


USER   = "fgurkhas-sb"     
REPO   = "case_eng"        
BRANCH = "main"            
SUBDIR = "dados"           
USE_CATALOG = False        
CATALOG     = "main"
SCHEMA      = "default"

# seta catalogo
if USE_CATALOG:
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")

# retira cache
for t in ["associado", "conta", "cartao", "movimento"]:
    try:
        spark.sql(f"UNCACHE TABLE {t}")
    except Exception:
        pass


spark.sql("""
INSERT INTO LOG
VALUES (2,"IMPORT DADOS GIT E CRIA FUNCOES",CURRENT_TIMESTAMP(),null,"",0)
""")

def gh_url(user, repo, branch, subdir, filename):
    path = f"{subdir}/{filename}" if subdir else filename
    return f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{path}"

def read_csv_github_comma(user, repo, branch, subdir, filename):
    """
    sep=','.
    tenta UTF-8; se falhar cai para Latin-1.
    """
    url = gh_url(user, repo, branch, subdir, filename)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    raw = io.BytesIO(r.content)
    try:
        df = pd.read_csv(raw, sep=",", encoding="utf-8")
    except UnicodeDecodeError:
        raw.seek(0)
        df = pd.read_csv(raw, sep=",", encoding="latin1")
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]
    return df
  


def overwrite_table_from_pdf(pdf, casts: dict, table: str):
    df = spark.createDataFrame(pdf)
    for col, typ in casts.items():
        if typ.lower().startswith("decimal"):
            df = df.withColumn(col, F.regexp_replace(F.col(col).cast("string"), ",", ".").cast(typ))
        else:
            df = df.withColumn(col, F.col(col).cast(typ))
    (df.write
       .mode("overwrite")                
       .option("overwriteSchema", "true")
       .saveAsTable(table))

spark.sql("""
UPDATE LOG SET DATA_HORA_FIM = CURRENT_TIMESTAMP(), STATUS = "OK" WHERE ID = 2
""")


#criacao tabelas

spark.sql("""
INSERT INTO LOG
VALUES (3,"CRIACAO TABELAS",CURRENT_TIMESTAMP(),null,"",0)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS associado (
  id INT,
  nome STRING,
  sobrenome STRING,
  idade INT,
  email STRING
)
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS conta (
  id INT,
  tipo STRING,
  data_criacao TIMESTAMP,
  id_associado INT
)
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS cartao (
  id INT,
  num_cartao INT,    
  nom_impresso STRING,
  id_conta INT,
  id_associado INT,
  data_criacao TIMESTAMP
)
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS movimento (
  id INT,
  vl_transacao DECIMAL(10,2),
  des_transacao STRING,
  data_movimento TIMESTAMP,
  id_cartao INT
)
""")

spark.sql("""
UPDATE LOG SET DATA_HORA_FIM = CURRENT_TIMESTAMP(), STATUS = "OK" WHERE ID = 3
""")

#carga de dados

spark.sql("""
INSERT INTO LOG
VALUES (4,"CARGA DE DADOS",CURRENT_TIMESTAMP(),null,"",0)
""")

# associado
pdf = read_csv_github_comma(USER, REPO, BRANCH, SUBDIR, "associado.csv")
overwrite_table_from_pdf(
    pdf,
    casts={"id":"int","nome":"string","sobrenome":"string","idade":"int","email":"string"},
    table="associado"
)

# conta
pdf = read_csv_github_comma(USER, REPO, BRANCH, SUBDIR, "conta.csv")
overwrite_table_from_pdf(
    pdf,
    casts={"id":"int","tipo":"string","data_criacao":"timestamp","id_associado":"int"},
    table="conta"
)

# cartao  (num_cartao INT)
pdf = read_csv_github_comma(USER, REPO, BRANCH, SUBDIR, "cartao.csv")
overwrite_table_from_pdf(
    pdf,
    casts={"id":"int","num_cartao":"int","nom_impresso":"string","id_conta":"int","id_associado":"int","data_criacao":"timestamp"},
    table="cartao"
)

# movimento (normaliza vírgula decimal se aparecer)
pdf = read_csv_github_comma(USER, REPO, BRANCH, SUBDIR, "movimento.csv")
overwrite_table_from_pdf(
    pdf,
    casts={"id":"int","vl_transacao":"decimal(10,2)","des_transacao":"string","data_movimento":"timestamp","id_cartao":"int"},
    table="movimento"
)

spark.sql("""
UPDATE LOG SET DATA_HORA_FIM = CURRENT_TIMESTAMP(), STATUS = "OK" WHERE ID = 4
""")


#validacao

spark.sql("""
INSERT INTO LOG

SELECT
5 AS ID,
'VALIDA TABELA - '||tabela as etapa,
current_timestamp() AS data_hora_ini,
current_timestamp() AS data_hora_fim,
'OK' AS status,
registros as qtd_reg

from
(
SELECT 'associado' AS tabela, COUNT(*) AS registros FROM associado group by 1
UNION ALL 
SELECT 'conta' AS tabela, COUNT(*) FROM conta group by 1
UNION ALL 
SELECT 'cartao' AS tabela, COUNT(*) FROM cartao group by 1
UNION ALL 
SELECT 'movimento' AS tabela, COUNT(*) FROM movimento group by 1
)
""")


#tabela flat

spark.sql("""
INSERT INTO LOG
VALUES (6,"CRIACAO TABELA FLAT",CURRENT_TIMESTAMP(),null,"",0)
""")

spark.sql("""
DROP TABLE IF EXISTS movimento_flat
""") 

spark.sql("""
CREATE TABLE movimento_flat
as 

select
ass.nome as nome_associado,
ass.sobrenome as sobrenome_associado,
cast(ass.idade  as string) as idade_associado,

cast(mo.vl_transacao  as string) as vlr_transacao_movimento,
cast(mo.des_transacao  as string) as des_transacao_movimento,
cast(mo.data_movimento  as string) ,

cast(ca.num_cartao as string)  as numero_cartao,
ca.nom_impresso as nome_impresso_cartao,
cast(ca.data_criacao as string) as data_criacao_cartao,

co.tipo as tipo_conta,
cast(co.data_criacao as string) as data_criacao_conta


from
associado as ass

inner join conta as co
on ass.id = co.id

left join cartao as ca
on ca.id = ass.id

left join movimento as mo
on mo.id_cartao = ca.id 

""")

spark.sql("""
UPDATE LOG 
SET DATA_HORA_FIM = CURRENT_TIMESTAMP(), 
STATUS = "OK" ,
QTD_REG = (SELECT COUNT(*) FROM movimento_flat)

WHERE ID = 6
""")

spark.sql("""
UPDATE LOG SET DATA_HORA_FIM = CURRENT_TIMESTAMP(), STATUS = "OK" WHERE ID = 1
""")




# COMMAND ----------

