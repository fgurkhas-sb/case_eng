# case_eng
Estudo técnico com modelo de dados, scripts SQL/ Python e massa de dados para análise

Processo
---------
Utilizado ambiente Databricks.
- SQL / Spark / Pandas
- Script conecta no repositório no Git, importa os dados de exemplo, trata, cria os objetos necessários, popula, gera tabela flat.
- Criada tabela LOG para registro dos passos do processo.

Desafios
---------
- Import de dados externos durante a execução
- Codificação (acentos) e separadores do CSV ( massa de dados )
- Tamanho INT gerado para número do cartão
- Duplicação registros a cada execução do import da base no Git
- Export da base em ambiente web, e versão free do Databricks.

Não executado
---------
- Export CSV, devido a limitações do ambiente gratuito do Databricks utilizado.

Mais tempo
---------
Validação e análise dos registros
Consistência, distribuição, concentrações e maior automação da carga.
