-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Função para ler as tabelas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Melhorias do notebook
-- MAGIC - Teste: Verificar se ao importar os dados com UTF8 remove os caracteres especiais. Exemplo á, ó, õ, ...

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def criar_view_temporaria(nome_tabela):
-- MAGIC     # Construindo o caminho do arquivo CSV com base no nome da tabela
-- MAGIC     caminho_csv = f"dbfs:/FileStore/tables/{nome_tabela}.csv"
-- MAGIC     
-- MAGIC     # Leitura do arquivo CSV usando PySpark
-- MAGIC     df = spark.read.format("csv") \
-- MAGIC         .option("header", "true") \
-- MAGIC         .option("inferSchema", "true") \
-- MAGIC         .option("sep", ';') \
-- MAGIC         .load(caminho_csv)
-- MAGIC     
-- MAGIC     # Criando uma view temporária com o nome especificado
-- MAGIC     df.createOrReplaceTempView(nome_tabela)
-- MAGIC     
-- MAGIC     # Exibindo as primeiras 10 linhas da view temporária
-- MAGIC     print(f"\nTabela: {nome_tabela}")
-- MAGIC     spark.sql(f"SELECT * FROM {nome_tabela} LIMIT 10").display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC criar_view_temporaria(nome_tabela="auto_cau")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC criar_view_temporaria(nome_tabela="auto_cau")
-- MAGIC criar_view_temporaria(nome_tabela="auto_cat")
-- MAGIC criar_view_temporaria(nome_tabela="auto_cep")
-- MAGIC criar_view_temporaria(nome_tabela="auto_cidade")
-- MAGIC criar_view_temporaria(nome_tabela="auto_cob")
-- MAGIC criar_view_temporaria(nome_tabela="auto_idade")
-- MAGIC criar_view_temporaria(nome_tabela="auto_reg")
-- MAGIC criar_view_temporaria(nome_tabela="auto_sexo")
-- MAGIC criar_view_temporaria(nome_tabela="auto2_grupo")
-- MAGIC criar_view_temporaria(nome_tabela="auto2_vei")
-- MAGIC criar_view_temporaria(nome_tabela="PremReg")
-- MAGIC criar_view_temporaria(nome_tabela="arq_casco_comp")
-- MAGIC criar_view_temporaria(nome_tabela="arq_casco3_comp")
-- MAGIC criar_view_temporaria(nome_tabela="arq_casco4_comp")
-- MAGIC criar_view_temporaria(nome_tabela="SinReg")

-- COMMAND ----------

Select * from Sinreg
where tipo_sin = 'CASCO' and descricao like 'SP%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Tratamento dos dados
-- MAGIC - Remover os caracteres especiais

-- COMMAND ----------

SELECT
  regexp_replace(descricao, '[àáâãä]' , 'a' ) AS descricao_sem_acentos
FROM Sinreg;

-- COMMAND ----------

SELECT
  regexp_replace(descricao, '[óòõ]' , 'o' ) AS descricao_sem_acentos
FROM Sinreg;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Contagem de registros por sexo

-- COMMAND ----------

SELECT sexo, COUNT(*) AS quantidade
FROM arq_casco_comp
GROUP BY sexo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Distribuição dos Segurados por faixa etária

-- COMMAND ----------

SELECT 
  CASE
    WHEN idade BETWEEN 18 AND 30 THEN '18-30'
    WHEN idade BETWEEN 31 AND 45 THEN '31-45'
    WHEN idade BETWEEN 46 AND 60 THEN '46-60'
    ELSE '60+' -- Faixa etária para idades acima de 60
  END AS faixa_etaria,
  COUNT(*) AS quantidade
FROM arq_casco_comp
GROUP BY faixa_etaria;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Análise de prêmios por região

-- COMMAND ----------

SELECT regiao, AVG(premio1) AS media_premio
FROM arq_casco_comp
GROUP BY regiao;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Análise por sexo

-- COMMAND ----------

SELECT sexo, AVG(idade) AS media_idade
FROM arq_casco_comp
GROUP BY sexo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Análise por região

-- COMMAND ----------

SELECT regiao, AVG(idade) AS media_idade
FROM arq_casco_comp
GROUP BY regiao;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Análise por ano do modelo

-- COMMAND ----------

SELECT ano_modelo, AVG(idade) AS media_idade
FROM arq_casco_comp
GROUP BY ano_modelo
ORDER BY ano_modelo DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Questão: Qual seria a consulta SQL para descobrir a média de sinistros (numSinistros) por região no arquivo SinReg? O que esses dados revelam sobre a frequência de sinistros em diferentes regiões?

-- COMMAND ----------

SELECT regiao, AVG(numSinistros) AS media_sinistros
FROM SinReg
GROUP BY regiao;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Questão: Qual a média de indenizações levando em consideração o valor das indenizações (indenizacoes) e o número de sinistros (numSinistros)?

-- COMMAND ----------

SELECT AVG(indenizacoes / numSinistros) AS media_indenizacao_por_sinistro
FROM SinReg
WHERE numSinistros > 0
GROUP BY regiao
ORDER BY media_indenizacao_por_sinistro;


-- COMMAND ----------

SELECT * FROM auto_cat

-- COMMAND ----------

-- Consulta para calcular a quantidade total de furtos por região
SELECT
    ac.REGIAO,  -- Supondo que REGIAO é a coluna que contém a informação de região
    COALESCE(SUM(ac.FREQ_SIN1), 0) AS total_furtos
FROM
    arq_casco_comp ac
GROUP BY
    ac.REGIAO
ORDER BY
    total_furtos DESC;



-- COMMAND ----------

-- Consulta para calcular a quantidade total de furtos por região, usando o nome da região
SELECT
    ar.DESCRICAO AS nome_regiao,  -- Nome da região da tabela auto_reg
    COALESCE(SUM(ac.FREQ_SIN1), 0) AS total_furtos
FROM
    arq_casco_comp ac
JOIN
    auto_reg ar ON ac.REGIAO = ar.CODIGO  -- Junção para obter o nome da região
GROUP BY
    ar.DESCRICAO
ORDER BY
    total_furtos DESC;



-- COMMAND ----------

-- Consulta para calcular a quantidade total de furtos por região, tipo de veículo, sexo e faixa etária
SELECT
    ar.DESCRICAO AS nome_regiao,         -- Nome da região da tabela auto_reg
    acat.CATEGORIA AS tipo_veiculo,      -- Tipo de veículo da tabela auto_cat (descrição da categoria)
    asx.DESCRICAO AS sexo,              -- Descrição do sexo da tabela auto_sexo
    ai.DESCRICAO AS faixa_etaria,        -- Descrição da faixa etária da tabela auto_idade
    COALESCE(SUM(ac.FREQ_SIN1), 0) AS total_furtos
FROM
    arq_casco_comp ac
JOIN
    auto_reg ar ON ac.REGIAO = ar.CODIGO  -- Junção para obter o nome da região
JOIN
    auto_cat acat ON ac.COD_TARIF = acat.CODIGO  -- Junção para obter o tipo de veículo (usando CATEGORIA)
JOIN
    auto_sexo asx ON ac.SEXO = asx.CODIGO  -- Junção para obter a descrição do sexo
JOIN
    auto_idade ai ON ac.IDADE = ai.CODIGO  -- Junção para obter a descrição da faixa etária
GROUP BY
    ar.DESCRICAO,                    -- Nome da região
    acat.CATEGORIA,                  -- Tipo de veículo (categoria)
    asx.DESCRICAO,                  -- Sexo
    ai.DESCRICAO                    -- Faixa etária
ORDER BY
    total_furtos DESC;


-- COMMAND ----------

-- Consultar registros específicos com o código 4 e incluir a descrição do veículo
SELECT
    ac.*,
    acat.CATEGORIA AS descricao_veiculo
FROM
    arq_casco_comp ac
JOIN
    auto_cat acat ON ac.COD_TARIF = acat.CODIGO
WHERE
    ac.COD_TARIF = 4;  -- Código para "Veículo de Carga (nacional e importado)"



-- COMMAND ----------

-- Consultar e filtrar registros por região, incluindo descrição do veículo e ordenar por furtos
SELECT
    ar.DESCRICAO AS nome_regiao,                  -- Nome da região da tabela auto_reg
    acat.CATEGORIA AS descricao_veiculo,         -- Descrição do tipo de veículo da tabela auto_cat
    COALESCE(SUM(ac.FREQ_SIN1), 0) AS total_furtos
FROM
    arq_casco_comp ac
JOIN
    auto_cat acat ON ac.COD_TARIF = acat.CODIGO  -- Junção para obter a descrição do tipo de veículo
JOIN
    auto_reg ar ON ac.REGIAO = ar.CODIGO         -- Junção para obter o nome da região
WHERE
    ac.COD_TARIF = 4                             -- Filtro para "Veículo de Carga (nacional e importado)"
GROUP BY
    ar.DESCRICAO,                               -- Nome da região
    acat.CATEGORIA                              -- Descrição do veículo
ORDER BY
    total_furtos DESC;                          -- Ordenar por total de furtos em ordem decrescente


-- COMMAND ----------

-- Consultar e filtrar registros por região, incluindo a sigla do estado e descrição do veículo, e ordenar por furtos
SELECT
    SUBSTRING(ar.DESCRICAO, 1, 2) AS estado,              -- Extrai a sigla do estado (primeiras duas letras)
    acat.CATEGORIA AS descricao_veiculo,                 -- Descrição do tipo de veículo da tabela auto_cat
    COALESCE(SUM(ac.FREQ_SIN1), 0) AS total_furtos
FROM
    arq_casco_comp ac
JOIN
    auto_cat acat ON ac.COD_TARIF = acat.CODIGO          -- Junção para obter a descrição do tipo de veículo
JOIN
    auto_reg ar ON ac.REGIAO = ar.CODIGO                 -- Junção para obter o nome da região
WHERE
    ac.COD_TARIF = 4                                   -- Filtro para "Veículo de Carga (nacional e importado)"
GROUP BY
    SUBSTRING(ar.DESCRICAO, 1, 2),                      -- Agrupa pela sigla do estado
    acat.CATEGORIA                                    -- Descrição do veículo
ORDER BY
    total_furtos DESC;                                -- Ordena por total de furtos em ordem decrescente

