# AutoSeg-Risk-Evaluation
Projeto de análise de dados voltado para o seguro de veículos, utilizando a base AutoSeg da SUSEP. O foco está na avaliação de riscos, definição de critérios de subscrição e otimização da precificação com base em variáveis como tipo de veículo, perfil do cliente e região.

---

## 1. Coleta e Extração de Dados
- **Bases de Dados Selecionadas:** AutoSeg  
  [Acesse a base de dados aqui](https://www2.susep.gov.br/menuestatistica/Autoseg/principal.aspx)
- **Ferramentas Utilizadas:** Python (pandas, numpy), SQL, Excel.
- **Objetivo:** Consolidar as bases de dados de diversos semestres em um único dataset para facilitar a análise.

## 2. Preparação e Tratamento dos Dados
- **Limpeza de Dados:**
  - Remoção de duplicatas e registros incompletos.
  - Tratamento de valores ausentes (imputação ou exclusão).
  - Identificação e tratamento de outliers.
- **Normalização e Transformação:**
  - Conversão de variáveis categóricas em variáveis numéricas (one-hot encoding).
  - Normalização de variáveis numéricas (idade, valor do veículo, etc.).

## 3. Feature Engineering: Derivação de Variáveis de Risco
- **Coluna: `Modelo_Veiculo`**
  - **Derivação:** Utilizar a marca e o modelo do veículo como uma variável categórica, podendo ser transformada em “grupos de risco” (ex.: veículos populares, SUVs, etc.).
  
- **Coluna: `Ano_Fabricacao_Veiculo`**
  - **Derivação:** Calcular a idade do veículo subtraindo o ano de fabricação do ano atual. Exemplo: `Idade_Veiculo = Ano_Atual - Ano_Fabricacao_Veiculo`.

- **Coluna: `Valor_Segurado_Veiculo` (ou `IS_Média`)**
  - **Derivação:** Utilizar diretamente como variável contínua para refletir o valor do bem segurado.

- **Coluna: `Sexo_Segurado`**
  - **Derivação:** Manter como uma variável categórica (masculino/feminino), que pode ser relevante para identificar diferenças de risco.

- **Coluna: `Idade_Segurado`**
  - **Derivação:** Usar diretamente como variável contínua ou categorizada em faixas etárias (ex.: 18-25, 26-35, etc.).

- **Coluna: `Cidade_Segurado` e `Estado_Segurado` (ou `Região Susep`)**
  - **Derivação:** Transformar em variáveis categóricas. Pode-se derivar um índice de risco por cidade ou estado, baseado em sinistralidade por região.

- **Coluna: `Valor_Premio`**
  - **Derivação:** Usar diretamente para calcular o prêmio pago pelo segurado. Pode ser combinado com o valor segurado para criar uma razão Prêmio/Valor_Segurado.

- **Coluna: `Numero_Sinistros`**
  - **Derivação:** Utilizar para calcular a frequência de sinistros: `Frequencia_Sinistros = Numero_Sinistros / Tempo_Exposicao`.

- **Coluna: `Valor_Indenizacao`**
  - **Derivação:** Calcular a severidade média dos sinistros: `Severidade_Sinistros = Valor_Indenizacao / Numero_Sinistros`.

- **Variáveis Externas (Possivelmente Integradas):**
  - **Índice de Criminalidade (dados externos)**
    - **Derivação:** Pode ser integrado como uma variável que reflete o risco por região (ex.: taxa de roubos por cidade).
  
  - **Densidade de Tráfego (dados externos)**
    - **Derivação:** Variável contínua ou categórica refletindo áreas com maior tráfego e, portanto, maior risco de colisão.

## 4. Análise Exploratória de Dados (EDA)
- **Objetivo:** Identificar padrões, correlações e relações entre variáveis que possam influenciar a precificação.
- **Técnicas Utilizadas:**
  - Análise estatística descritiva.
  - Visualizações gráficas (histogramas, boxplots, scatterplots).
  - Identificação de fatores de risco com maior impacto nos sinistros e nos prêmios.
