# MicrosoftFabric-SQLSaturdayVix2023
Arquivos utilizados no mão na massa do SQL Saturday Vix 2023 - Iniciando sua fábrica de dados no Microsoft Fabric

Em caso de dúvida, chama no linkedin: https://www.linkedin.com/in/elainecro/ 

Mas gostaria de lembrar que este é um repositório voltado para ter insumos suficientes para demonstrar as ferramentas disponibilizadas no Microsoft Fabric. O foco não é ter o melhor script python e nem a melhor medida DAX. 

s2


## Passo a passo para executar o mão na massa

Acesse o Microsoft Fabric em https://app.fabric.microsoft.com/, crie um novo Workspace para essa POC e vamos lá.

1. Crie um Lakehouse
2. Crie um DataWarehouse
3. Acesse o Lakehouse e suba o arquivo excel (PROJECAO RECEITA-10ANOS.xlsx)
4. Crie um fluxo de dados para carregar o arquivo excel.

    a. Dê um nome a ele. Ex: dfReceitas.

	PS: Você pode seguir os passos abaixo ou pode importar o arquivo dfReceitas.pqt dentro do power query e então realizar o ajuste dos apontamentos de leitura do arquivo e recriar o passo 3 de direcionar o destino dos dados.

	Ou, siga os passos abaixo dentro do fluxo de dados:

        - Carregue o arquivo excel do lakehouse
        - Duplique esse item para ter 3 itens iguais e configure cada um como segue abaixo.
            1. Setores:
                1. navega para tbSetores
                2. coloque a coluna Codigo para o tipo inteiro e a coluan Setor para tipo texto
            2. Itens:
                1. navega para tbItens
                2. Checar tipos das colunas - inteiro, texto, inteiro, decimal, data
            3. IndicadorPorItem
                1. navega para tbIndicadorPorItem
                2. Checar tipos das colunas - inteiro,inteiro, texto

    b. Aponte o destino de dados para todos jogando para o Lakehouse

5. Crie um notebook para criar dados da inflação lendo da API do banco central

    a. notebookBC_Inflacao
	
	- Crie duas células e cole o código abaixo. Backup do notebook também no arquivo "notebookBC_Inflacao.ipynb"

        
        ```python
        # Lendo API do Banco Central para pegar os indicadores de Inflação dos últimos anos, calcular médias anuais e gravar em uma tabela no Lakehouse
        import requests
        import pandas as pd
        
        tabela_final = pd.DataFrame()
        pular_indice = 0
        tamanho_pagina = 100000
        
        while True:
            urlBase = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoInflacao12Meses?$top={tamanho_pagina}&$skip={pular_indice}&$filter=Suavizada%20eq%20'S'&$format=json"
            print(urlBase)
        
            requisicao = requests.get(urlBase)
            informacoes = requisicao.json()
            tabela = pd.DataFrame(informacoes["value"])
            
            if len(informacoes['value']) < 1:
                break
        
            tabela_final = pd.concat([tabela_final, tabela])
            pular_indice += tamanho_pagina
        
        # display(tabela_final)
        
        # Filtrando dataFrame
        df_filtrado = tabela_final[tabela_final['baseCalculo'] == 0][['Indicador', 'Data', 'Media']]
        
        # Cria coluna Ano
        df_filtrado['Data'] = pd.to_datetime(df_filtrado['Data'])
        df_filtrado['Ano'] = df_filtrado['Data'].dt.year
        
        # Agrupa Indicador e Ano para calcular a MediaAnual
        media_por_ano = df_filtrado.groupby(['Indicador', 'Ano'])['Media'].mean().reset_index()
        media_por_ano = media_por_ano.rename(columns={'Media': 'MediaAnual'})
        display(media_por_ano)
        
        #Converte dataframe pandas para spark
        dfMediaPorAno = spark.createDataFrame(media_por_ano)
        
        # Salva tabela de médias dos indicadores por ano
        # nome_tabela = "BC_Inflacao_Anual"
        # spark_df.write.mode("overwrite").format("delta").save("Tables/" + nome_tabela)
        ```
        
        ```python
        # Criando a predição dos indicadores para os próximos 10 anos e salvando em tabela
        
        import numpy as np
        from sklearn.linear_model import LinearRegression
        
        # Lista para armazenar os resultados
        predicoes = []
        
        # Iterar sobre cada indicador único
        for indicador in media_por_ano['Indicador'].unique():
            
            # Filtrar os dados para o indicador atual
            subset = media_por_ano[media_por_ano['Indicador'] == indicador]
            
            # Preparar os dados
            X = subset['Ano'].values.reshape(-1, 1)  # Ano como variável independente
            y = subset['MediaAnual'].values           # Média Anual como variável dependente
            
            # Treinar o modelo
            model = LinearRegression().fit(X, y)
            
            # Prever para os próximos 10 anos
            anos_futuros = np.array(range(X[-1][0] + 1, X[-1][0] + 11)).reshape(-1, 1)
            predicoes_futuras = model.predict(anos_futuros)
            
            # Adicionar as predições à lista
            for ano, predicao in zip(anos_futuros, predicoes_futuras):
                predicoes.append({
                    'Indicador': indicador,
                    'Ano': ano[0],
                    'MediaAnualPredita': predicao
                })
        
        # Converter a lista de predições para um DataFrame
        df_predicoes = pd.DataFrame(predicoes)
        
        # Converte dataframe pandas para spark
        dfPredicoes = spark.createDataFrame(df_predicoes)
        
        # Salvando tabela com as predições dos próximos anos
        # nome_tabela = "BC_Inflacao_Predicoes"
        # spark_df.write.mode("overwrite").format("delta").save("Tables/" + nome_tabela)
        
        display(df_predicoes)
        ```
        
        ```python
        # Unindo dataFrames e salvando no Lakehouse como tabela
        dfInflacao = dfMediaPorAno.union(dfPredicoes)
        display(dfInflacao)
        
        # Salvando tabela com as predições dos próximos anos
        nome_tabela = "BC_Inflacao"
        dfInflacao.write.mode("overwrite").format("delta").save("Tables/" + nome_tabela)
        ```
        
6. Criar mais um data flow, este é para carregar os dados do lakehouse para o datawarehouse. Nome ex: dfLoadToDW
    1. Carregar as 4 tabelas do lakehouse - Setores, ,Itens, IndicadorPorItem, Inflacao
    2. Consulta: IndicadorPorItem
        1. Remover linhas em branco
        2. Remover coluna Codigo
        3. Não tem destino de dado
    3. Consulta: Itens
        1. Remover linhas em branco
        2. Inserir coluna Ano com base na DataInicio
        3. Alterar tipo de dado Ano para inteiro
        4. Renomear colunas: ValorTotal → ValorAluguel, Ano→ AnoInicioAluguel
        5. Remover coluna DataInicio
        6. Linkar destino de dado para o DW
    4. Consulta: Setores
        1. Limpar linhas em branco
        2. Linkar destino de dado para o DW
    5. Consulta: BC_Inflacao
        1. Limpar linhas em branco
        2. Não tem destino de dado
    6. Adicionar consulta, via combinação abaixo: Tabelao
        1. Combinar Itens (Codigo) com IndicadorPorItem (CodigoItem)
        2. Expandir coluna Indicador
        3. Combinar com BC_Inflacao (Indicador)
        4. Expandir colunas AnoIndicador, MediaAnual
        5. Substituir coluna AnoInicioAluguel com 9999 onde for null
        6. Coluna condicional (Manter)
            1. se AnoInicioAluguel é igual a null = Fica
            2. se AnoIndicador é maior ou igual ao AnoInicioAluguel = Fica
        7. Filtra linhas com Fica
        8. Remover colunas Manter, Item, Setor
        9. Renomear coluna Codigo para CodigoItem
        10. Linkar destino de dado para o DW
    
    
7. Criar pipeline
    1. LoadInflacao - notebook
		1. no sucesso chama LoadDW - dfLoadToDW
    2. LoadReceitas - dfReceitas
        1. no sucesso chama LoadDW - dfLoadToDW
    3. em caso de erro todos mandam email - NotificaEmail
    4. Mostrar configurações de retentativa de tempo máximo de execução de cada caixinha
8. Após processar pipeline
    1. Linkar modelo
		- Tabelao[CodigoItem] -> Itens[Codigo]
		- Itens[Setor] -> Setores[Codigo]
    2. Criar tabela de Medidas no DW
		```shell
		ValorReajuste = 
		CALCULATE(
			(SELECTEDVALUE(Tabelao[ValorAluguel]) * (SUM(Tabelao[MediaAnual])/100)),
			Tabelao[AnoIndicador] > Tabelao[AnoInicioAluguel] &&
			Tabelao[AnoIndicador] <= SELECTEDVALUE(Tabelao[AnoIndicador]
			)) + SELECTEDVALUE(Tabelao[ValorAluguel])
		```


Tudo pronto pra consumir num relatório do Power BI :) 