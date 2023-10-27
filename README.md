# MicrosoftFabric-SQLSaturdayVix2023
Arquivos utilizados no mão na massa do SQL Saturday Vix 2023 - Iniciando sua fábrica de dados no Microsoft Fabric

Em caso de dúvida, chama no linkedin: https://www.linkedin.com/in/elainecro/ 

Mas gostaria de lembrar que este é um repositório voltado para ter insumos suficientes para demonstrar as ferramentas disponibilizadas no Microsoft Fabric. O foco não é ter o melhor script python e nem a melhor medida DAX. 

s2

## Contexto do Laboratório
Imagine que uma empresa que possui itens para aluguel (salas, galpões, apartamentos, casas, etc) gostaria de visualizar a projeção da evolução dos valores dos alugueis dos seus itens no decorrer dos próximos 10 anos.
Essa projeção é feita em cima de alguns indicadores financeiros, como por exemplo IPCA e IGPM.

Então vamos construir esse laboratório a fim de disponibilizar um dashboard similar ao do link, para disponibilizar essa visão da projeção para essa empresa.

URL do painel:
https://app.fabric.microsoft.com/links/onJCAVvpCJ?ctid=95d27196-39a3-465b-9e4c-ea4b4232215d&pbi_source=linkShare&bookmarkGuid=cc7a333b-0874-4cc3-b1e0-a641fb5b2c31 


## Passo a passo para executar o mão na massa

Acesse o Microsoft Fabric em https://app.fabric.microsoft.com/, crie um novo Workspace para essa POC e vamos lá.

1. Crie um Lakehouse
2. Crie um DataWarehouse
3. Acesse o Lakehouse e suba o arquivo excel (PROJECAO RECEITA-10ANOS.xlsx)
4. Crie um fluxo de dados para carregar o arquivo excel.

    a. Dê um nome a ele. Ex: dfReceitas.

	PS: Você pode seguir os passos abaixo ou pode importar o arquivo dfReceitas.pqt na tela inicial do DataFlow - opção "Importar de um modelo do Power Query". Neste caso bastará corrigir os apontamentos das origens dos dados para o seu Lakehouse.

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
	- O código está no arquivo notebookBC_Inflacao.ipynb
	- Este código vai fazer a leitura do histórico dos indicadores da API do Banco Central, vai tratar, gerar as médias anuais do histórico, filtar por IPCA e IGPM (que é o foco da POC) e realizar a execução de um script de predição de valores para os próximos 10 anos. E por fim vai salvar a tabela gerada no lakehouse na tabela BC_Inflacao.
	    
6. Criar mais um data flow, este é para carregar os dados do lakehouse para o datawarehouse. Nome ex: dfLoadToDW
	
	PS: Pode seguir os passos abaixo, ou pode importar o arquivo dfLoadToDW.pqt na tela inicial do DataFlow - opção "Importar de um modelo do Power Query". Neste caso bastará corrigir os apontamentos das origens dos dados para o seu Lakehouse.
    
	1. Carregar as 4 tabelas do lakehouse - Setores, ,Itens, IndicadorPorItem, BC_Inflacao
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
-------------

PS: O arquivo notebookPreencheProjecoes.ipynb, é um notebook que eu tinha feito antes de seguir o caminho de criar a medida da projeção acumulada em DAX. Este script realiza a junção dos dataframes que representam Itens, IndicadoresPorItem e BC_Inflacao e realiza o cálculo do valor projetado dos anos futuros.

-------------

Aproveito para deixar também o link das documentações da Microsoft que podem te ajudar muito nos estudos do Microsoft Fabric.

- https://learn.microsoft.com/pt-br/fabric/
- https://learn.microsoft.com/pt-br/fabric/get-started/end-to-end-tutorials
