# pipeline-games-airflow
Um projeto da pós graduação onde analiso dois datasets de jogos e retorno os 64 jogos mais bem avaliados de 2019

PARA TESTAR A PIPELINE EXECUTE OS SEGUINTES PASSOS:

# 1
Mova a pasta do projeto para sua pasta do airfow instalado em seu pc, ou caso já possua uma pasta 'dag' mova 
todos os arquivos deste projeto para lá

# 2
Usando o vsCode ou editor de texto desejado abra o arquivo csv_dag.py

# 3
Após abrir pesquise por pd.read_csv e games_2019.csv, após achar certifique-se de que os caminhos indicados correspondem ao camindo 
da pasta dag em seu computador, caso esteja diferente altere.

# 4
Execute seu airflow e atualize a listagem das dags, após isso irá aparecer uma dag chamada "dag", caso não apareça 
faça a busca pelos termos: dag ou data.

# 5 
Execute a dag e aguarde o finalizamento, após finalizar na sua pasta dag será gerado um csv chamado: most_rated_games_2019.csv

# 6
Abra o arquivo e fique a vontade para analisar os dados. Também há um outro arquivo csv chamado 'duplicated.csv', 
basicamente ele possui os dados desta pipeline sem o tratamento de dados duplicados, abra-o e compare com o arquivo 
most_rated_games_2019.csv caso ache necessário.

# 7 
Caso ache conveniente gravar os dados no elasticSearch execute o arquivo insert_elastic.py via terminal desta forma:
python3 insert_elastic.py e em seguida usando o postman ou ferramenta semelhante execute o seguinte endpoint: 
http://localhost:9200/ela/_search . OBs.: certifique-se de que seu elastic está na mesma porta da url e de que o serviço 
está running.
