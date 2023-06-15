import pandas as pd

#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dataFrameVgsales=pd.read_csv('/opt/airflow/dags/vgsales.csv')
dataFrame1980a2023=pd.read_csv('/opt/airflow/dags/1980a2023.csv')


# initializing the default arguments
default_args = {
		'owner': 'airflow',
        'depends_on_past': False,
		'start_date': datetime(2023, 6, 2),
		'retries': 1,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
generetor_csv_dag = DAG('dag',
		default_args=default_args,
		description='DAG',
		schedule_interval='@daily', 
		catchup=False,
		tags=['dag, data']
)

# python callable function
def generate_new_csv():
		vgsalesDataFrameFormated = formatingDataFrameVgsales()
		_1980a2023DataFrameFormated = formatingDataFrame1980a2023()


		#MERGING DATAFRAMES
		frames = [vgsalesDataFrameFormated, _1980a2023DataFrameFormated]

		result = pd.concat(frames)
		result.sort_values('score', ascending=False, inplace=True)

		#DROP DUPLICATED ROWS
		result.drop_duplicates(inplace=True)
		result.reset_index(drop=True, inplace=True)
		result.to_csv('/opt/airflow/dags/most_rated_games_2019.csv')

def formatingDataFrameVgsales():
       
       filteredDataFrame = dataFrameVgsales[(dataFrameVgsales['Year']==2019)].drop(columns=['ESRB_Rating', 'Platform', 'Publisher', 
       'User_Score','Total_Shipped', 'Global_Sales', 'NA_Sales', 'PAL_Sales', 'JP_Sales','Other_Sales', 'Rank'])

       #DROPING NULL VALUES
       filteredDataFrame.dropna(subset = ['Critic_Score'], inplace=True)

       filteredDataFrame.columns=[x.lower() for x in filteredDataFrame.columns]
       filteredDataFrame.rename(columns = {'name':'game', 'critic_score':'score'}, inplace = True)
       filteredDataFrame['year'] = filteredDataFrame['year'].astype(int)

       filteredDataFrame = reorderColumnsDataFrame(filteredDataFrame)
       
       return filteredDataFrame

def formatingDataFrame1980a2023():
       
       filteredDataFrame = dataFrame1980a2023.drop(columns=['Times Listed',
       'Number of Reviews', 'Summary', 'Reviews', 'Plays', 'Playing',
       'Backlogs', 'Wishlist'])

       filteredDataFrame.columns=[x.lower() for x in filteredDataFrame.columns]
       filteredDataFrame.rename(columns = {'title':'game', 'release date':'year', 'genres':'genre', 
                                           'team':'developer', 'rating':'score'}, inplace = True)
       
       filteredDataFrame = getYearFromStringData(filteredDataFrame)
       
       filteredDataFrame = reorderColumnsDataFrame(filteredDataFrame)

       #DUPLICATING SCORE FROM COLOUMN
       #NOTE: THIS IS NECCESSERY BECAUSE THE MAX. SCORE IS 5 AND IN THE OTHER DATAFRAME IS 10
       filteredDataFrame['score'] = filteredDataFrame['score'] * 2

       return filteredDataFrame


def reorderColumnsDataFrame(dataFrame):
       #REORDING COLUMNS
       dataFrame = dataFrame.reindex(columns=['score', 'game', 'genre', 'developer', 'year'])
       return dataFrame


def getYearFromStringData(dataFrame):
       dataFrame['year'] = dataFrame['year'].str[-4:]
       dataFrame = dataFrame[(dataFrame['year']=='2019')]
       dataFrame['year'] = dataFrame['year'].astype(int)
       return dataFrame



start_task = DummyOperator(task_id='start_task', dag=generetor_csv_dag)

csv_task = PythonOperator(task_id='csv_task', python_callable=generate_new_csv, dag=generetor_csv_dag)


end_task = DummyOperator(task_id='end_task', dag=generetor_csv_dag)


start_task >> csv_task >> end_task