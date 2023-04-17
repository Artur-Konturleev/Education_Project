import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

vgsales = 'https://git.lab.karpov.courses/lab/airflow/-/blob/master/dags/a.batalov/vgsales.csv'
vgsales_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a.konturlev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 4, 16)
}
    
@dag(default_args=default_args, schedule_interval = '0 12 * * *', catchup=False)
def games():
    @task(retries=3)
    def get_data():
        game_sales = requests.get(vgsales, stream=True)
        zipfile = ZipFile(BytesIO(game_sales.content))
        game_sales_data = zipfile.read(vgsales_file).decode('utf-8')
        return game_sales_data
  
    @task(retries=4, retry_delay=timedelta(10))
    def get_most_sales_game(game_sales_data):
        game_sales_data_df = pd.read_csv(game_sales_data)
        df_2007 = game_sales_data_df.query("Year == 2007")
        best_selling_game = df_2007.groupby('Name', as_index = False).agg({'Global_Sales': 'sum'}).sort_values(by = 'Global_Sales', ascending = False)
        max_count_sale = best_selling_game['Global_Sales'].max ()
        best_selling_game = best_selling_game[best_selling_game.Global_Sales == max_count_sale]
        best_selling_game = best_selling_game.Name
        return best_selling_game.to_csv(index=False, header=False)

    @task(retries=4, retry_delay=timedelta(10))
    def get_most_sales_genre(game_sales_data):
        game_sales_data_df = pd.read_csv(game_sales_data)
        df_2007 = game_sales_data_df.query("Year == 2007")
        best_selling_Genre = df_2007.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'}).sort_values(by = 'EU_Sales', ascending = False)
        max_count_sale_genre = best_selling_Genre['EU_Sales'].max ()
        best_selling_Genre = best_selling_Genre[best_selling_Genre.EU_Sales == max_count_sale_genre]
        best_selling_Genre = best_selling_Genre.Genre
        return best_selling_Genre.to_csv(index=False, header=False)    
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_most_sales_platform_NA(game_sales_data):
        game_sales_data_df = pd.read_csv(game_sales_data)
        df_2007 = game_sales_data_df.query("Year == 2007")
        platform_with_the_highest_number_of_games_sold_NA = df_2007.groupby(['Platform', 'Name'], as_index = False).agg({'NA_Sales': 'sum'})
        platform_with_the_highest_number_of_games_sold_NA = platform_with_the_highest_number_of_games_sold_NA.query("NA_Sales > 1.00").groupby('Platform', as_index = False).agg({'Name': pd.Series.nunique}).sort_values(by = 'Name', ascending = False)
        max_count_game_on_Platform = platform_with_the_highest_number_of_games_sold_NA['Name'].max ()
        platform_with_the_highest_number_of_games_sold_NA = platform_with_the_highest_number_of_games_sold_NA[platform_with_the_highest_number_of_games_sold_NA.Name == max_count_game_on_Platform]
        platform_with_the_highest_number_of_games_sold_NA = platform_with_the_highest_number_of_games_sold_NA.Platform
        return platform_with_the_highest_number_of_games_sold_NA.to_csv(index=False, header=False)  
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_most_sales_publisher_JP(game_sales_data):
        game_sales_data_df = pd.read_csv(game_sales_data)
        df_2007 = game_sales_data_df.query("Year == 2007")
        publisher_with_the_highest_average = df_2007.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'}).sort_values(by = 'JP_Sales', ascending = False)
        Publisher_with_max_avg__sale = publisher_with_the_highest_average['JP_Sales'].max ()
        publisher_with_the_highest_average = publisher_with_the_highest_average[publisher_with_the_highest_average.JP_Sales == Publisher_with_max_avg__sale]
        publisher_with_the_highest_average = publisher_with_the_highest_average.Publisher
        return publisher_with_the_highest_average.to_csv(index=False, header=False)      
    
    @task(retries=4, retry_delay=timedelta(10))
    def get_most_sales_games_EU_JP(game_sales_data):
        game_sales_data_df = pd.read_csv(game_sales_data)
        df_2007 = game_sales_data_df.query("Year == 2007")
        games_sold_better_in_Europe_than_in_Japan = df_2007.groupby('Name', as_index = False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        games_sold_better_in_Europe_than_in_Japan = df_2007[df_2007.EU_Sales > df_2007.JP_Sales ].Name.nunique()
        return {'games_sold_better_in_Europe_than_in_Japan': games_sold_better_in_Europe_than_in_Japan}  

    @task(retries=3)
    def print_data(best_selling_game, best_selling_Genre, platform_with_the_highest_number_of_games_sold_NA, publisher_with_the_highest_average, games_sold_better_in_Europe_than_in_Japan):

        context = get_current_context()
        date = context['ds']

        print(f''' best selling game this year for {date}''')

        print(f'''the best-selling game genre in Europe for {date}''')
        
        print(f'''Most sold platform in North America (> 1.0 million copies) for {date}''')
            
        print(f'''Publisher with the highest average sales in Japan for {date}''')
                
        print(f'''The number of games sold is better in Europe than in Japan for {date}''')

    game_sales_data = get_data()
    best_selling_game = get_most_sales_game(game_sales_data)
    best_selling_Genre = get_most_sales_genre(game_sales_data)
    platform_with_the_highest_number_of_games_sold_NA = get_most_sales_platform_NA(game_sales_data)
    publisher_with_the_highest_average = get_most_sales_publisher_JP(game_sales_data)
    games_sold_better_in_Europe_than_in_Japan = get_most_sales_games_EU_JP(game_sales_data)

    print_data(best_selling_game, best_selling_Genre, platform_with_the_highest_number_of_games_sold_NA, publisher_with_the_highest_average, games_sold_better_in_Europe_than_in_Japan)

games = games()