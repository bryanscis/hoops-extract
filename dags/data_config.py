from airflow.datasets import Dataset
from utils import get_current_season

year = get_current_season()

all_players_file_path = f'/opt/airflow/data/all_players.json'
all_current_players_file_path = f'/opt/airflow/data/{year}/{year}_all_players.csv'
box_scores_file_path = f'/opt/airflow/data/{year}/games/'
cleaned_current_players_file_path = f'/opt/airflow/data/{year}/cleaned_{year}_all_players.csv'
cleaned_names_file_path = f'/opt/airflow/data/{year}/cleaned_{year}_matched_players.json'
schedule_file_path = f'/opt/airflow/data/{year}/nba_{year}_schedule.csv'
statistics_file_path = f'/opt/airflow/data/{year}/nba_{year}_statistics.csv'

all_players_dataset = Dataset(all_players_file_path)
all_current_players_dataset = Dataset(all_current_players_file_path)
box_scores_dataset = Dataset(box_scores_file_path)
cleaned_current_players_dataset = Dataset(cleaned_current_players_file_path)
schedule_dataset = Dataset(schedule_file_path)
statistics_dataset = Dataset(statistics_file_path)