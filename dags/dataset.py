from airflow.datasets import Dataset
from utils import get_current_season

schedule_dataset = Dataset(f"/opt/airflow/data/{get_current_season}/schedules/nba_{get_current_season}_schedule.csv")
box_scores_dataset = Dataset(f'/opt/airflow/data/{get_current_season}/games/')
statistics_dataset = Dataset(f'/opt/airflow/data/{get_current_season}/nba_{get_current_season}_statistics.csv/')
all_players_dataset = Dataset(f'opt/airflow/data/all_players.json')
current_players_dataset = Dataset(f'/opt/airflow/data/{get_current_season}/{get_current_season}_all_players.csv/')