from airflow.datasets import Dataset
from utils import get_current_season

year = get_current_season()

schedule_dataset = Dataset(f"/opt/airflow/data/{year}/schedules/nba_{year}_schedule.csv")
box_scores_dataset = Dataset(f'/opt/airflow/data/{year}/games/')
statistics_dataset = Dataset(f'/opt/airflow/data/{year}/nba_{year}_statistics.csv/')
all_players_dataset = Dataset(f'opt/airflow/data/all_players.json')
current_players_dataset = Dataset(f'/opt/airflow/data/{year}/{year}_all_players.csv/')