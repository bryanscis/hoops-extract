import csv
import logging
from airflow.hooks.postgres_hook import PostgresHook
from data_config import cleaned_schedule_file_path
from dags.utils import get_current_season
from dags.load.queries import select_team_name_id, insert_games_query

def load_schedule():
    '''
    Loads schedule information into PostgreSQL database after extracting and transforming.
    '''
    current_season= get_current_season()
    hook = PostgresHook(postgres_conn_id='nba_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        with open(cleaned_schedule_file_path, 'r') as rf:
            reader = csv.reader(rf, delimiter='\t')
            for row in reader:
                game_date, start_time, home_team, away_team = row[0], row[1], row[3], row[2]
                cursor.execute(select_team_name_id, (home_team,))
                home_team_id = cursor.fetchone()[0]
                cursor.execute(select_team_name_id, (away_team,))
                away_team_id = cursor.fetchone()[0]
                cursor.execute(insert_games_query, (home_team_id, away_team_id, game_date, start_time, current_season,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()