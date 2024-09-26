import csv
import logging
from airflow.hooks.postgres_hook import PostgresHook
from data_config import cleaned_statistics_file_path
from dags.load.queries import select_game_id, select_team_id, update_game
from utils import get_current_season

def load_statistics():
    '''
    Loads statistics of game into database.
    '''
    hook = PostgresHook(postgres_conn_id='nba_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        with open(cleaned_statistics_file_path, 'r') as rf:
            reader = csv.reader(rf, delimiter='\t')
            season = get_current_season()
            for row in reader:
                cursor.execute(select_team_id, (row[0],))
                home_team_id = cursor.fetchone()[0]
                cursor.execute(select_team_id, (row[1],))
                away_team_id = cursor.fetchone()[0]
                cursor.execute(select_game_id, (home_team_id, away_team_id, row[2], season))
                game_id = cursor.fetchone()[0]
                cursor.execute(update_game, (row[6], row[7], row[5], row[8], row[9], game_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f'Error loading game statistics into database. Error: {e}.')
        raise e
    finally:
        cursor.close()
        conn.close()