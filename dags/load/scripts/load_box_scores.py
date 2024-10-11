import csv
import logging
from airflow.hooks.postgres_hook import PostgresHook
from dags.load.queries import select_season_query ,select_game_id, select_team_id, select_player_team, insert_player_stats
from pathlib import Path
from scripts.misc import split_name
from utils import get_current_season

def load_box_scores(**kwargs):
    '''
    Loads box scores into PostgreSQL database after extraction and transformation.
    '''
    file_paths = kwargs['dag_run'].conf.get('new_games', [])
    hook = PostgresHook(postgres_conn_id='nba_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(select_season_query, (get_current_season(),))
        season_id = cursor.fetchone()[0]
        for file_name in file_paths:
            file = Path(file_name).stem
            date, home_team, away_team = str(file[:8]), str(file[8:11]), str(file[11:])
            cursor.execute(select_team_id, (home_team,))
            home_team_id = cursor.fetchone()[0]
            cursor.execute(select_team_id, (away_team,))
            away_team_id = cursor.fetchone()[0]
            cursor.execute(select_game_id, (home_team_id, away_team_id, date, season_id))
            game_id = cursor.fetchone()[0]
            with open(file_name, 'r') as rf:
                reader = csv.reader(rf, delimiter='\t')
                for row in reader:
                    try:
                        first_name, last_name, suffix = split_name(row[0])
                        cursor.execute(select_player_team, (first_name, last_name, suffix, [home_team_id, away_team_id]))
                        current_player_id = cursor.fetchone()[0]
                        cursor.execute(insert_player_stats, (game_id, current_player_id, row[1], int(float(row[2])), int(float(row[3])), int(float(row[5])), 
                                                            int(float(row[6])), int(float(row[8])), int(float(row[9])), int(float(row[11])), int(float(row[12])), 
                                                            int(float(row[13])), int(float(row[14])), int(float(row[15])), int(float(row[16])), int(float(row[17])), 
                                                            int(float(row[18])), int(float(row[19])), int(float(row[21])), True if row[1] == '00:00:00' else False ))
                    except Exception as e:
                        logging.error(f"Error processing {row} in {file_name}: {e}")
                        raise e
        conn.commit()
    except Exception as e:
        logging.error(f"Error {e} on file: {file_name}.")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()