import csv
import logging
from airflow.hooks.postgres_hook import PostgresHook
from dags.load.queries import select_season_query, select_player_id, select_team_id, insert_player_query, insert_player_team_season_query
from scripts.misc import split_name, team_realgm
from data_config import cleaned_current_players_file_path
from dags.utils import get_current_season

def load_players():
    '''
    Loads player information into PostgreSQL database after extracting and transforming.
    '''
    hook = PostgresHook(postgres_conn_id='nba_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(select_season_query, (get_current_season(),))
        season_id = cursor.fetchone()[0]
        with open(cleaned_current_players_file_path, "r") as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                first_name, last_name, suffix = split_name(row[0])
                cursor.execute(select_player_id, (first_name, last_name))
                player_id = cursor.fetchone()
                if not player_id:
                    cursor.execute(insert_player_query, (first_name, last_name, suffix, row[1], row[2], row[3], row[8], row[9], row[10]))
                    player_id = cursor.fetchone()[0]
                else:
                    player_id = player_id[0]

                teams = row[5].split(", ")
                for i, cur_team in enumerate(teams):
                    team = team_realgm.get(cur_team, cur_team)
                    cursor.execute(select_team_id, (team,))
                    team_id = cursor.fetchone()[0]
                    cursor.execute(insert_player_team_season_query, (player_id, team_id, int(row[4]), season_id, (i == 0)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f'Error loading players into database. Error: {e} on row {row}.')
        raise e
    finally:
        cursor.close()
        conn.close()