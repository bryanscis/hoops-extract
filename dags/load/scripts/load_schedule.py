import csv
import logging
from airflow.hooks.postgres_hook import PostgresHook
from data_config import cleaned_schedule_file_path
from dags.utils import get_current_season
from dags.load.queries import select_team_name_id, select_all_games_query, insert_new_game_query, update_game_query
from datetime import datetime

def load_new_schedule():
    """
    Returns schedule in set format.
    """
    new_schedule = set()
    with open(cleaned_schedule_file_path, 'r') as rf:
        reader = csv.reader(rf, delimiter='\t')
        for row in reader:
            game_date, start_time, home_team, away_team = row[0], row[1], row[3], row[2]
            new_schedule.add((datetime.strptime(game_date, '%Y-%m-%d').date(), start_time, home_team, away_team))
    return new_schedule

def load_schedule():
    '''
    Loads schedule information into PostgreSQL database after extracting and transforming.
    '''
    current_season= get_current_season()
    hook = PostgresHook(postgres_conn_id='nba_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        new_schedule = load_new_schedule()
        cursor.execute(select_all_games_query, (current_season,))
        db_games = cursor.fetchall()
        db_schedule = {(game[4], game[1], game[2], game[3]): game[0] for game in db_games}

        missing_db_games = {key: db_schedule[key] for key in db_schedule.keys() - new_schedule}
        logging.info(f"Games in database not in new schedule: {missing_db_games}.")
        missing_games_teams = {(game[2], game[3]): id for game, id in missing_db_games.items()}

        new_games = new_schedule - db_schedule.keys()
        logging.info(f"Games in new schedule not accounted for in database: {new_games}")
        
        for game in new_games:
            game_date, start_time, home_team, away_team = game
            cursor.execute(select_team_name_id, (home_team,))
            home_team_id = cursor.fetchone()[0]
            cursor.execute(select_team_name_id, (away_team,))
            away_team_id = cursor.fetchone()[0]

            # Rescheduled game in database needs update
            if (home_team, away_team) in missing_games_teams:
                db_game_id = missing_games_teams[(home_team, away_team)]
                logging.info(f"Game not accounted for in database for {home_team, away_team} at id: {db_game_id}. Updating game_id at {db_game_id} with game date {game_date} and start time {start_time}.")
                cursor.execute(update_game_query, (game_date, start_time, db_game_id))
            # Game is new and needs to be inserted
            else:
                logging.info(f"New game to be added for {home_team, away_team} on {game_date, start_time}")
                cursor.execute(insert_new_game_query, (home_team_id, away_team_id, game_date, start_time, current_season))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()