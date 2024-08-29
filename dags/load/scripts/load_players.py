from database.connect import DatabaseConnection
from scripts.misc import split_name, team_realgm
from airflow.hooks.postgres_hook import PostgresHook
import csv
import logging

def load_players():
    '''
    Loads player information into PostgreSQL database after extracting and transforming.
    '''
    player_file = './data/2024/2024_all_players.csv'
    select_season_query = ("""
        SELECT season_id FROM season WHERE season_year = %s;
    """)
    select_player_id = ("SELECT player_id FROM player WHERE first_name = %s AND last_name = %s;")
    select_team_id = ("SELECT team_id FROM team WHERE abbreviation = %s")
    insert_player_query = ("""
        INSERT INTO player (first_name, last_name, suffix, position, height, weight, pre_draft_team, draft_pick, nationality)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (first_name, last_name, suffix, draft_pick, nationality)
        DO UPDATE SET position = EXCLUDED.position, 
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    pre_draft_team = EXCLUDED.pre_draft_team
        RETURNING player_id;
    """)
    insert_player_team_season_query = ("""
        INSERT INTO player_team_season (player_id, team_id, age, season_id, current_team)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_id, team_id, season_id)
        DO UPDATE SET age = EXCLUDED.age,
                    current_team = EXCLUDED.current_team
        WHERE player_team_season.current_team = TRUE OR EXCLUDED.current_team = TRUE;
    """)
    hook = PostgresHook(postgres_conn_id='nba_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        with open(player_file, "r") as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                first_name, last_name, suffix = split_name(row[0])
                cursor.execute(select_player_id, (first_name, last_name))
                player_id = cursor.fetchone()
                cursor.execute(select_season_query, (2024,))
                season_id = cursor.fetchone()[0]
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
        raise
    finally:
        cursor.close()
        conn.close()