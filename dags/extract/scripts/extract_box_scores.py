import csv
import logging
import pandas as pd
from utils import get_current_season
from pathlib import Path
from datetime import datetime
from scripts.game import Game
from scripts.misc import get_team_abbreviation


def extract_box_scores():
    ''''
    Extracts box scores of current season to './data/{year}/schedules/nba_{year}_schedule.csv'. 
    '''
    year = get_current_season()
    schedule_file = f'/opt/airflow/data/{year}/schedules/nba_{year}_schedule.csv'
    
    if not Path(schedule_file).is_file():
        raise FileNotFoundError(f"Schedule file {schedule_file} not found")
    logging.info(f'Schedule file {schedule_file} found.')

    with open(schedule_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            game_date = datetime.strptime(row[0], '%a, %b %d, %Y')
            if game_date.date() < datetime.now().date():
                home_team, away_team = get_team_abbreviation(row[4]), get_team_abbreviation(row[2])
                game_file = f'./data/{year}/games/{game_date.strftime("%Y%m%d")}{home_team}{away_team}.csv'
                try:
                    if not Path(game_file).is_file():
                        logging.info(f'Creating game file {home_team} vs {away_team} on {game_date.date()}')
                        current_game = Game(home_team, away_team, row[0], row[1], year)
                        total_box_df =  pd.concat([current_game.home_box(),current_game.away_box()], ignore_index=True)
                        total_box_df.to_csv(f'{game_file}', sep='\t', index=False, header=False)
                        logging.info(f'Game file {home_team} vs {away_team} on {game_date.date()} has been created at "{game_file}".')
                except Exception as e:
                    logging.info(f'Error occured while getting game on {game_date.date()} for {home_team} vs {away_team}: {e}')