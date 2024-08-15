import logging
import csv
import re
from scripts.validate import BaseValidate
from utils import get_current_season

def transform_players():
    '''
    Transforms current players to specific format.
    '''
    year = get_current_season()
    current_players_file_path = f'./data/{year}/{year}_all_players.csv'
    BaseValidate.load_players()
    with open(current_players_file_path, mode='r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            player_name = re.sub(r'\W+', '', "".join(row[0])).lower()
            if player_name not in BaseValidate._normalized_players:
                logging.info(f'{player_name} {row} not found.')