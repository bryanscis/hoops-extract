import logging
import csv
import re
import Levenshtein
from scripts.validate import BaseValidate
from utils import get_current_season

def find_closest_name(name_to_match):
    '''
    Returns array of closest names that can be matched to player using Levenshtein distance.
    '''
    normalized_names = BaseValidate._normalized_players.keys()
    distances = []

    for name in normalized_names:
        distance = Levenshtein.distance(name, name_to_match)
        distances.append((name, distance))
    
    distances.sort(key=lambda x: x[1])

    return distances[:5]

def transform_players():
    '''
    Transforms current players to a specific format.
    '''
    year = get_current_season()
    current_players_file_path = f'./data/{year}/{year}_all_players.csv'
    
    try:
        BaseValidate.load_players()
    except Exception as e:
        logging.error(f'Error loading players: {e}')
        return

    try:
        with open(current_players_file_path, mode='r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                player_name = re.sub(r'\W+', '', "".join(row[0])).lower()
                if player_name not in BaseValidate._normalized_players:
                    logging.info(f'{player_name} {row} not found. Using Levenshtein to match.')
                    closest_matches = find_closest_name(player_name)
                    for closest_name, distance in closest_matches:
                        logging.info(f'Closest match for {player_name}: {closest_name} with distance {distance}')
    except FileNotFoundError:
        logging.error(f'File {current_players_file_path} not found.')
    except Exception as e:
        logging.error(f'An error occurred during player transformation: {e}')

