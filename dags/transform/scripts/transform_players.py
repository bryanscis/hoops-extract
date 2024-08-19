import logging
import csv
import re
import Levenshtein
import json
import heapq
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
        if len(distances) < 5:
            heapq.heappush(distances, (-distance, name))
        else:
            heapq.heappushpop(distances, (-distance, name))

    closest_matches = sorted([(-dist, name) for dist, name in distances], key=lambda x: x[0])
    for name in closest_matches[:5]:
        logging.info(f'Close match for {name_to_match}: {name[1]} with distance {name[0]}.')

    return closest_matches

def transform_players():
    '''
    Transforms current players to specific format.
    '''
    year = get_current_season()
    current_players_file_path = f'./data/{year}/{year}_all_players.csv'
    cleaned_names_file_path = f'./data/{year}/cleaned_{year}_matched_players.json'
    try:
        with open(cleaned_names_file_path, mode='r') as json_file:
            matched_players = json.load(json_file)
    except FileNotFoundError:
        matched_players = {}
    unmatched_players = {}

    BaseValidate.load_players()

    with open(current_players_file_path, mode='r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            player_name = re.sub(r'\W+', '', "".join(row[0])).lower()
            if player_name in matched_players:
                logging.info(f'{player_name} already matched as {matched_players[player_name]}. Skipping.')
                continue
            if player_name not in BaseValidate._normalized_players:
                logging.info(f'{player_name} not found. Using Levenshtein to match.')
                closest_matches = find_closest_name(player_name)
                best_match = closest_matches[0][1]
                logging.info(f'Closest match for {player_name}: {best_match} with distance {closest_matches[0][0]}.')
                
                unmatched_players[player_name] = best_match

    if unmatched_players:
        matched_players.update(unmatched_players)
        with open(cleaned_names_file_path, mode='w') as json_file:
            json.dump(matched_players, json_file, indent=4)
        logging.info(f'Unmatched players written/updated to {cleaned_names_file_path}. Double check file to ensure proper names.')
    else:
        logging.info(f'No new unmatched players found. {cleaned_names_file_path} remains unchanged.')
