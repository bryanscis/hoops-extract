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

def transform_players(**kwargs):
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
        logging.info(f'{cleaned_names_file_path} not found.')
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
        kwargs['ti'].xcom_push(key='unmatched_players', value=unmatched_players)
    else:
        logging.info(f'No new unmatched players found. {cleaned_names_file_path} remains unchanged.')
        kwargs['ti'].xcom_push(key='unmatched_players', value=unmatched_players)

def update_current_players_file():
    '''
    Updates the original player file with cleaned player names after transformations.
    '''
    year = get_current_season()
    current_players_file_path = f'./data/{year}/{year}_all_players.csv'
    cleaned_names_file_path = f'./data/{year}/cleaned_{year}_matched_players.json'
    try:
        with open(cleaned_names_file_path, mode='r') as json_file:
            cleaned_players = json.load(json_file)
    except FileNotFoundError:
        logging.warning(f"{cleaned_names_file_path} not found. Exiting.")
        return
    BaseValidate.load_players()
    updated_rows = []
    with open(current_players_file_path, mode='r') as player_file:
        reader = csv.reader(player_file, delimiter='\t')
        for row in reader:
            player_name = re.sub(r'\W+', '', "".join(row[0])).lower()
            if player_name in cleaned_players:
                updated_name = BaseValidate._normalized_players[cleaned_players[player_name]]
                row[0] = updated_name
                logging.info(f"Updated player name: {player_name} -> {updated_name}")
            else:
                row[0] =  BaseValidate._normalized_players[player_name]
            updated_rows.append(row)
    with open(current_players_file_path, mode='w', newline='') as player_file:
        writer = csv.writer(player_file, delimiter='\t')
        writer.writerows(updated_rows)
    logging.info(f"Player file updated successfully. Output written to {current_players_file_path}.")

def check_player_changes(**kwargs):
    '''
    Decides branch 'wait_for_verification' if cleaned_players has been updated or proceed to 'end_log' instead.

    Returns:
    - 'wait_for_verification': if there cleaned_players has been updated
    - 'end_log'              : if there is no update of cleaned_players

    '''
    unmatched_players = kwargs['ti'].xcom_pull(task_ids='transform_current_players', key='unmatched_players')
    if unmatched_players:
        logging.info("New players have been added. Please verify the new player names in the cleaned_names_file_path before proceeding.")
        return 'wait_for_verification'
    else:
        logging.info("No new players added. Proceeding to end_log.")
        return 'end_log'

def complete_verification():
    logging.info("Verification has been completed manually.")