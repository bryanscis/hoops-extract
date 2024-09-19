import logging
import pandas as pd
from data_config import cleaned_games_directory
from pathlib import Path
from unidecode import unidecode
from scripts.validate import BaseValidate

def transform_box_score(**kwargs):
    '''
    Transforms box scores into cleaned format. Triggered after extracting new box scores.
    '''
    file_paths = kwargs['dag_run'].conf.get('new_games', [])
    new_file_paths = []
    logging.info(f'New file paths: {file_paths}.')
    BaseValidate.load_players()
    player_names = set(BaseValidate._players.keys())
    for file in file_paths:
        try:
            cleaned_game_path = cleaned_games_directory + file.rsplit('/', 1)[1]
            if Path(cleaned_game_path).is_file():
                logging.info(f'{cleaned_game_path} has already been transformed. Skipping.')
                continue
            logging.info(f'Transforming {file}.')
            df = pd.read_csv(file, delimiter='\t',index_col=False, header=None)
            if df.shape[1] != 22:
                raise ValueError(f'Incorrect number of columns in file: {file}. Expected 22, got {df.shape[1]}.')
            df.iloc[:, 0] = df.iloc[:, 0].apply(unidecode)
            invalid_names = ~df.iloc[:, 0].isin(player_names)
            if invalid_names.any():
                invalid_players = df.loc[invalid_names, df.columns[0]].unique()
                raise ValueError(f"Player names not found in player_names: {', '.join(invalid_players)}")
            df.iloc[:, 1] = pd.to_timedelta(df.iloc[:, 1])
            df.iloc[:, 1] = df.iloc[:, 1].apply(lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}")
            df[df.columns[2:]] = df[df.columns[2:]].apply(pd.to_numeric)
            df.to_csv(cleaned_game_path, sep='\t', index=False, header=None)
            new_file_paths.append(cleaned_game_path)
            logging.info(f'Box score successfully cleaned at {cleaned_game_path}.')
        except Exception as e:
            logging.error(f'Error processing file {file}: {e}')
            raise
    return {'new_games': new_file_paths}