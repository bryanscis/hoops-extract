import logging
import pandas as pd
from data_config import cleaned_games_directory
from pathlib import Path

def transform_box_score(**kwargs):
    '''
    Transforms box scores into cleaned format. Triggered after extracting new box scores.
    '''
    file_paths = kwargs['dag_run'].conf.get('new_games', [])
    logging.info(f'New file paths: {file_paths}.')
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
            df.iloc[:, 1] = pd.to_timedelta(df.iloc[:, 1])
            df.iloc[:, 1] = df.iloc[:, 1].apply(lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}")
            for col in df.columns[2:]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df.to_csv(cleaned_game_path, sep='\t', index=False, header=None)
            logging.info(f'Box score successfully cleaned at {cleaned_game_path}.')
        except Exception as e:
            logging.error(f'Error processing file {file}: {e}')
            raise