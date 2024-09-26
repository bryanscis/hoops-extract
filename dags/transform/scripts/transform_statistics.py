import logging
import pandas as pd
from data_config import statistics_file_path, cleaned_statistics_file_path

def transform_statistics():
    ''' 
    Transforms statistics file into cleaned format. Triggered after extracting new box scores.
    '''
    df = pd.read_csv(statistics_file_path, delimiter='\t', header=None, 
                     names=['home_team', 'away_team', 'game_date', 'start_time', 
                            'season_year', 'attendance', 'home_team_score', 
                            'away_team_score', 'duration', 'stage'])

    df['game_date'] = pd.to_datetime(df['game_date'], format='%a, %b %d, %Y', errors='coerce')
    df['game_date'] = df['game_date'].dt.strftime('%Y-%m-%d')

    df['attendance'] = df['attendance'].replace(',', '').astype(float, errors='ignore')
    df['attendance'].fillna(0, inplace=True)
    df['attendance'] = df['attendance'].astype(int)

    df['home_team_score'] = pd.to_numeric(df['home_team_score'], errors='coerce').astype(int)
    df['away_team_score'] = pd.to_numeric(df['away_team_score'], errors='coerce').astype(int) 

    df['duration'].fillna('0:00', inplace=True)
    df['stage'].fillna('unknown', inplace=True)

    if df.isnull().values.any():
        invalid_rows = df[df.isnull().any(axis=1)]
        for index, row in invalid_rows.iterrows():
            logging.info(f"Invalid row at index {index}: {row.to_dict()}")
        raise ValueError(f"{statistics_file_path} contains invalid values.")
    
    df.to_csv(cleaned_statistics_file_path, sep='\t', index=False, header=False)