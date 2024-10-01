import logging
import pandas as pd
from data_config import schedule_file_path, cleaned_schedule_file_path

def transform_schedule():
    """
    Transforms schedule into cleaned format. Triggered after extracting schedule.
    """
    df = pd.read_csv(schedule_file_path, delimiter='\t', header=None, 
                     names=['game_date', 'start_time', 'away_team', 'away_team_score', 'home_team', 'home_team_score', 'bs_placeholder', 'ot', 'attendance', 
                            'duration', 'arena', 'notes'])
    
    df['game_date'] = pd.to_datetime(df['game_date'], format='%a, %b %d, %Y', errors='coerce')
    df['game_date'] = df['game_date'].dt.strftime('%Y-%m-%d')

    df = df.drop(columns=['away_team_score', 'home_team_score', 'bs_placeholder','ot', 'attendance', 'duration'])

    df.to_csv(cleaned_schedule_file_path, sep='\t', index=False, header=False)

    logging.info('Transformation of schedule file complete.')