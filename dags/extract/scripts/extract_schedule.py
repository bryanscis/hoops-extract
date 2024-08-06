from dags.utils import get_current_season
from scripts.nba_schedule import extract_schedule as get_schedule
from pathlib import Path
import logging

def extract_schedule():
    '''
    Extracts NBA schedule of current year to a CSV file located at '/data/{year}/schedules/nba_{year}_schedule.csv'.
    '''
    year = get_current_season()
    filename = f'./data/{year}/schedules/nba_{year}_schedule.csv'
    if not Path(filename).is_file():
        try:
            logging.info(f"Schedule file for {year} does not exist. Fetching schedule.")
            get_schedule(str(year))
            logging.info(f"Schedule for {year} successfully fetched.")
        except Exception as e:
            logging.error(f"Failed to fetch schedule for {year}: {e}")
            return
    logging.info(f"Schedule file for {year} already exists.")
    