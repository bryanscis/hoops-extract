import logging
from dags.utils import get_current_season
from data_config import schedule_file_path
from pathlib import Path
from scripts.nba_schedule import extract_schedule as get_schedule

def extract_schedule():
    '''
    Extracts NBA schedule of current year to a CSV file located at '/data/{year}/schedules/nba_{year}_schedule.csv'.
    '''
    year = get_current_season()
    if not Path(schedule_file_path).is_file():
        try:
            logging.info(f"Schedule file for {year} does not exist. Fetching schedule.")
            get_schedule(str(year))
            logging.info(f"Schedule for {year} successfully fetched.")
        except Exception as e:
            logging.error(f"Failed to fetch schedule for {year}: {e}")
            raise e
    logging.info(f"Schedule file for {year} already exists.")
    