from bs4 import BeautifulSoup
import requests
import csv
from validate import BaseValidate

def extract_schedule(season):
    '''
    Creates new file of NBA in CSV format of given season. 

    Params:
    - season (string): ending of NBA season
    '''
    validator = BaseValidate()
    validator.validate(season=season)
    content = validator.fetch_content(f'https://www.basketball-reference.com/leagues/NBA_{season}_games.html')
    soup = BeautifulSoup(content, 'html.parser')
    nba_months = []
    for month_content in soup.find('div', {'class': 'filter'}).find_all('a'):
        nba_months.append(month_content.text)
    for month in nba_months:
        content = validator.fetch_content(f'https://www.basketball-reference.com/leagues/NBA_{season}_games-{month.lower()}.html')
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.find('table', {'id': 'schedule'})
        with open(f'./schedules/nba_{season}_schedule.csv', "a", newline="") as f:
            writer = csv.writer(f, delimiter='\t')
            for row in table.tbody.find_all('tr'):
                temp_row = [row.find('th').text]
                columns = row.find_all('td')
                for column in columns:
                    temp_row.append(column.get_text())
                writer.writerow(temp_row)