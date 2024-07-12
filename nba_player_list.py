from validate import BaseValidate
from bs4 import BeautifulSoup
import csv
from validate import BaseValidate, ValidationError

def extract_all_players(season):
    validator = BaseValidate()
    validator.validate(season=season)
    content = validator.fetch_content(f'https://basketball.realgm.com/nba/players/2024')
    soup = BeautifulSoup(content, 'html.parser')
    table_id = soup.findAll('table')
    if len(table_id) != 1:
        raise ValidationError(f'More than one table found. Error with {season} player data.')
    table = soup.find('table', attrs={'id': table_id[0]})
    with open(f'./data  /{season}_all_players.csv', 'a', newline="") as f:
        writer = csv.writer(f, delimiter='\t')
        for row in table.tbody.find_all('tr'):
                temp_row = []
                for column in row.find_all('td'):
                     temp_row.append(column.text)
                writer.writerow(temp_row)