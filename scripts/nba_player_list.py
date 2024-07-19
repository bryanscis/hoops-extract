from bs4 import BeautifulSoup
import csv
from .validate import BaseValidate, ValidationError

def extract_all_players(season):
    validator = BaseValidate()
    validator.validate(season=season)
    content = validator.fetch_content(f'https://basketball.realgm.com/nba/players/{season}')
    soup = BeautifulSoup(content, 'html.parser')
    table = soup.find('table', attrs={'class': 'tablesaw'})
    with open(f'./data/{season}/{season}_all_players.csv', 'a', newline="") as f:
        writer = csv.writer(f, delimiter='\t')
        for row in table.tbody.find_all('tr'):
            temp_row = []
            for column in row.find_all('td'):
                nationality = None
                if column.get('data-th') == 'Nationality':
                    nationality = column.find('a').get_text()
                temp_row.append(column.text if not nationality else nationality )
            writer.writerow(temp_row)