from bs4 import BeautifulSoup
import requests
import csv

nba_months =['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june']

def extract_schedule(season):
    '''
    Creates new file of NBA in CSV format of given season. 

    Params:
    - season (string): ending of NBA season
    '''
    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"}
    response =  requests.get('https://www.basketball-reference.com/leagues/NBA_2024_games-november.html', headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'schedule'})
    with open(f'./schedules/nba_{season}_schedule.csv', "a", newline="") as f:
        writer = csv.writer(f, delimiter='\t')
        for row in table.tbody.find_all('tr'):
            temp_row = []
            columns = row.find_all('td')
            for column in columns:
                temp_row.append(column.get_text())
            writer.writerow(temp_row)