from bs4 import BeautifulSoup
import csv
from validate import BaseValidate, ValidationError
from misc import get_team_abbreviation

def extract_schedule(season):
    '''
    Creates new file of NBA schedule in CSV format of given season. 

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
        with open(f'./data/{season}/nba_{season}_schedule.csv', "a", newline="") as f:
            writer = csv.writer(f, delimiter='\t')
            for row in table.tbody.find_all('tr'):
                temp_row = [row.find('th').text]
                columns = row.find_all('td')
                for column in columns:
                    temp_row.append(column.get_text())
                writer.writerow(temp_row)
        
def extract_roster_schedule(abb, season):
    '''
    Creates new file of NBA schedule in CSV format of given season and team abbreviation.

    Params:
    - abb    (string): team abbreviation
    - season (string): ending of NBA season
    '''
    validator = BaseValidate()
    validator.validate(season=season)
    content = validator.fetch_content(f'https://www.basketball-reference.com/teams/{abb.upper()}/{season}_games.html')
    soup = BeautifulSoup(content, 'html.parser')

    # Used to analyze both playoff table and regular table
    def analyze_table(table, writer):
        for row in table.tbody.find_all('tr'):
            if not row.get('class') == None:
                continue
            data = {}
            for col in row.find_all('td'):
                if col['data-stat'] in ['date_game', 'game_start_time', 'game_location', 'opp_name', 'pts', 'opp_pts', 'attendance', 'game_duration']:
                    data[col['data-stat']] = get_team_abbreviation(col.get_text()) if col['data-stat'] == 'opp_name' else col.get_text()
            home_team, away_team = (data['opp_name'], abb.upper()) if data['game_location'] == '@' else (abb.upper(), data['opp_name'])
            home_team_score, away_team_score = (data['opp_pts'], data['pts']) if data['game_location'] == '@' else (data['pts'], data['opp_pts'])
            writer.writerow([home_team, away_team, data['date_game'], data['game_start_time'], season, 
                             int(data.get('attendance', '0').replace(',', '')) if data.get('attendance') else 0, home_team_score, away_team_score, data['game_duration']])

    with open(f'./data/{season}/schedules/{abb}_all_games.csv', 'a', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        regular_table = soup.find('table', attrs={'id': 'games'})
        try:
            analyze_table(regular_table, writer)
        except Exception as e:
            raise ValidationError(f'Cannot find regular games table for {abb} in {season}.')
        playoff_table = soup.find('table', {'id': 'games_playoffs'})
        if playoff_table:
            analyze_table(playoff_table, writer)