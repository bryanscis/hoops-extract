import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from validate import GameValidate

class Game:
    stages = {'regular': 'regular', 'in-season': 'ist', 'play-in': 'play_in' ,'first round': 'first_round', 'semifinals': 'conf_semi', 'conference finals': 'conf_final', 'finals': 'final'}

    def __init__(self, home_team, away_team, date, start_time, season):
        self.cols = ['player', 'mp', 'fg', 'fga', 'fg%', '3p', '3pa', '3p%', 'ft', 'fta', 'ft%', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']
        self.home_team, self.away_team, self.date, self.start_time, self.season = home_team, away_team, date, start_time, season
        self.attendance, self.home_score, self.away_score, self.game_time, self.stage= None, None, None, None, None
        self.home_df, self.away_df = self.scrape_game(home_team, away_team, date)

    def scrape_game(self, home_team, away_team, date):
        '''
        Scrape game data for specified teams and date. Returns dataframe of both home and away team.

        Params:
        - home_team (str): The name of the home team.
        - away_team (str): The name of the away team.
        - date      (str): The date of the game.
        '''
        content = GameValidate().validate(self.home_team, self.away_team, self.date)
        soup = BeautifulSoup(content, 'html.parser')
        away_table, home_table = soup.find('table', attrs={'id': f'box-{self.away_team}-game-basic'}), soup.find('table', attrs={'id': f'box-{self.home_team}-game-basic'})
        self.home_score = home_table.tfoot.find('td', {'data-stat': 'pts'}).text
        self.away_score = away_table.tfoot.find('td', {'data-stat': 'pts'}).text
        self.stage = self.extract_stage(soup.find('div', {'id': 'content', 'role': 'main', 'class': 'box'}))
        self.attendance, self.game_time = self.extract_details(soup.find_all('div'))
        return self.create_df(home_table), self.create_df(away_table)
    
    def extract_stage(self, content):
        '''
        Extracts type of game (ie. semifinals, finals etc..) from provided content.

        Params:
        - content (bs4 element): BeautifulSoup object from HTML content
        '''
        if content:
            h1_text = content.find('h1').get_text().lower()
            if h1_text:
                for key, value in self.stages.items():
                    if key in h1_text:
                        return value
            if content.find('div', string='In-Season Tournament'):
                return self.stages['in-season']
        return self.stages['regular']

    def extract_details(self, content):
        '''
        Extracts miscellaneous statistics of games from provided content.

        Params:
        - content (bs4 element): BeautifulSoup object from HTML content
        '''
        attendance, game_time = None, None
        for div in content:
            strong = div.find('strong')
            if strong: 
                if 'Attendance:' in strong.get_text():
                    attendance = div.get_text().replace('Attendance:', '').strip()
                elif 'Time of Game:' in strong.get_text():
                    game_time = div.get_text().replace('Time of Game:', '').strip()
            if attendance and game_time:
                return attendance, game_time
        return attendance, game_time

    def create_df(self, table):
        '''
        Creates dataframe for game information given table and returns transformed Pandas dataframe.

        Params:
        - table (bs4 element): table provided to create dataframe
        '''
        rows = []
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            if row.find('th').get_text() == 'Reserves':
                continue
            cur = [row.find('th').get_text()]
            for column in columns:
                stat, value = column['data-stat'], column.get_text()
                if stat in ['fg', 'fga', 'fg%', '3p', '3pa', 'ft', 'fta', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']:
                    value = float(value) if value else np.nan
                elif stat == 'mp' and value:
                    value = '00:' + value
                cur.append(value)
            rows.append(cur)
        df = pd.DataFrame(rows, columns=self.cols)
        df.replace(r'^\s*$', None, regex=True, inplace = True)
        df = df.astype(dtype = {'player': "str", 'mp': "str", 'fg': "float", 'fga': "float", 'fg%': "float", '3p': "float", '3pa': "float", '3p%': "float", 
                                'ft': "float", 'fta': "float", 'ft%': "float", 'orb': "float", 'drb': "float", 'trb': "float", 'ast': "float", 'stl': "float",
                                'blk': "float", 'tov': "float", 'pf': "float", 'pts': "float", 'plus_minus':"float" })
        return df
    
    def home_box(self):
        '''
        Returns box score of home team in form of Pandas dataframe.
        '''
        return self.home_df
    
    def away_box(self):
        '''
        Returns box score of away team in form of Pandas dataframe.
        '''
        return self.away_df
    
    def get_statistics(self):
        '''
        Returns dictionary of game statistics.
        '''
        statistics_keys = [
            'home_team', 'away_team', 'date', 'start_time', 'season', 
            'attendance', 'home_score', 'away_score', 
            'game_time', 'stage'
        ]
        statistics_values = [
            self.home_team, self.away_team, self.date, self.start_time, self.season, 
            self.attendance, self.home_score, self.away_score, 
            self.game_time, self.stage
        ]
        return {key: value for key, value in zip(statistics_keys, statistics_values)}