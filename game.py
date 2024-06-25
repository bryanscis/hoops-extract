import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from validate import GameValidate

class Game:
    def __init__(self, home_team, away_team, date ,season):
        self.cols = ['player', 'mp', 'fg', 'fga', 'fg%', '3p', '3pa', '3p%', 'ft', 'fta', 'ft%', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']
        self.home_team, self.away_team, self.date , self.season = home_team, away_team, date, season
        self.home_df, self.away_df = self.scrape_game(home_team, away_team, date)

    def scrape_game(self, home_team, away_team, date):
        content = GameValidate().validate(self.home_team, self.away_team, self.date)
        soup = BeautifulSoup(content, 'html.parser')
        away_table, home_table = soup.find('table', attrs={'id': f'box-{self.away_team}-game-basic'}), soup.find('table', attrs={'id': f'box-{self.home_team}-game-basic'})
        return self.create_df(home_table), self.create_df(away_table)

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