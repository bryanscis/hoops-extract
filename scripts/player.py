from bs4 import BeautifulSoup, Comment
import pandas as pd
import numpy as np
from datetime import datetime
import csv
from .validate import PlayerValidate, ValidationError

class Player:
    def __init__(self, first, last, season, suffix=None):
        self.cols = ['game_season', 'date_game', 'age', 'team_id','game_location', 'opp_id','game_result', 'gs', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 
                     'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus' ]
        self.first, self.last, self.suffix, self.season = first, last, suffix, season
        self.regular_table, self.playoff_table = self.scrape_player()
        self.df, self.playoff_df = self.create_df(self.regular_table), self.create_df(self.playoff_table)

    def scrape_player(self):
        '''
        Validates and returns both regular and playoff table.
        '''
        content = PlayerValidate().validate(self.first, self.last, self.suffix, self.season)
        soup = BeautifulSoup(content,'html.parser')
        reg_season, playoff = soup.find('table', attrs={'id': 'pgl_basic'}), None
        # Find playoff table within commented JS
        for comment in soup.find_all(string=lambda string: isinstance(string, Comment)):
            if comment.find("<table") > 0:
                playoff_soup = BeautifulSoup(comment, 'html.parser')
                playoff = playoff_soup.find('table')
        regular = self.extract_player_stats(reg_season)
        playoff = self.extract_player_stats(playoff) if playoff else None
        return regular, playoff
    
    def extract_player_stats(self, table):
        '''
        Extracts player statistics from given table and returns array of rows with individual game statistics.

        Params:
        - table (bs4 element): table provided to extract statistics
        '''
        if not table:
            raise ValidationError(f"{self.first} {self.last}'s table cannot be found. Please check URL.")
        rows = []
        for row in table.tbody.find_all('tr'):
            if not row.get('class') == None: continue
            columns = row.find_all('td')
            cur = []
            for column in columns:
                stat, value = column['data-stat'], column.get_text()
                if stat == 'reason':
                    cur.append(value)
                    cur.extend([np.nan] * 21)
                    continue
                if stat in ['fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus']:  
                    value = float(value) if value else np.nan
                elif stat == 'mp' and value:
                    value = '00:' + value
                cur.append(value)
            rows.append(cur)
        return rows

    def create_df(self, rows):
        '''
        Creates dataframe for Player information given table and returns transformed Pandas dataframe.

        Params:
        - rows (bs4 element): table provided to create dataframe
        '''
        df = pd.DataFrame(rows, columns=self.cols)
        df = df.astype(dtype = {'game_season': "object", 'date_game': "str", 'age': "str", 'team_id': "str", 'game_location': "str", 'opp_id': "str", 
                               'game_result': "str", 'gs': "str", 'mp': "str", 'fg': "float", 'fga': "float", 'fg_pct': "float", 'fg3': "float", 'fg3a': "float", 
                               'fg3_pct': "float", 'ft': "float", 'fta': "float", 'ft_pct': "float", 'orb': "float", 'drb': "float", 'trb': "float", 'ast': "float", 'stl': "float",
                               'blk': "float", 'tov': "float", 'pf': "float", 'pts': "float", 'game_score': "float", 'plus_minus':"float" })
        return df
    
    def recent_games(self, n= 5, total=True, at=''):
        '''
        Prints recent n games and creates a subset of the dataframe with additional row of averages or totals.

        Params:
        - n (int): most recent n games
        - total (boolean): True for total or False for averages
        - at (str): Expects one of ['home', 'away', ''] for home, away or all games, respectively

        Errors:
        - raises AssertionError: if at is not a valid option
        '''
        if at not in ['home', 'away', '']:
            raise AssertionError("Invalid at. Expected one of: 'home', 'away' or ''.")
        if at == 'home':
            recent_df = self.df.iloc[-n:].query('game_location==""').copy() 
        elif at == 'away':
            recent_df = self.df.iloc[-n:].query('game_location=="@"').copy() 
        else:
            recent_df = self.df.iloc[-n:].copy() 
        mp = pd.to_timedelta(recent_df["mp"], errors='coerce')
        if total:
            recent_df.loc["total", ~recent_df.columns.isin(['fg_pct', 'fg3_pct', 'ft_pct'])] = recent_df.sum(numeric_only=True)
            recent_df.loc["total", "mp"] = mp.sum()
        else:
            recent_df.loc["avg"] = round(recent_df.mean(numeric_only=True), ndigits=2)
            recent_df.loc["avg", "mp"] = mp.mean().round('1s')
        return recent_df
    
    def dump_to_csv(self, path=''):
        '''
        Dumps final player data in a CSV format into provided path. 

        Params:
        - path (str): path for the file. If not provided, defaults to './data/{season}/players/{first}_{last}.csv'.
        '''
        if path == '' and self.suffix:
            path = f'./data/{self.season}/players/{self.first.lower().replace(".", "")}_{self.last.lower().rstrip(".,")}_{self.suffix.lower().rstrip(".,")}.csv'
        elif path == '':
            path = f'./data/{self.season}/players/{self.first.lower().replace(".", "")}_{self.last.lower().rstrip(".,")}.csv'

        with open(path, 'a', newline="") as f:
            writer = csv.writer(f, delimiter='\t')
            for row in self.regular_table:
                writer.writerow(row)
            if self.playoff_table:
                for row in self.playoff_table:
                    writer.writerow(row)