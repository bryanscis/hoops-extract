from bs4 import BeautifulSoup, Comment
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from .validate import PlayerValidate

class Player:
    def __init__(self, first, last, season):
        self.cols = ['game_season', 'date_game', 'age', 'team_id','game_location', 'opp_id','game_result', 'gs', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 
                     'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus' ]
        self.first, self.last, self.season = first, last, season
        self.df, self.playoff_df = self.scrape_player()

    def scrape_player(self):
        content = PlayerValidate().validate(self.first, self.last, self.season)
        soup = BeautifulSoup(content,'html.parser')
        reg_season, playoff = soup.find('table', attrs={'id': 'pgl_basic'}), None
        # Find playoff table within commented JS
        for comment in soup.find_all(string=lambda string: isinstance(string, Comment)):
            if comment.find("<table") > 0:
                playoff_soup = BeautifulSoup(comment, 'html.parser')
                playoff = playoff_soup.find('table')
        regular_df = self.create_df(reg_season)
        playoff_df = self.create_df(playoff) if playoff else None
        return regular_df, playoff_df

    def create_df(self, table):
        '''
        Creates dataframe for Player information given table and returns transformed Pandas dataframe.

        Params:
        - table (bs4 element): table provided to create dataframe
        '''
        rows = []
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            cur = []
            for column in columns:
                stat, value = column['data-stat'], column.get_text()
                if stat in ['fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus']:  
                    value = float(value) if value else np.nan
                elif stat == 'mp' and value:
                    value = '00:' + value
                cur.append(value)
            rows.append(cur)
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