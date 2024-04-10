from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime

class Player:
    def __init__(self):
        self.df = self.scrape_player()

    def scrape_player(self):
        with open('curry.html') as f:
            soup = BeautifulSoup(f, 'html.parser')
        table = soup.find('table', attrs={'id': 'pgl_basic'})
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
        df = pd.DataFrame(rows, columns=['game_season', 'date_game', 'age', 'team_id','game_location', 'opp_id','game_result', 'gs', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 
                                         'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus' ])
        df = df.astype(dtype = {'game_season': "object", 'date_game': "str", 'age': "str", 'team_id': "str", 'game_location': "str", 'opp_id': "str", 
                               'game_result': "str", 'gs': "str", 'mp': "str", 'fg': "Int64", 'fga': "Int64", 'fg_pct': "float64", 'fg3': "Int64", 'fg3a': "Int64", 
                               'fg3_pct': "float64", 'ft': "Int64", 'fta': "Int64", 'ft_pct': "float64", 'orb': "Int64", 'drb': "Int64", 'trb': "Int64", 'ast': "Int64", 'stl': "Int64",
                               'blk': "Int64", 'tov': "Int64", 'pf': "Int64", 'pts': "Int64", 'game_score': "float64", 'plus_minus':"float64" })
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