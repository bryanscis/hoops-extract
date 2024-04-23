from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

class Roster:

    def __init__(self):
        self.cols = ['player', 'age', 'g', 'gs', 'mp', 'fg', 'fga', 'fg%', '3p', '3pa', '3p%', '2p', '2pa', '2p%', 'efg%', 'ft', 'fta', 'ft%', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts']
        self.df = self.scrape_roster()

    def scrape_roster(self):
        with open('gsw.html') as f:
            soup = BeautifulSoup(f, 'html.parser')
        table = soup.find('div', attrs={'id': 'div_totals'})
        rows = []
        for row in table.tbody.find_all('tr'):
            cur = []
            for column in row.find_all('td'):
                stat, value = column['data-stat'], column.get_text('data-stat')
                if value == '':
                    value = np.nan
                cur.append(value)
            rows.append(cur)
        df = pd.DataFrame(rows, columns=self.cols)
        df = df.astype(dtype= {'player': 'str', 'age': 'int', 'g': 'int', 'gs': 'int', 'mp': 'float', 'fg': 'float', 'fga': 'float', 'fg%': 'float', '3p': 'float', '3pa': 'float', '3p%':'float', 
                               '2p': 'float', '2pa': 'float', '2p%': 'float', 'efg%': 'float', 'ft':'float', 'fta': 'float', 'ft%': 'float', 'orb': 'float', 'drb': 'float', 'trb':'float',
                                'ast': 'float', 'stl': 'float', 'blk': 'float', 'tov': 'float', 'pf': 'float', 'pts': 'float'})
        return df
        
    def per_game(self):
        '''
        Returns dataframe of per game statistics of NBA roster.
        '''
        pg_df = self.df.copy() 
        pg_df.iloc[:, 4:] = round(pg_df.iloc[:, 4:].div(pg_df.g, axis=0), ndigits=2)
        return pg_df

    def per_36(self):
        '''
        Returns dataframe of per 36 minute statistic of NBA roster. Calculated by dividing stat by minutes played and then multiplying by 36.
        '''
        p36_df = self.df.copy()
        p36_df.iloc[:, 5:] = round(p36_df.iloc[:, 5:].div(p36_df.mp, axis=0).mul(36, axis=0), ndigits=2)
        return p36_df