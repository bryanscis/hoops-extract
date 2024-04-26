from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

class Game:
    def __init__(self):
        self.cols = ['player', 'mp', 'fg', 'fga', 'fg%', '3p', '3pa', '3p%', 'ft', 'fta', 'ft%', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']
        self.home, self.away = self.scrape_game()

    def scrape_game(self):
        with open('brk_gsw_2013.html') as f:
            soup = BeautifulSoup(f, 'html.parser')
        home_table, away_table = soup.find('table', attrs={'id': 'box-BRK-game-basic'}), soup.find('table', attrs={'id': 'box-GSW-game-basic'})
        return self.create_df(home_table), self.create_df(away_table)

    def create_df(self, table):
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