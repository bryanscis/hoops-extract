from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

class Roster:

    def __init__(self):
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
        df = pd.DataFrame(rows, columns=['player', 'age', 'g', 'gs', 'mp', 'fg', 'fga', 'fg%', '3p', '3pa', '3p%', '2p', '2pa', '2p%', 'efg%', 'ft', 'fta', 'ft%', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts'])
        df = df.astype(dtype= {'player': 'str', 'age': 'Int64', 'g': 'Int64', 'gs': 'Int64', 'mp': 'Int64', 'fg': 'Int64', 'fga': 'Int64', 'fg%': 'float64', '3p': 'Int64', '3pa': 'Int64', '3p%':'float', 
                               '2p': 'Int64', '2pa': 'Int64', '2p%': 'float', 'efg%': 'float', 'ft':'Int64', 'fta': 'Int64', 'ft%': 'float', 'orb': 'Int64', 'drb': 'Int64', 'trb':'Int64',
                                'ast': 'Int64', 'stl': 'Int64', 'blk': 'Int64', 'tov': 'Int64', 'pf': 'Int64', 'pts': 'Int64'})