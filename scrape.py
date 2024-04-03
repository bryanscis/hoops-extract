from bs4 import BeautifulSoup
import pandas as pd

with open('curry.html') as f:
    soup = BeautifulSoup(f, 'html.parser')

table = soup.find('table', attrs={'id': 'pgl_basic'})
rows = []
for row in table.tbody.find_all('tr'):
    columns = row.find_all('td')
    cur = []
    for column in columns:
        stat, value = column['data-stat'], column.get_text()
        cur.append(value)
    rows.append(cur)

df = pd.DataFrame(rows, columns=['game_season', 'date_game', 'age', 'team_id','game_location', 'opp_id','game_result', 'gs', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus' ])