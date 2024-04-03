from bs4 import BeautifulSoup

with open('curry.html') as f:
    soup = BeautifulSoup(f, 'html.parser')

table = soup.find('table', attrs={'id': 'pgl_basic'})
for row in table.tbody.find_all('tr'):
    columns = row.find_all('td')
    for column in columns:
        stat, value = column['data-stat'], column.get_text()