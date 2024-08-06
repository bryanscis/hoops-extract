import json
import requests
import logging
from string import ascii_lowercase
from unidecode import unidecode
from scripts.proxies import proxies
from bs4 import BeautifulSoup

def extract_players():
    '''
    Extracts all players name and their respective Basketball Reference URLs and outputs to location at './data/all_players.json'.
    '''
    players_data = []
    with open("./data/all_players.json", "w") as writefile:
        for alphabet in ascii_lowercase:
            url = f'https://www.basketball-reference.com/players/{alphabet.lower()}/'
            headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"}
            try:
                logging.info(f"Parsing players from {url}.")
                page = requests.get(url, headers=headers, proxies=proxies)
                soup = BeautifulSoup(page.content, 'html.parser')
                table = soup.find('table', attrs={'id': 'players'})
                for row in table.tbody.find_all('tr'):
                    if not row.get('class') == None: continue
                    name = row.find('th', {'data-stat': 'player'}).text.strip()
                    player_url = f"https://www.basketball-reference.com{row.find('th', {'data-stat': 'player'}).a['href']}"
                    players_data.append({"Name": unidecode(name), "URL": player_url})
                logging.info(f"Players with last name starting with '{alphabet.upper()}' added.")
            except:
                logging.info(f"Error parsing information from {url}. Exiting.")
        try:
            json.dump(players_data, writefile, indent=4)
            logging.info(f"Players and their respective URLs has been dumped to './data/all_players.json'.")
        except:
            logging.info(f"Error dumping file to './data/all_players.json'.")