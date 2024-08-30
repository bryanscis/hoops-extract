import json
import requests
import logging
from bs4 import BeautifulSoup
from string import ascii_lowercase
from unidecode import unidecode
from scripts.proxies import proxies

def extract_all_players():
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
                    player_code = row.find('th', {'data-stat': 'player'}).get('data-append-csv')
                    player_url = f"https://www.basketball-reference.com/players/{alphabet}/{player_code}"
                    players_data.append({"Name": unidecode(name), "URL": player_url})
                logging.info(f"Players with last name starting with '{alphabet.upper()}' added.")
            except Exception as e:
                logging.exception(f"Error parsing information from {url}. Exiting.")
                raise e
        try:
            json.dump(players_data, writefile, indent=4)
            logging.info(f"Players and their respective URLs has been dumped to './data/all_players.json'.")
        except Exception as e:
            logging.exception(f"Error dumping file to './data/all_players.json'.")
            raise e