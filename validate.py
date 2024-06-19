from unidecode import unidecode
import requests
from bs4 import BeautifulSoup

class BaseValidate:
    def fetch_content(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            print(f'Error fetching data from {url} : {e}')
            return None

class PlayerValidate(BaseValidate):
    def generate_url(self, first='', last='', season='2023'):
        first, last = unidecode(first), unidecode(last)
        url = f'https://www.basketball-reference.com/players/{last[0]}/{last[:5]}{first[:2]}01/gamelog/{season}'
        return url
    
    def validate(self, first='', last='', season='2023'):
        url = self.generate_url(first, last, season)
        content = self.fetch_content(url)
        return content