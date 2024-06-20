from unidecode import unidecode
import requests
from datetime import datetime

class ValidationError(Exception):
    '''Custom exception class for validation errors.'''
    pass

class BaseValidate:
    def fetch_content(self, url):
        '''
        Returns content from given URL.
        
        Params:
        - url (string): URL to fetch content from
        '''
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise ValidationError(f'Error fetching data from {url} : {e}')

class PlayerValidate(BaseValidate):

    def validate(self, first, last, season='2023'):
        '''
        Checks to see if user input information is reachable.

        Params:
        - first  (string): first name
        - last   (string): last name
        - season (string): ending of NBA season
        '''
        first, last = unidecode(first).lower(), unidecode(last).lower()
        if not first.isalpha() and not last.isalpha():
            raise ValidationError('First or last name needs to be alphabetical.')
        if not first or not last:
            raise ValidationError('First or last name cannot be empty.')
        if not season.isdigit() or not (1946 <= int(season) <= datetime.now().year):
            raise ValidationError("Season must be a valid year between 1946 and the current year.")
        
        url = f'https://www.basketball-reference.com/players/{last[0]}/{last[:5]}{first[:2]}01/gamelog/{season}'
        content = self.fetch_content(url)
        if not content:
            raise ValidationError('No content can be fetched with the parameters.')
        
        return content