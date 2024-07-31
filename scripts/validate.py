import json
import requests
from unidecode import unidecode
from datetime import datetime
from .proxies import proxies
import re

class ValidationError(Exception):
    '''Custom exception class for validation errors.'''
    pass

class BaseValidate:
    _teams = None
    _players = None
    _normalized_players = None

    @staticmethod
    def load_teams(file_path='./data/teams.json'):
        """
        Loads team data from a JSON file into a class-level list.

        Args:
            file_path (str): Path to the JSON file containing teams data.

        Raises:
            ValidationError: If the teams data file cannot be loaded.
        """
        if not BaseValidate._teams:
            try:
                with open(file_path, 'r') as file:
                    BaseValidate._teams = json.load(file)
            except FileNotFoundError as e:
                raise ValidationError(f'Error loading teams data: {e}')

    @staticmethod
    def load_players(file_path='./data/all_players.json'):
        """
        Loads player data from a JSON file into a class-level list.

        Args:
            file_path (str): Path to the JSON file containing player name and its URL.

        Raises:
            ValidationError: If the players data file cannot be loaded.
        """
        if not BaseValidate._players:
            try:
                with open(file_path, 'r') as file:
                    players = json.load(file)
                    BaseValidate._players = {player['Name']: player['URL'] for player in players}
                    BaseValidate._normalized_players = {re.sub(r'\W+', '', name).lower(): name for name in BaseValidate._players.keys()}
            except FileNotFoundError as e:
                raise ValidationError(f'Error loading player data: {e}')
            
    def fetch_content(self, url):
        '''
        Returns content from given URL.
        
        Params:
        - url (string): URL to fetch content from
        '''
        try:
            headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"}
            response = requests.get(url, headers=headers, proxies=proxies)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            raise ValidationError(f'Error fetching data from {url} : {e}')
        
    def validate(self, **kwargs):
        '''
        Base validate function for common checks across all Validate classes.

        Params:
        - **kwargs:
            - season (string): ending of NBA season
        '''
        season = kwargs.get('season')
        if not season.isdigit() or not (1946 <= int(season) <= datetime.now().year):
            raise ValidationError("Season must be a valid year between 1946 and the current year.")

class PlayerValidate(BaseValidate):

    def validate(self, first, last, suffix=None, season='2023'):
        '''
        Checks to see if user input information is reachable for players.

        Params:
        - first  (string): first name
        - last   (string): last name
        - season (string): ending of NBA season
        '''
        super().validate(season=season)
        if not first.isalpha() and not last.isalpha():
            raise ValidationError('First or last name needs to be alphabetical.')
        if not first or not last:
            raise ValidationError('First or last name cannot be empty.')
        suffix = " " + suffix if suffix else ''
        normalized_full_name = f'{first} {last}{suffix}'
        matched_name = self._normalized_players.get(re.sub(r'\W+', '', normalized_full_name).lower(), None)
        if not matched_name:
            raise ValidationError('Player cannot be found. Please check player name.')
        
        player_url = self._players[matched_name]

        url = f'{player_url}/gamelog/{season}'
        content = self.fetch_content(url).decode('utf-8')
        if not content:
            raise ValidationError('No content can be fetched with the parameters.')
        
        return content
    
class RosterValidate(BaseValidate):

    def validate(self, team, season):
        '''
        Checks to see if user input information is reachable for roster.

        Params:
        - team   (string): team's name which should be its abbreviation ie. GSW, SAC etc..
        - season (string): ending of NBA season
        '''
        super().validate(season=season)
        if not team.replace(' ', '').isalpha():
            raise ValidationError("Team name must contain only alphabetic characters and spaces.")
        if len(team) != 3:
            raise ValidationError("Team name should only be in its abbreviation form.")
        if not team:
            raise ValidationError("Team name cannot be empty.")
        
        url = f'https://www.basketball-reference.com/teams/{team}/{season}.html'
        content = self.fetch_content(url)
        if not content:
            raise ValidationError('No content can be fetched with the parameters.')
        
        return content
    
class GameValidate(BaseValidate):

    def validate(self, home_team, away_team, date):
        '''
        Checks to see if user input information is reachable for game.

        Params:
        - home_team   (string): home team's name which should be its abbreviation ie. GSW, SAC etc..
        - away_team   (string): away team's name which should be its abbreviation ie. GSW, SAC etc..
        - date        (string): date of match
        '''
        BaseValidate.load_teams()
        formatted_date = None
        try:
            date_obj = datetime.strptime(date, "%a, %b %d, %Y")
            formatted_date = date_obj.strftime("%Y%m%d")
        except:
            raise ValidationError('Date must follow format of "Weekday, Month Day, Year" ie. "Tue, Oct 24, 2023".')
        if home_team.upper() not in self._teams.keys():
            raise ValidationError(f"Home team {home_team} is not a valid NBA team abbreviation.")
        if away_team.upper() not in self._teams.keys():
            raise ValidationError(f"Away team {away_team} is not a valid NBA team abbreviation.")
        
        url = f'https://www.basketball-reference.com/boxscores/{formatted_date}0{home_team}.html'
        content = self.fetch_content(url)
        if not content:
            raise ValidationError('No content can be fetched with the parameters.')

        return content