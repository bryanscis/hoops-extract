team = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "CHO": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",
    "NYK": "New York Knicks",
    "BRK": "Brooklyn Nets",
    "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHO": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",
    "WAS": "Washington Wizards"
}

team_realgm = {
    "PHX": "PHO",
    "PHL": "PHI", 
    "CHA": "CHO",
    "UTH": "UTA",
    "SAN": "SAS"
}

def get_team_abbreviation(team_name):
    for abbreviation, name in team.items():
        if name == team_name:
            return abbreviation
    return None

def split_name(full_name):
    parts = full_name.split(' ')
    first_name, last_name, suffix = parts[0], parts[1], None
    if len(parts) > 2:
        if parts[-1] in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']:
            suffix = parts[-1]
        else:
            last_name = last_name.rstrip(',.')
            suffix = parts[-1]
    return first_name, last_name, suffix