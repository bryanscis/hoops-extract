-- initialize.sql
-- Script to create initial schema and tables

-- Table: player
CREATE TABLE IF NOT EXISTS player (
    player_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    team_id INT REFERENCES team(team_id),
    UNIQUE (first_name, last_name)
);

-- Table: team
CREATE TABLE IF NOT EXISTS team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(50) NOT NULL,
    abbreviation CHAR(3) UNIQUE NOT NULL
);

-- Table: game
CREATE TABLE IF NOT EXISTS game (
    game_id SERIAL PRIMARY KEY,
    home_team_id INT REFERENCES team(team_id),
    away_team_id INT REFERENCES team(team_id),
    game_date DATE,
    home_team_score INT,
    away_team_score INT,
    attendance INT,
    game_time VARCHAR(5),
    season_id INT REFERENCES season(season_id)
);

-- Table: player_stats
CREATE TABLE IF NOT EXISTS player_stats (
    player_stats_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES game(game_id),
    player_id INT REFERENCES player(player_id),
    minutes_played INT,
    points INT,
    assists INT,
    rebounds INT,
    steals INT,
    blocks INT,
    turnovers INT,
    fouls INT
);

-- Table: season
CREATE TABLE IF NOT EXISTS season (
    season_id SERIAL PRIMARY KEY,
    season_year INT,
    UNIQUE (season_year)
);

INSERT INTO team (team_name, abbreviation) VALUES
    ('Atlanta Hawks', 'ATL'),
    ('Boston Celtics', 'BOS'),
    ('Charlotte Hornets', 'CHA'),
    ('Chicago Bulls', 'CHI'),
    ('Cleveland Cavaliers', 'CLE'),
    ('Dallas Mavericks', 'DAL'),
    ('Denver Nuggets', 'DEN'),
    ('Detroit Pistons', 'DET'),
    ('Golden State Warriors', 'GSW'),
    ('Houston Rockets', 'HOU'),
    ('Indiana Pacers', 'IND'),
    ('Los Angeles Clippers', 'LAC'),
    ('Los Angeles Lakers', 'LAL'),
    ('Memphis Grizzlies', 'MEM'),
    ('Miami Heat', 'MIA'),
    ('Milwaukee Bucks', 'MIL'),
    ('Minnesota Timberwolves', 'MIN'),
    ('New Orleans Pelicans', 'NOP'),
    ('New York Knicks', 'NYK'),
    ('Brooklyn Nets', 'BKN'),
    ('Oklahoma City Thunder', 'OKC'),
    ('Orlando Magic', 'ORL'),
    ('Philadelphia 76ers', 'PHI'),
    ('Phoenix Suns', 'PHO'),
    ('Portland Trail Blazers', 'POR'),
    ('Sacramento Kings', 'SAC'),
    ('San Antonio Spurs', 'SAS'),
    ('Toronto Raptors', 'TOR'),
    ('Utah Jazz', 'UTH'),
    ('Washington Wizards', 'WAS');