-- initialize.sql
-- Script to create initial schema and tables

-- Table: team
CREATE TABLE IF NOT EXISTS team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(50) UNIQUE NOT NULL,
    abbreviation CHAR(3) UNIQUE NOT NULL
);

-- Table: season
CREATE TABLE IF NOT EXISTS season (
    season_id SERIAL PRIMARY KEY,
    season_year INT UNIQUE NOT NULL CHECK (season_year > 1945)
);

-- Table: player
CREATE TABLE IF NOT EXISTS player(
    player_id SERIAL PRIMARY KEY,
    first_name VARCHAR(25) NOT NULL,
    last_name VARCHAR(25) NOT NULL,
    suffix VARCHAR(5) DEFAULT NULL,
    position VARCHAR(5),
    height VARCHAR(4),
    weight INT CHECK (weight > 0),
    pre_draft_team VARCHAR(75),
    draft_pick VARCHAR(75),
    nationality VARCHAR(75),
    UNIQUE(first_name, last_name, draft_pick, nationality)
);

-- Table: player_team_season
CREATE TABLE IF NOT EXISTS player_team_season(
    player_team_season_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES player(player_id) ON DELETE CASCADE,
    team_id INT REFERENCES team(team_id) ON DELETE CASCADE,
    age INT,
    season_id INT REFERENCES season(season_id) ON DELETE CASCADE,
    current_team BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (player_id, season_id, team_id, current_team)
);

-- Table: game
CREATE TABLE IF NOT EXISTS game (
    game_id SERIAL PRIMARY KEY,
    home_team_id INT REFERENCES team(team_id) ON DELETE CASCADE,
    away_team_id INT REFERENCES team(team_id) ON DELETE CASCADE,
    game_date DATE,
    start_time VARCHAR(10),
    season_year INT REFERENCES season(season_year) ON DELETE CASCADE,
    home_team_score INT,
    away_team_score INT,
    attendance INT,
    duration INTERVAL,
    stage VARCHAR(20),
    UNIQUE(game_id, home_team_id, away_team_id, game_date, start_time, season_year)
);

-- Table: player_stats
CREATE TABLE IF NOT EXISTS player_stats (
    player_stats_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES game(game_id) ON DELETE CASCADE,
    player_id INT REFERENCES player(player_id) ON DELETE CASCADE,
    game_started BOOLEAN,
    minutes_played VARCHAR(15),
    fg_made INT CHECK (fg_made >= 0),                   
    fg_attempted INT CHECK (fg_attempted >= 0),          
    threes_made INT CHECK (threes_made >= 0),           
    threes_attempted INT CHECK (threes_attempted >= 0),      
    ft_made INT CHECK (ft_made >= 0),               
    ft_attempted INT CHECK (ft_attempted >= 0),          
    orb INT CHECK (orb >= 0),                   
    drb INT CHECK (drb >= 0),                   
    rebounds INT CHECK (rebounds >= 0),              
    assists INT CHECK (assists >= 0),               
    steals INT CHECK (steals >= 0),                
    blocks INT CHECK (blocks >= 0),                
    turnovers INT CHECK (turnovers >= 0),             
    fouls INT CHECK (fouls >= 0),                 
    points INT CHECK (points >= 0),                
    plus_minus INT,             
    inactive BOOLEAN DEFAULT FALSE,
    UNIQUE (game_id, player_id)
);

INSERT INTO team (team_name, abbreviation) VALUES
    ('Atlanta Hawks', 'ATL'),
    ('Boston Celtics', 'BOS'),
    ('Charlotte Hornets', 'CHO'),
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
    ('Brooklyn Nets', 'BRK'),
    ('Oklahoma City Thunder', 'OKC'),
    ('Orlando Magic', 'ORL'),
    ('Philadelphia 76ers', 'PHI'),
    ('Phoenix Suns', 'PHO'),
    ('Portland Trail Blazers', 'POR'),
    ('Sacramento Kings', 'SAC'),
    ('San Antonio Spurs', 'SAS'),
    ('Toronto Raptors', 'TOR'),
    ('Utah Jazz', 'UTA'),
    ('Washington Wizards', 'WAS');

INSERT INTO season (season_year)
SELECT generate_series(1946, 2024) AS season_year;