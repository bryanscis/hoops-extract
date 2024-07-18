-- insert.sql
-- Functions that insert values into certain tables

-- Function to insert a player and return player_id
CREATE OR REPLACE FUNCTION insert_player(
    first_name TEXT, last_name TEXT, suffix TEXT, position TEXT, height TEXT, weight TEXT,
    pre_draft_team TEXT, draft_pick TEXT, nationality TEXT)
RETURNS INTEGER AS $$
BEGIN
    INSERT INTO player (first_name, last_name, suffix, position, height, weight, pre_draft_team, draft_pick, nationality)
    VALUES (first_name, last_name, suffix, position, height, weight, pre_draft_team, draft_pick, nationality)
    RETURNING player_id INTO player_id;
    RETURN player_id;
END;
$$ LANGUAGE plpgsql;

-- Function to insert player_team_season
CREATE OR REPLACE FUNCTION insert_player_team_season(
    player_id INTEGER, team_id INTEGER, age INTEGER, season_id INTEGER, current_team BOOLEAN)
RETURNS VOID AS $$
BEGIN
    INSERT INTO player_team_season (player_id, team_id, age, season_id, current_team)
    VALUES (player_id, team_id, age, season_id, current_team);
END;
$$ LANGUAGE plpgsql;