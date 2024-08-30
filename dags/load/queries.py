select_season_query = ("SELECT season_id FROM season WHERE season_year = %s")
select_player_id = ("SELECT player_id FROM player WHERE first_name = %s AND last_name = %s;")
select_team_id = ("SELECT team_id FROM team WHERE abbreviation = %s")

insert_player_query = ("""
    INSERT INTO player (first_name, last_name, suffix, position, height, weight, pre_draft_team, draft_pick, nationality)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (first_name, last_name, suffix, draft_pick, nationality)
    DO UPDATE SET position = EXCLUDED.position, 
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                pre_draft_team = EXCLUDED.pre_draft_team
    RETURNING player_id;
""")
insert_player_team_season_query = ("""
    INSERT INTO player_team_season (player_id, team_id, age, season_id, current_team)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (player_id, team_id, season_id)
    DO UPDATE SET age = EXCLUDED.age,
                current_team = EXCLUDED.current_team
    WHERE player_team_season.current_team = TRUE OR EXCLUDED.current_team = TRUE;
""")