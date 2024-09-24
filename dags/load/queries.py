select_season_query = ("SELECT season_id FROM season WHERE season_year = %s")
select_player_id = ("SELECT player_id FROM player WHERE first_name = %s AND last_name = %s;")
select_team_id = ("SELECT team_id FROM team WHERE abbreviation = %s")
select_game_id = ("SELECT game_id FROM game WHERE home_team_id = %s AND away_team_id = %s AND game_date = %s AND season_year = %s")
select_player_team = ("SELECT p.player_id FROM player p JOIN player_team_season pts ON p.player_id = pts.player_id WHERE p.first_name = %s AND p.last_name = %s AND (p.suffix = %s OR p.suffix IS NULL)AND pts.team_id = ANY(%s)")

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

insert_player_stats = ("""
    INSERT INTO player_stats (game_id, player_id, minutes_played, fg_made, fg_attempted, threes_made, threes_attempted, ft_made, ft_attempted, orb, drb, rebounds, assists, steals, blocks, turnovers, fouls, points, plus_minus, inactive)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")