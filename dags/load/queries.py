select_season_query = ("SELECT season_id FROM season WHERE season_year = %s")
select_player_id = ("SELECT player_id FROM player WHERE first_name = %s AND last_name = %s;")
select_team_id = ("SELECT team_id FROM team WHERE abbreviation = %s")
select_team_name_id = ("SELECT team_id FROM team WHERE team_name = %s;")
select_game_id = ("SELECT game_id FROM game WHERE home_team_id = %s AND away_team_id = %s AND game_date = %s AND season_id = %s")
select_player_team = ("SELECT p.player_id FROM player p JOIN player_team_season pts ON p.player_id = pts.player_id WHERE p.first_name = %s AND p.last_name = %s AND (p.suffix = %s OR p.suffix IS NULL)AND pts.team_id = ANY(%s)")
select_all_games_query = ("""
    SELECT g.game_id, g.start_time, t1.team_name AS home_team, t2.team_name AS away_team, g.game_date
    FROM game g
    JOIN team t1 ON g.home_team_id = t1.team_id
    JOIN team t2 ON g.away_team_id = t2.team_id
    WHERE g.season_id = %s;
""")

insert_new_game_query = ("""
    INSERT INTO game(home_team_id, away_team_id, game_date, start_time, season_id) VALUES (%s, %s, %s, %s, %s);
""")

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

update_game_query = """
    UPDATE game SET game_date = %s, start_time = %s WHERE game_id = %s;
"""

update_statistics_query = ("""
    UPDATE game SET home_team_score = %s, away_team_score = %s, attendance = %s, duration = %s, stage = %s WHERE game_id = %s;
""")