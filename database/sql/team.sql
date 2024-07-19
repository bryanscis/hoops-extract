-- Returns Game table with abbreviation 
select
    ht.abbreviation,
    at.abbreviation,
    game_date,
    start_time,
    season_year,
    home_team_score,
    away_team_score,
    attendance,
    duration
from
    game g
    left join team ht on g.home_team_id = ht.team_id
    left join team at on g.away_team_id = at.team_id
order by
    game_date,
    start_time,
    ht.abbreviation,
    at.abbreviation,
    duration;