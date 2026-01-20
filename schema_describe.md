Here the schema explanation of the nba dataset:

table team_details: ?
team_id - fk/pk (pk) 

table inactive_players: - yes (PRIMARY KEY game_id, player_id)
game_id - fk
player_id - fk
team_id - fk

table officials: - yes (PRIMARY KEY game_id, official_id)
game_id - fk
official_id - fk

table game: - yes
season_id - pk
team_id_home - fk
game_id - fk
team_id_away - fk

table draft_combine_stats: ?
player_id - fk/pk

table line_score: - yes (PRIMARY KEY (game_id, team_id))
game_id - fk
team_id_home - fk

table other_stats:
game_id - fk/pk
league_id - fk/pk
team_id_home - fk/pk
team_id_away - fk/pk

table team: 
id - pk

table game_info:
game_id - pk

table draft_history:
person_id - pk
team_id - fk

table play_by_play:
game_id pk
player1_team_id - fk
player2_team_id - fk
player3_team_id - fk
player1_id - fk
player2_id - fk
player3_id - fk

table player: - yes, fk from player_id and person id
id - pk

table team_history:
team_id - fk

table common_player_info: ?
person_id - fk/pk
team_id - fk

table game_summary:
game_id - fk/pk
game_status_id - fk
home_team_id - fk
visitor_team_id - fk