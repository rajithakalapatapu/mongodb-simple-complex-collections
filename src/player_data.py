from pymongo import MongoClient


def find_matches_player_started(db, team, player_number):
    matches_started = []
    started = db.test_startinglineups.find({"TeamID": team, "PlayerID": player_number})
    for start in started:
        game_id = start.get("GameID")
        games = db.test_scheduleresults.find({"GameID": game_id})
        for game in games:
            stadium_cursor = db.test_stadiums.find_one({"SID": game.get("SID")})
            stadium, city = stadium_cursor.get("SName"), stadium_cursor.get("SCity")
            teams = db.test_teams.find_one({"TeamID": game.get("TeamID2")})
            opposing_team = teams["Team"]
            starting_game_data = {
                "MatchDate": game.get("MatchDate"),
                "City": city,
                "Stadium": stadium,
                "OpposingTeam": opposing_team,
            }
            matches_started.append(starting_game_data)
    return matches_started


def find_matches_player_scored(db, team, player_number):
    matches_scored = []
    goals = db.test_goals.find({"TeamID": team, "PlayerID": player_number})
    own_goals = db.test_owngoals.find({"TeamID": team, "PlayerID": player_number})

    for goal in goals:
        game_results = db.test_scheduleresults.find_one({"GameID": goal.get("GameID")})
        stadium_cursor = db.test_stadiums.find_one({"SID": game_results.get("SID")})
        stadium, city = stadium_cursor.get("SName"), stadium_cursor.get("SCity")
        teams = db.test_teams.find_one({"TeamID": goal.get("TeamID")})
        opposing_team = teams["Team"]

        goal_data = {
            "GoalType": "Normal" if goal["Penalty"].lower() == "n" else "Penalty",
            "Time": goal["Time"],
            "MatchDate": game_results.get("MatchDate"),
            "Stadium": stadium,
            "City": city,
            "OpposingTeam": opposing_team,
        }
        matches_scored.append(goal_data)

    for own_goal in own_goals:
        game_results = db.test_scheduleresults.find_one(
            {"GameID": own_goal.get("GameID")}
        )
        stadium_cursor = db.test_stadiums.find_one({"SID": game_results.get("SID")})
        stadium, city = stadium_cursor.get("SName"), stadium_cursor.get("SCity")
        teams = db.test_teams.find_one({"TeamID": own_goal.get("TeamID")})
        opposing_team = teams["Team"]

        own_goal_data = {
            "GoalType": "OwnGoal",
            "Time": own_goal["Time"],
            "MatchDate": game_results.get("MatchDate"),
            "Stadium": stadium,
            "City": city,
            "OpposingTeam": opposing_team,
        }

        matches_scored.append(own_goal_data)
    return matches_scored


def populate_player_data(db):
    print("Removing previous player data if any")
    db.player_data.remove({})
    rosters = db.test_rosters.find()
    player_data = []

    for roster in rosters:
        if not roster.get("FIFA Popular Name"):
            continue
        player = {
            "PName": roster.get("FIFA Popular Name"),
            "Team": roster.get("Team"),
            "PNo": roster.get("PlayerID"),
            "Position": roster.get("Position"),
            "Started": find_matches_player_started(
                db, roster.get("TeamID"), roster.get("PlayerID")
            ),
            "Scored": find_matches_player_scored(
                db, roster.get("TeamID"), roster.get("PlayerID")
            ),
        }

        player_data.append(player)

    print("Inserting player data")
    db.player_data.insert_many(player_data)


def run_queries(db):
    players = ["MERCADO Gabriel", "HIGUAIN Gonzalo", "ROJO Marcos"]
    for player in players:
        print("Finding all data for player ", player)
        data = db.player_data.find({"PName": player})
        for d in data:
            import pprint

            # pprint.pprint(d)
            print(d)


def run():
    client = MongoClient("mongodb://localhost:27017")
    db = client.admin  # use the db name here
    # populate_player_data(db)
    run_queries(db)


if __name__ == "__main__":
    run()
