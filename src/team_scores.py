from pymongo import MongoClient


def get_team_name_from_team_id(teams, team_id):
    team_cursor = teams.find_one({"TeamID": team_id})
    team_name = team_cursor["Team"]
    return team_name


def get_stadium_details_from_id(stadiums, stadium_id):
    stadium_cursor = stadiums.find_one({"SID": stadium_id})
    return stadium_cursor.get("SName"), stadium_cursor.get("SCity")


def populate_team_scores_collection(db):
    # key -> team name
    # value -> match object (match date, city, stadium name, team 1 name, team 1 score, team 2 name, team 2 score)

    print("First, clear out the collection")
    db.team_scores.remove({})

    teams_scores = []
    teams = db.test_teams.find()
    for team in teams:
        if not team["TeamID"]:
            continue

        matches_played = db.test_scheduleresults.find({"TeamID1": team.get("TeamID")})

        team_scores = {}
        team_name = team["Team"]
        team_scores["team"] = team_name
        team_scores["scores"] = []

        for match_played in matches_played:
            match_date = match_played["MatchDate"]
            team1_name = get_team_name_from_team_id(
                db.test_teams, match_played["TeamID1"]
            )
            team2_name = get_team_name_from_team_id(
                db.test_teams, match_played["TeamID2"]
            )
            team1_score = match_played["Team1_Score"]
            team2_score = match_played["Team2_Score"]
            stadium_name, stadium_city = get_stadium_details_from_id(
                db.test_stadiums, match_played["SID"]
            )

            team_scores["scores"].append(
                {
                    "MatchDate": match_date,
                    "StadiumName": stadium_name,
                    "StadiumCity": stadium_city,
                    "Team1Name": team1_name,
                    "Team1Score": team1_score,
                    "Team2Name": team2_name,
                    "Team2Score": team2_score,
                }
            )

        teams_scores.append(team_scores)

    print("Inserting team scores")
    db.team_scores.insert_many(teams_scores)


def run_queries(db):
    print("Running queries for TEAM_SCORES")
    teams = ["Japan", "Portugal", "Spain", "Morocco", "Argentina"]
    for team in teams:
        print("Finding all scores made by ", team)
        scores = db.team_scores.find({"team": team})
        for score in scores:
            import pprint

            print(score.get("scores"))


def run():
    client = MongoClient("mongodb://localhost:27017")
    db = client.admin  # use the db name here
    populate_team_scores_collection(db)
    run_queries(db)


if __name__ == "__main__":
    run()
