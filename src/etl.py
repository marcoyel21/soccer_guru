from lib import etl 
from datetime import datetime, timedelta

leagues=[{"league":"Champions League",
           "league_id":2},
          {"league":"Premier League",
           "league_id":8},
        {"league":"La liga",
           "league_id":564},
        {"league":"Liga Portugal",
           "league_id":462},
        {"league":"Ligue 1",
           "league_id":301},
         {"league":"Bundesliga",
           "league_id":82},
        {"league":"Serie A",
           "league_id":384},
        {"league":"Eredivise",
           "league_id":72},
        {"league":"Europa League",
           "league_id":5}]


# Load data from this week and past week
past_week = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
future_week = (datetime.today() + timedelta(days=7)).strftime("%Y-%m-%d")
today=datetime.today().strftime("%Y-%m-%d")

print("Welcome back, today is ",today)
print("Last week matches added to database")
#last week data
for i in range(len(leagues)):
    league_id=(leagues[i]["league_id"])
    league=(leagues[i]["league"])
    print("Uploading data from ",league)
    etl.send_last_week_data(league_id,past_week,today,"historic")

print("Next week matches added to database")
#future week data
for i in range(len(leagues)):
    league_id=(leagues[i]["league_id"])
    league=(leagues[i]["league"])
    print("Uploading data from ",league)
    etl.send_future_matches(league_id,today,future_week,"historic")
