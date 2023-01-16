from lib import etl 
from datetime import datetime, timedelta

leagues=[{"league":"Champions League",
           "league_id":8},
          {"league":"Premier League",
           "league_id":2}]


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