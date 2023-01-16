import os
import yaml
from google.cloud import bigquery
import requests
from tqdm import tqdm
from datetime import datetime

import yaml
path = os.path.dirname(os.path.abspath(__file__))
script_path_yaml=os.path.join(path,'config.yaml')
config_file = open(script_path_yaml, 'r')
config = yaml.safe_load(config_file)



# Functions
def unique(list1):
 
    # initialize a null list
    unique_list = []
 
    # traverse for all elements
    for x in list1:
        # check if exists in unique_list or not
        if x not in unique_list:
            unique_list.append(x)
    return unique_list

def api_call(endpoint):
    sports_key=config['sports_token']
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    end_url = "?api_token=" + str(sports_key)
    url=base_url+endpoint+end_url
    r = requests.get(url)
    return r.json()['data']

def api_call_seasons_all_pages():
    sports_key=config['sports_token']
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    end_url = "?api_token=" + str(sports_key)
    catalogue=[]
    catalogue=[]
    endpoint="seasons"
    for i in range(5):
        page_number=i+1
        page = "&page="+str(page_number)
        url=base_url+endpoint+end_url+page
        r = requests.get(url)
        list_= r.json()['data']
        catalogue.extend(list_)
    return catalogue


def get_all_teams_from_all_seasons(league_id):
    # get all seasons and then filter for the league id 
    all_seasons=api_call_seasons_all_pages()
    x_league_seasons = [d for d in all_seasons if d['league_id']==league_id]
    # for each season_id get the teams lists
    whole_teams={}
    for i in range(len(x_league_seasons)):
        season_id=x_league_seasons[i]["id"]
        whole_teams[i]=api_call("teams/season/"+str(season_id))
    # unpack the data and get only unique elements
    teams_in_pm={}
    list_teams=[]
    for season in range(len(whole_teams)):
        for team in range(len(whole_teams[season])):
            single_team=whole_teams[season][team]['id']
            list_teams.append(single_team)
    unique_teams=unique(list_teams)
    return(unique_teams)

def get_current_season_teams(league_id):
    all_seasons=api_call_seasons_all_pages()
    x_league_seasons = [d for d in all_seasons if d['league_id']==league_id]
    current_season_id=x_league_seasons[-1]["id"]
    whole_teams=api_call("teams/season/"+str(current_season_id))
    list_teams=[]
    for team in range(len(whole_teams)):
        single_team=whole_teams[team]['id']
        list_teams.append(single_team)
    return list_teams

                                                   

def get_relevant_data_past(data):
    data_subset=dict((k, data[k]) for k in ('id', 'league_id', 'season_id','localteam_id','visitorteam_id','winner_team_id'))
    scores=data["scores"]
    scores_subset=dict((k, scores[k]) for k in ('localteam_score', 'visitorteam_score'))
    standings=data["standings"]
    standings_subset=dict((k, standings[k]) for k in ('localteam_position', 'visitorteam_position'))
    time=data['time']['starting_at']
    time_subset=dict((k, time[k]) for k in ('date','time'))
    dt = str(datetime.now())
    created_at={'created_at': dt}


    try:
        local_stats=data['stats']['data'][0]
        local_stats_subset=dict((k, local_stats[k]) for k in ('passes','attacks','shots','fouls','corners','offsides','possessiontime','yellowcards','redcards','saves','substitutions','goal_kick','goal_attempts','free_kick','tackles'))
    
        names = {'passes':'l_passes','attacks':'l_attack','shots':'l_shots','fouls':'l_fouls', 'corners':'l_corners', 'offsides':'l_offsides', 'possessiontime':'l_possessiontime',
          'yellowcards':'l_yellowcards','redcards':'l_redcards','saves':'l_saves','substitutions':'l_substitutions',
          'goal_kick':'l_goal_kick','goal_attempts':'l_goal_attempts','free_kick':'l_free_kick','tackles':'l_tackles'}
        local_stats_subset=dict((names[key], value) for (key, value) in local_stats_subset.items())
    
    
        visitor_stats=data['stats']['data'][1]
        visitor_stats_subset=dict((k, local_stats[k]) for k in ('passes','attacks','shots','fouls','corners','offsides','possessiontime','yellowcards','redcards','saves','substitutions','goal_kick','goal_attempts','free_kick','tackles'))
        names = {'passes':'v_passes','attacks':'v_attack','shots':'v_shots','fouls':'v_fouls', 'corners':'v_corners', 'offsides':'v_offsides', 'possessiontime':'v_possessiontime',
          'yellowcards':'v_yellowcards','redcards':'v_redcards','saves':'l_saves','substitutions':'l_substitutions',
          'goal_kick':'v_goal_kick','goal_attempts':'v_goal_attempts','free_kick':'v_free_kick','tackles':'v_tackles'}
        visitor_stats_subset=dict((names[key], value) for (key, value) in visitor_stats_subset.items())
        summary={**data_subset,**scores_subset,**standings_subset,**time_subset,
                 **local_stats_subset,**visitor_stats_subset,**created_at}
    except:
        summary={**data_subset,**scores_subset,**standings_subset,**time_subset,**created_at}
        
    summary['Y'] = int(summary['winner_team_id']==summary['localteam_id'])
    summary['Y_goals'] = int(summary['localteam_score']-summary['visitorteam_score'])
    return summary

def get_relevant_data_future(data):
    data_subset=dict((k, data[k]) for k in ('id', 'league_id', 'season_id','localteam_id','visitorteam_id','winner_team_id'))
    scores=data["scores"]
    scores_subset=dict((k, scores[k]) for k in ('localteam_score', 'visitorteam_score'))
    standings=data["standings"]
    standings_subset=dict((k, standings[k]) for k in ('localteam_position', 'visitorteam_position'))
    time=data['time']['starting_at']
    time_subset=dict((k, time[k]) for k in ('date','time'))
    dt = str(datetime.now())
    created_at={'created_at': dt}
    summary={**data_subset,**scores_subset,**standings_subset,**time_subset,**created_at}
        
    return summary

def insert_dict_past(team_data,table):
    
    PROJECT_ID = "soccerguru"
    DATASET_ID = "matches"
    TABLE_ID = table

    client = bigquery.Client()
    # 2) insert data
    rows_to_insert = [get_relevant_data_past(team_data)]

    errors = client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", rows_to_insert
    )
    
    if errors == []:
        pass
    else:
        print("Encountered errors while inserting rows: {}".format(errors),team_data['id'])
        
def insert_dict_future(team_data,table):
    
    PROJECT_ID = "soccerguru"
    DATASET_ID = "matches"
    TABLE_ID = table

    client = bigquery.Client()
    # 2) insert data
    rows_to_insert = [get_relevant_data_future(team_data)]

    errors = client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", rows_to_insert
    )
    
    if errors == []:
        pass
    else:
        print("Encountered errors while inserting rows: {}".format(errors),team_data['id'])

def send_historic_data(league,date1,date2,table):
    unique_teams=get_all_teams_from_all_seasons(league)
    print("List of possible teams obtained")
    stats="&include=stats"
    endpoint="fixtures/between/"+date1+"/"+date2+"/"
    sports_key=config['sports_token']
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    end_url = "?api_token=" + str(sports_key)+"&include=stats"

    for i in tqdm(range(len(unique_teams))):
        team=str(unique_teams[i])
        url=base_url+endpoint+team+end_url
        r = requests.get(url)
        matches=r.json()['data']
        x_league_seasons = [d for d in matches if d['league_id']==league]
        for j in range(len(x_league_seasons)):
            try:
                insert_dict_past(x_league_seasons[j],table)    
            except:
                print("error")
                pass
            
def send_last_week_data(league,date1,date2,table):
    unique_teams=get_current_season_teams(league)
    print("List of possible teams obtained")
    stats="&include=stats"
    endpoint="fixtures/between/"+date1+"/"+date2+"/"
    sports_key=config['sports_token']
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    end_url = "?api_token=" + str(sports_key)+"&include=stats"

    for i in tqdm(range(len(unique_teams))):
        team=str(unique_teams[i])
        url=base_url+endpoint+team+end_url
        r = requests.get(url)
        matches=r.json()['data']
        x_league_seasons = [d for d in matches if d['league_id']==league]
        for j in range(len(x_league_seasons)):
            try:
                insert_dict_past(x_league_seasons[j],table)    
            except:
                print("error")
                pass
            
def send_future_matches(league,date1,date2,table):
    unique_teams=get_current_season_teams(league)
    print("Current list of teams obtained")
    stats="&include=stats"
    endpoint="fixtures/between/"+date1+"/"+date2+"/"
    sports_key=config['sports_token']
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    end_url = "?api_token=" + str(sports_key)+"&include=stats"
    
    for i in tqdm(range(len(unique_teams))):
        team=str(unique_teams[i])
        url=base_url+endpoint+team+end_url
        r = requests.get(url)
        matches=r.json()['data']
        x_league_seasons = [d for d in matches if d['league_id']==league]

        for j in range(len(x_league_seasons)):
            try:
                insert_dict_future(x_league_seasons[j],table)    
            except:
                print("error")
                pass
